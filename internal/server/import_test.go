package server

import (
	"archive/zip"
	"bytes"
	"encoding/json/v2"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.kenn.io/agentsview/internal/db"
	"go.kenn.io/agentsview/internal/importer"
)

func TestHandleImportClaudeAI(t *testing.T) {
	srv := testServer(t, 5*time.Second)

	conversations := `[
      {
        "uuid": "api-test-001",
        "name": "API Test",
        "summary": "",
        "created_at": "2026-03-01T10:00:00.000000Z",
        "updated_at": "2026-03-01T10:05:00.000000Z",
        "account": {"uuid": "acct-1"},
        "chat_messages": [
          {
            "uuid": "m1",
            "text": "Test message",
            "content": [{"type":"text","text":"Test message"}],
            "sender": "human",
            "created_at": "2026-03-01T10:00:00.000000Z",
            "updated_at": "2026-03-01T10:00:00.000000Z",
            "attachments": [],
            "files": []
          }
        ]
      }
    ]`

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", "conversations.json")
	require.NoError(t, err)
	_, _ = part.Write([]byte(conversations))
	writer.Close()

	req := httptest.NewRequestWithContext(t.Context(),
		http.MethodPost,
		"/api/v1/import/claude-ai",
		&body,
	)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	rec := httptest.NewRecorder()
	srv.mux.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	var stats importer.ImportStats
	require.NoError(t, json.UnmarshalRead(rec.Body, &stats))
	assert.Equal(t, 1, stats.Imported)
	assert.Zero(t, stats.Updated)
}

// TestHandleImportRejectsWriterClosedBeforeStream pins the maintenance-mode
// UX: while a worker pass holds the write barrier, import requests fail before
// the stream body opens with the transient 503 + Retry-After instead of a
// misleading 500 or an HTTP-200 SSE error event.
func TestHandleImportRejectsWriterClosedBeforeStream(t *testing.T) {
	for _, tt := range []struct {
		name     string
		path     string
		filename string
	}{
		{name: "claude-ai", path: "/api/v1/import/claude-ai", filename: "conversations.json"},
		{name: "chatgpt", path: "/api/v1/import/chatgpt", filename: "export.zip"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			srv := testServer(t, 5*time.Second)
			local, ok := srv.db.(*db.DB)
			require.True(t, ok)
			require.NoError(t, local.CloseWriter())
			t.Cleanup(func() { require.NoError(t, local.ReopenWriter()) })

			var body bytes.Buffer
			writer := multipart.NewWriter(&body)
			part, err := writer.CreateFormFile("file", tt.filename)
			require.NoError(t, err)
			_, _ = part.Write([]byte("[]"))
			writer.Close()

			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, tt.path, &body)
			req.Header.Set("Content-Type", writer.FormDataContentType())
			req.Header.Set("Accept", "text/event-stream")
			rec := httptest.NewRecorder()
			srv.mux.ServeHTTP(rec, req)

			require.Equal(t, http.StatusServiceUnavailable, rec.Code,
				"body: %s", rec.Body.String())
			assert.Equal(t, writerClosedRetryAfterSeconds,
				rec.Header().Get("Retry-After"))
			assert.NotContains(t, rec.Body.String(), "event:",
				"the rejection must not open an SSE stream")
		})
	}
}

func TestHandleImportChatGPT_RequiresZip(t *testing.T) {
	srv := testServer(t, 5*time.Second)

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", "data.json")
	require.NoError(t, err)
	_, _ = part.Write([]byte("[]"))
	writer.Close()

	req := httptest.NewRequestWithContext(t.Context(),
		http.MethodPost,
		"/api/v1/import/chatgpt",
		&body,
	)
	req.Header.Set(
		"Content-Type", writer.FormDataContentType(),
	)

	rec := httptest.NewRecorder()
	srv.mux.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code, "body: %s", rec.Body.String())
}

func TestHandleImportClaudeAI_SSE(t *testing.T) {
	srv := testServer(t, 5*time.Second)

	conversations := `[{
      "uuid": "sse-test-001",
      "name": "SSE Test",
      "created_at": "2026-03-01T10:00:00.000000Z",
      "updated_at": "2026-03-01T10:05:00.000000Z",
      "chat_messages": [{
        "uuid": "m1", "text": "hello", "sender": "human",
        "content": [{"type":"text","text":"hello"}],
        "created_at": "2026-03-01T10:00:00.000000Z"
      }]
    }]`

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile(
		"file", "conversations.json",
	)
	require.NoError(t, err)
	_, _ = part.Write([]byte(conversations))
	writer.Close()

	req := httptest.NewRequestWithContext(t.Context(),
		http.MethodPost,
		"/api/v1/import/claude-ai",
		&body,
	)
	req.Header.Set(
		"Content-Type", writer.FormDataContentType(),
	)
	req.Header.Set("Accept", "text/event-stream")

	rec := httptest.NewRecorder()
	srv.mux.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	require.Contains(t, rec.Header().Get("Content-Type"), "text/event-stream")

	// Parse the done event from the SSE body.
	var stats importer.ImportStats
	lines := strings.Split(rec.Body.String(), "\n")
	for i, line := range lines {
		if line == "event: done" && i+1 < len(lines) {
			data := strings.TrimPrefix(
				lines[i+1], "data: ",
			)
			require.NoError(t, json.Unmarshal([]byte(data), &stats))
		}
	}
	assert.Equal(t, 1, stats.Imported)
}

func TestHandleImportClaudeAI_NoFile(t *testing.T) {
	srv := testServer(t, 5*time.Second)

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	writer.Close()

	req := httptest.NewRequestWithContext(t.Context(),
		http.MethodPost,
		"/api/v1/import/claude-ai",
		&body,
	)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	rec := httptest.NewRecorder()
	srv.mux.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code, "body: %s", rec.Body.String())
}

func claudeAIReplaceExport(texts ...string) string {
	msgs := make([]string, len(texts))
	for i, text := range texts {
		sender := "human"
		if i%2 == 1 {
			sender = "assistant"
		}
		ts := fmt.Sprintf("2026-03-01T10:0%d:00.000000Z", i)
		msgs[i] = fmt.Sprintf(`{"uuid":"m%d","text":%q,"content":[{"type":"text","text":%q}],"sender":%q,"created_at":%q,"updated_at":%q,"attachments":[],"files":[]}`,
			i, text, text, sender, ts, ts)
	}
	return `[{"uuid":"replace-001","name":"Replace","summary":"",` +
		`"created_at":"2026-03-01T10:00:00.000000Z","updated_at":"2026-03-01T10:05:00.000000Z",` +
		`"account":{"uuid":"acct-1"},"chat_messages":[` + strings.Join(msgs, ",") + `]}]`
}

func chatGPTReplaceZip(t *testing.T, texts ...string) []byte {
	t.Helper()
	nodes := make([]string, len(texts))
	parent := "r"
	for i, text := range texts {
		role := "user"
		if i%2 == 1 {
			role = "assistant"
		}
		node := fmt.Sprintf("n%d", i+1)
		children := "[]"
		if i+1 < len(texts) {
			children = fmt.Sprintf(`["n%d"]`, i+2)
		}
		nodes[i] = fmt.Sprintf(`%q:{"id":%q,"parent":%q,"children":%s,"message":{"id":"m-%s","create_time":%d,"author":{"role":%q,"name":null,"metadata":{}},"content":{"content_type":"text","parts":[%q]},"status":"finished_successfully","metadata":{}}}`,
			node, node, parent, children, node, 1706745600+10*i, role, text)
		parent = node
	}
	conv := `[{"id":"cg-replace","conversation_id":"cg-replace","title":"Replace",` +
		`"create_time":1706745600.0,"update_time":1706745700.0,"current_node":"` + parent + `",` +
		`"mapping":{"r":{"id":"r","parent":null,"children":["n1"],"message":null},` + strings.Join(nodes, ",") + `}}]`
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.Create("conversations-000.json")
	require.NoError(t, err)
	_, err = w.Write([]byte(conv))
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	return buf.Bytes()
}

func postImport(t *testing.T, srv *Server, path, filename string, data []byte, stream bool) importer.ImportStats {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", filename)
	require.NoError(t, err)
	_, _ = part.Write(data)
	require.NoError(t, writer.Close())
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	if stream {
		req.Header.Set("Accept", "text/event-stream")
	}
	rec := httptest.NewRecorder()
	srv.mux.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	var stats importer.ImportStats
	if !stream {
		require.NoError(t, json.UnmarshalRead(rec.Body, &stats))
		return stats
	}
	lines := strings.Split(rec.Body.String(), "\n")
	found := false
	for i, line := range lines {
		if line == "event: done" && i+1 < len(lines) {
			require.NoError(t, json.Unmarshal([]byte(strings.TrimPrefix(lines[i+1], "data: ")), &stats))
			found = true
		}
	}
	require.True(t, found, "no done event in %s", rec.Body.String())
	return stats
}

func TestHandleImportReplaceQuery(t *testing.T) {
	for _, tt := range []struct {
		name      string
		path      string
		filename  string
		sessionID string
		stream    bool
		initial   []byte
		refused   []byte
	}{
		{
			name: "claude-ai json", path: "/api/v1/import/claude-ai", filename: "conversations.json",
			sessionID: "claude-ai:replace-001",
			initial:   []byte(claudeAIReplaceExport("Hello", "Hi there", "Extra")),
			refused:   []byte(claudeAIReplaceExport("Hello", "Hi there")),
		},
		{
			name: "claude-ai sse", path: "/api/v1/import/claude-ai", filename: "conversations.json",
			sessionID: "claude-ai:replace-001", stream: true,
			initial: []byte(claudeAIReplaceExport("Hello", "Hi there", "Extra")),
			refused: []byte(claudeAIReplaceExport("Hello", "Hi there")),
		},
		{
			name: "chatgpt json", path: "/api/v1/import/chatgpt", filename: "export.zip",
			sessionID: "chatgpt:cg-replace",
			initial:   chatGPTReplaceZip(t, "Hello", strings.Repeat("x", 500), "metadata row"),
			refused:   chatGPTReplaceZip(t, "Hello", strings.Repeat("x", 800)),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			srv := testServer(t, 5*time.Second)
			stats := postImport(t, srv, tt.path, tt.filename, tt.initial, tt.stream)
			require.Equal(t, 1, stats.Imported)

			stats = postImport(t, srv, tt.path, tt.filename, tt.refused, tt.stream)
			assert.Equal(t, 1, stats.Errors)
			assert.Zero(t, stats.Updated)

			query := "?" + url.Values{"replace": {tt.sessionID}}.Encode()
			stats = postImport(t, srv, tt.path+query, tt.filename, tt.refused, tt.stream)
			assert.Equal(t, 1, stats.Updated)
			assert.Zero(t, stats.Errors)

			local, ok := srv.db.(*db.DB)
			require.True(t, ok)
			msgs, err := local.GetAllMessages(t.Context(), tt.sessionID)
			require.NoError(t, err)
			assert.Len(t, msgs, 2)
			trashed, err := local.ListTrashedSessions(t.Context())
			require.NoError(t, err)
			require.Len(t, trashed, 1)
			assert.True(t, strings.HasPrefix(trashed[0].ID, tt.sessionID+":replaced:"), trashed[0].ID)
		})
	}
}
