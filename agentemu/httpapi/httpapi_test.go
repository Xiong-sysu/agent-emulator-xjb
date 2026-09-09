package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/HuangLab-SYSU/block-emulator-x/agentemu"
)

func httpTestConfig(t *testing.T, endpoint string, retries int) agentemu.Config {
	t.Helper()

	var cfg agentemu.Config
	cfg.Base.ResultDir = t.TempDir()
	cfg.Experiment.Seed = 42
	cfg.Experiment.Trace = filepath.Join(t.TempDir(), "round_1.jsonl")
	cfg.AgentAPI.Mode = agentemu.AgentAPIModeHTTP
	cfg.AgentAPI.Endpoint = endpoint
	cfg.AgentAPI.RequestTimeoutSeconds = 2
	cfg.AgentAPI.Retries = retries
	cfg.AgentAPI.ChainResultsCSV = filepath.Join(t.TempDir(), "missing.csv")

	return cfg
}

// recordingAgent answers with the given body and captures request payloads.
func recordingAgent(t *testing.T, received chan<- []byte, status int, body string) http.Handler {
	t.Helper()

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
		}

		received <- raw

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	})
}

func TestHTTPAgentAPISendsFeedbackAndReturnsTrace(t *testing.T) {
	received := make(chan []byte, 8)

	srv := httptest.NewServer(recordingAgent(t, received, http.StatusOK,
		`{"stop":false,"trace":[{"agent_id":"alice","action":"pay","target":"bob","amount":5,"ts":1}]}`))
	defer srv.Close()

	cfg := httpTestConfig(t, srv.URL, 1)

	// Registry snapshot written by the agent source is picked up.
	registry := `[{"agent_id":"alice","did":"did:broker:0x01","active":true}]`
	require.NoError(t, os.WriteFile(filepath.Join(cfg.Base.ResultDir, agentemu.RegistryFileName), []byte(registry), 0o644))

	api := New(cfg)
	records, err := api.NextTrace(context.Background(), agentemu.RoundResult{Round: 1, RecordCount: 3, TxCount: 5})
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, agentemu.ActionPay, records[0].Action)

	var payload struct {
		Round  int `json:"round"`
		Agents []struct {
			AgentID string `json:"agent_id"`
		} `json:"agents"`
	}
	require.NoError(t, json.Unmarshal(<-received, &payload))
	require.Equal(t, 1, payload.Round)
	require.Len(t, payload.Agents, 1)
	require.Equal(t, "alice", payload.Agents[0].AgentID)
}

func TestHTTPAgentAPIStopEndsTheStory(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"stop":true,"trace":[]}`))
	}))
	defer srv.Close()

	api := New(httpTestConfig(t, srv.URL, 1))

	records, err := api.NextTrace(context.Background(), agentemu.RoundResult{Round: 3})
	require.NoError(t, err)
	require.Empty(t, records)
}

func TestHTTPAgentAPIRetriesThenSucceeds(t *testing.T) {
	var calls int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if atomic.AddInt32(&calls, 1) == 1 {
			http.Error(w, "boom", http.StatusInternalServerError)

			return
		}

		_, _ = w.Write([]byte(`{"stop":false,"trace":[{"agent_id":"a","action":"join","ts":1}]}`))
	}))
	defer srv.Close()

	api := New(httpTestConfig(t, srv.URL, 1))

	records, err := api.NextTrace(context.Background(), agentemu.RoundResult{Round: 1})
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.EqualValues(t, 2, atomic.LoadInt32(&calls))
}

func TestHTTPAgentAPIFailsAfterRetriesAreSpent(t *testing.T) {
	var calls int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&calls, 1)
		http.Error(w, "down", http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	api := New(httpTestConfig(t, srv.URL, 1))

	_, err := api.NextTrace(context.Background(), agentemu.RoundResult{Round: 1})
	require.ErrorContains(t, err, "failed after 2 attempts")
	require.EqualValues(t, 2, atomic.LoadInt32(&calls))
}

func TestHTTPAgentAPIRejectsInvalidTrace(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"stop":false,"trace":[{"action":"pay"}]}`)) // no agent_id
	}))
	defer srv.Close()

	api := New(httpTestConfig(t, srv.URL, 0))

	_, err := api.NextTrace(context.Background(), agentemu.RoundResult{Round: 1})
	require.ErrorContains(t, err, "invalid trace")
}

func TestNewAgentAPIFactory(t *testing.T) {
	fileCfg := httpTestConfig(t, "http://unused", 1)
	fileCfg.AgentAPI.Mode = agentemu.AgentAPIModeFile
	fileCfg.AgentAPI.Endpoint = ""

	api, err := NewAgentAPI(fileCfg)
	require.NoError(t, err)
	require.IsType(t, &agentemu.FileAgentAPI{}, api)

	httpCfg := httpTestConfig(t, "http://127.0.0.1:1", 1)
	api, err = NewAgentAPI(httpCfg)
	require.NoError(t, err)
	require.IsType(t, &HTTPAgentAPI{}, api)

	unknown := httpTestConfig(t, "http://unused", 1)
	unknown.AgentAPI.Mode = "carrier-pigeon"
	_, err = NewAgentAPI(unknown)
	require.ErrorContains(t, err, "unsupported agent_api.mode")
}

func TestBackoffSleepsRoughly(t *testing.T) {
	start := time.Now()
	require.True(t, sleepBackoff(context.Background(), 1))
	require.GreaterOrEqual(t, time.Since(start), backoffBase-50*time.Millisecond)
}
