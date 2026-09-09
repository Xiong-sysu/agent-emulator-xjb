package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/HuangLab-SYSU/block-emulator-x/agentemu"
)

func activeAgents(ids ...string) []agentemu.Agent {
	agents := make([]agentemu.Agent, 0, len(ids))
	for _, id := range ids {
		agents = append(agents, agentemu.Agent{AgentID: id, DID: "did:broker:0x" + id, Active: true})
	}

	return agents
}

func TestDeterministicDecideStopsAtRoundCap(t *testing.T) {
	resp := deterministicDecide(feedbackRequest{Round: 3, Agents: activeAgents("a", "b")}, 3, 42)
	require.True(t, resp.Stop)
	require.Empty(t, resp.Trace)
}

func TestDeterministicDecideStopsWithTooFewActiveAgents(t *testing.T) {
	resp := deterministicDecide(feedbackRequest{Round: 1, Agents: activeAgents("a")}, 5, 42)
	require.True(t, resp.Stop)
}

func TestDeterministicDecideProducesValidRound(t *testing.T) {
	agents := activeAgents("alice", "bob", "carol")
	resp := deterministicDecide(feedbackRequest{Round: 1, Agents: agents}, 3, 42)
	require.False(t, resp.Stop)
	require.Len(t, resp.Trace, 2)

	pay := resp.Trace[0]
	require.Equal(t, agentemu.ActionPay, pay.Action)
	require.NotEmpty(t, pay.AgentID)
	require.NotEmpty(t, pay.Target)
	require.NotEqual(t, pay.AgentID, pay.Target)
	require.Positive(t, pay.Amount)

	// Only known agents are referenced.
	known := map[string]bool{}
	for _, agent := range agents {
		known[agent.AgentID] = true
	}
	require.True(t, known[pay.AgentID])
	require.True(t, known[pay.Target])

	log := resp.Trace[1]
	require.Equal(t, agentemu.ActionAppendLog, log.Action)
	require.Equal(t, pay.AgentID, log.AgentID)

	// Same round and seed decide identically.
	again := deterministicDecide(feedbackRequest{Round: 1, Agents: agents}, 3, 42)
	require.Equal(t, resp, again)
}

// fakeChat serves one OpenAI-compatible chat completion with the given
// content.
func fakeChat(t *testing.T, content string) *httptest.Server {
	t.Helper()

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":` + fmt.Sprintf("%q", content) + `}}]}`))
	}))
}

func TestLLMDecideParsesModelAnswer(t *testing.T) {
	srv := fakeChat(t, `{"stop":false,"trace":[{"agent_id":"alice","action":"pay","target":"bob","amount":3,"ts":1}]}`)
	defer srv.Close()

	t.Setenv("TEST_LLM_KEY", "sk-test")

	opts := llmOptions{apiBase: srv.URL, model: "deepseek-chat", apiKeyEnv: "TEST_LLM_KEY", timeout: 0}
	resp, err := llmDecide(context.Background(), opts, []byte(`{"round":1}`))
	require.NoError(t, err)
	require.False(t, resp.Stop)
	require.Len(t, resp.Trace, 1)
	require.Equal(t, agentemu.ActionPay, resp.Trace[0].Action)
}

func TestLLMDecideStripsCodeFences(t *testing.T) {
	fenced := "```json\n{\"stop\":true,\"trace\":[]}\n```"
	srv := fakeChat(t, fenced)
	defer srv.Close()

	t.Setenv("TEST_LLM_KEY", "sk-test")

	opts := llmOptions{apiBase: srv.URL, model: "deepseek-chat", apiKeyEnv: "TEST_LLM_KEY"}
	resp, err := llmDecide(context.Background(), opts, []byte(`{"round":2}`))
	require.NoError(t, err)
	require.True(t, resp.Stop)
}

func TestLLMDecideRequiresAPIKey(t *testing.T) {
	t.Setenv("TEST_LLM_KEY_MISSING", "")

	opts := llmOptions{apiBase: "http://127.0.0.1:1", model: "m", apiKeyEnv: "TEST_LLM_KEY_MISSING"}
	_, err := llmDecide(context.Background(), opts, []byte(`{}`))
	require.ErrorContains(t, err, "environment variable TEST_LLM_KEY_MISSING is not set")
}

func TestLLMDecideSurfacesServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "quota exceeded", http.StatusTooManyRequests)
	}))
	defer srv.Close()

	t.Setenv("TEST_LLM_KEY", "sk-test")

	opts := llmOptions{apiBase: srv.URL, model: "m", apiKeyEnv: "TEST_LLM_KEY"}
	_, err := llmDecide(context.Background(), opts, []byte(`{}`))
	require.ErrorContains(t, err, "chat api status 429")
}

func TestStripJSONFences(t *testing.T) {
	require.Equal(t, `{"a":1}`, stripJSONFences("```json\n{\"a\":1}\n```"))
	require.Equal(t, `{"a":1}`, stripJSONFences("  {\"a\":1}  "))
	require.Equal(t, `{"a":1}`, stripJSONFences("```\n{\"a\":1}\n```"))
}
