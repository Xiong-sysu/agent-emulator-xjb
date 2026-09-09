package agentemu

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFileAgentAPIServesConsecutiveRoundsThenStops(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "round_1.jsonl"),
		[]byte("{\"agent_id\":\"alice\",\"action\":\"join\",\"ts\":1}\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "round_2.jsonl"),
		[]byte("{\"agent_id\":\"alice\",\"action\":\"pay\",\"target\":\"bob\",\"amount\":3,\"ts\":2}\n"), 0o644))

	api := NewFileAgentAPI(dir)
	ctx := context.Background()

	next, err := api.NextTrace(ctx, RoundResult{Round: 1})
	require.NoError(t, err)
	require.Len(t, next, 1)
	require.Equal(t, ActionPay, next[0].Action)

	// No round_3.jsonl -> the story is over, not an error.
	next, err = api.NextTrace(ctx, RoundResult{Round: 2})
	require.NoError(t, err)
	require.Empty(t, next)
}

func TestFileAgentAPIBadJSON(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "round_1.jsonl"),
		[]byte("{\"agent_id\":\"alice\",\"action\":\"join\",\"ts\":1}\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "round_2.jsonl"), []byte("{broken}\n"), 0o644))

	api := NewFileAgentAPI(dir)

	_, err := api.NextTrace(context.Background(), RoundResult{Round: 1})
	require.ErrorContains(t, err, "load "+filepath.Join(dir, "round_2.jsonl"))
}

func TestFileAgentAPIMissingFirstRoundFile(t *testing.T) {
	api := NewFileAgentAPI(t.TempDir())

	next, err := api.NextTrace(context.Background(), RoundResult{Round: 1})
	require.NoError(t, err)
	require.Empty(t, next)
}
