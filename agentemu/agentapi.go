package agentemu

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// RoundResult summarizes one finished round. It is the feedback payload an
// AgentAPI receives before producing the next trace.
type RoundResult struct {
	Round       int `json:"round"`
	RecordCount int `json:"record_count"`
	TxCount     int `json:"tx_count"`
}

// AgentAPI is the director of the multi-round loop: given the result of the
// finished round it returns the next round's trace, or an empty slice to stop
// the simulation. Future implementations (e.g. an HTTP-backed AI agent) only
// need to satisfy this interface; callers stay unchanged.
type AgentAPI interface {
	NextTrace(ctx context.Context, prev RoundResult) ([]Record, error)
}

// FileAgentAPI serves rounds from round_<N>.jsonl files that live in one
// directory: given the finished round N it loads round_<N+1>.jsonl. A missing
// file means the story is over (no error).
type FileAgentAPI struct {
	dir string
}

func NewFileAgentAPI(dir string) *FileAgentAPI {
	return &FileAgentAPI{dir: dir}
}

func (f *FileAgentAPI) NextTrace(_ context.Context, prev RoundResult) ([]Record, error) {
	path := filepath.Join(f.dir, fmt.Sprintf("round_%d.jsonl", prev.Round+1))

	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("stat round file: %w", err)
	}

	records, err := LoadTrace(path)
	if err != nil {
		return nil, fmt.Errorf("load %s: %w", path, err)
	}

	return records, nil
}
