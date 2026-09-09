package agentemu

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config is intentionally separate from BlockEmulator-X's config.yaml so legacy
// experiments retain their existing configuration and execution paths.
type Config struct {
	Base struct {
		BlockEmulatorConfig string `yaml:"blockemulator_config"`
		ResultDir           string `yaml:"result_dir"`
	} `yaml:"base"`
	Experiment struct {
		Seed  int64  `yaml:"seed"`
		Trace string `yaml:"trace"`
	} `yaml:"experiment"`
	AgentAPI struct {
		// Mode selects the feedback implementation: "file" (round_<N>.jsonl
		// story files, default) or "http" (external agent service).
		Mode string `yaml:"mode"`
		// Endpoint is the agent service URL posted to on every round
		// (mode: http), e.g. http://127.0.0.1:9092/next_trace.
		Endpoint string `yaml:"endpoint"`
		// RequestTimeoutSeconds bounds one HTTP request (default 30).
		RequestTimeoutSeconds int `yaml:"request_timeout_seconds"`
		// Retries is how often a failed request is repeated with backoff
		// before the simulation terminates gracefully (default 3).
		Retries int `yaml:"retries"`
		// WaitConfirmTimeoutSeconds is how long to wait for all queued
		// transactions to appear as confirmed in the chain CSV before asking
		// the agent for the next round (mode: http only; default 120, 0
		// disables the wait).
		WaitConfirmTimeoutSeconds int `yaml:"wait_confirm_timeout_seconds"`
		// ChainResultsCSV is the supervisor measurement file consulted for
		// confirmations and feedback statistics.
		ChainResultsCSV string `yaml:"chain_results_csv"`
	} `yaml:"agent_api"`
	Protocols struct {
		Pay struct {
			Plugin          string `yaml:"plugin"`
			ContractAddress string `yaml:"contract_address"`
		} `yaml:"pay"`
		Audit struct {
			Plugin          string `yaml:"plugin"`
			ContractAddress string `yaml:"contract_address"`
			BatchSize       int    `yaml:"batch_size"`
		} `yaml:"audit"`
		Identity struct {
			Plugin          string `yaml:"plugin"`
			ContractAddress string `yaml:"contract_address"`
		} `yaml:"identity"`
	} `yaml:"protocols"`
}

// Agent APIMode values.
const (
	AgentAPIModeFile = "file"
	AgentAPIModeHTTP = "http"
)

func LoadConfig(path string) (Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read agentemu config: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return Config{}, fmt.Errorf("unmarshal agentemu config: %w", err)
	}

	if cfg.Base.ResultDir == "" || cfg.Experiment.Trace == "" {
		return Config{}, fmt.Errorf("base.result_dir and experiment.trace are required")
	}

	if cfg.AgentAPI.Mode == "" {
		cfg.AgentAPI.Mode = AgentAPIModeFile
	}

	if cfg.AgentAPI.Mode != AgentAPIModeFile && cfg.AgentAPI.Mode != AgentAPIModeHTTP {
		return Config{}, fmt.Errorf("agent_api.mode must be %q or %q", AgentAPIModeFile, AgentAPIModeHTTP)
	}

	if cfg.AgentAPI.Mode == AgentAPIModeHTTP && cfg.AgentAPI.Endpoint == "" {
		return Config{}, fmt.Errorf("agent_api.endpoint is required when agent_api.mode is %q", AgentAPIModeHTTP)
	}

	if cfg.AgentAPI.RequestTimeoutSeconds <= 0 {
		cfg.AgentAPI.RequestTimeoutSeconds = 30
	}

	if cfg.AgentAPI.Retries <= 0 {
		cfg.AgentAPI.Retries = 3
	}

	if cfg.AgentAPI.WaitConfirmTimeoutSeconds <= 0 {
		cfg.AgentAPI.WaitConfirmTimeoutSeconds = 120
	}

	if cfg.AgentAPI.ChainResultsCSV == "" {
		cfg.AgentAPI.ChainResultsCSV = "./exp/results/relay_stats_detail_tx_info.csv"
	}

	if cfg.Protocols.Pay.Plugin == "" || cfg.Protocols.Audit.Plugin == "" || cfg.Protocols.Identity.Plugin == "" {
		return Config{}, fmt.Errorf("a plugin must be selected for pay, audit, and identity")
	}

	if cfg.Protocols.Pay.Plugin != "direct-pay" {
		return Config{}, fmt.Errorf("initial release only supports protocols.pay.plugin=direct-pay")
	}

	if cfg.Protocols.Audit.BatchSize <= 0 {
		cfg.Protocols.Audit.BatchSize = 1
	}

	return cfg, nil
}
