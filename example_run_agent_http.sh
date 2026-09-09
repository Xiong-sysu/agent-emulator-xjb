#!/bin/bash

# AgentEmulator HTTP feedback run: 1 shard x 4 consensus nodes + supervisor +
# the example agent service (deterministic policy).
#
# The simulator posts each round's confirmed on-chain results to the agent
# service, which decides the next round's trace until it says stop
# (-max-rounds, 3 by default: the initial trace plus two decided rounds).
#
# Results land in ./exp/agentemu-results (plan, events, registry, rounds) and
# ./exp/results (chain measurements); Agent_Stats.csv via agentemu-report.

set -ex

AGENT_ADDR=127.0.0.1:9092
MAX_ROUNDS=3

# Delete the old experiment directory.
rm -rf ./exp/
mkdir -p ./exp/

# Download modules and pre-compile.
go mod download
go build ./...

# Build and start the example agent service (a built binary can be killed
# directly, unlike a `go run` child).
go build -o ./exp/agentemu-example-agent ./cmd/agentemu-example-agent
./exp/agentemu-example-agent -addr "${AGENT_ADDR}" -max-rounds "${MAX_ROUNDS}" &
AGENT_PID=$!

cleanup() {
  kill "${AGENT_PID}" 2>/dev/null || true
}
trap cleanup EXIT

# Start the consensus nodes of shard 0.
CLUSTER_PIDS=()
for j in 0 1 2 3; do
  go run cmd/consensusnode/main.go -shard_id=0 -node_id="${j}" -config=config_agent_http.yaml &
  CLUSTER_PIDS+=($!)
done

# Start the supervisor.
go run cmd/supervisor/main.go -shard_id=0x7fffffff -node_id=0 -config=config_agent_http.yaml &
CLUSTER_PIDS+=($!)

# The agent service never exits on its own; wait only for the cluster.
wait "${CLUSTER_PIDS[@]}"
