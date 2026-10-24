---
name: local-stack
description: Runs the HVACPulse local Kafka-to-Delta loop, mock-plant backfill, and checkpoint-safe replay. Use when asked to run the local stack, backfill or cycle the mock plant, replay from Kafka or Bronze, or verify a local end-to-end run.
---

# Local stack

Use `scripts/local/run_local_stack.ps1`. Do not invent a second runner. Dataset names and grain stay in the streaming rule.

Prerequisites: Docker Desktop, Java 17, `uv sync --group dev`. Do not set `SPARK_LOCAL_IP`. If OneDrive locks `.local/`, set `HVACPULSE_DATA_ROOT` outside the repo (for example `C:\Temp\hvacpulse-local`) before any task.

## Air-handler loop

```powershell
.\scripts\local\run_local_stack.ps1 -Task all
```

That task already runs up, topics, simulate, bronze, silver, gold, quality, recommendations, reconcile, maintain, verify, and benchmark. Paste the verify and benchmark output. `verify` prints `E2E result: PASS` only when the count script says so.

Re-running bronze, silver, and gold on the same simulation must add zero Silver rows and leave Gold metrics unchanged.

## Plant backfill

```powershell
.\scripts\local\run_local_stack.ps1 -Task plant-all
.\scripts\local\run_local_stack.ps1 -Task plant-cycle -SimulateControl
```

`plant-all` starts Kafka, backfills snapshots, drains Bronze, plant Silver, and features, trains, then takes one shadow decision. Add `-SimulateControl` only when the user wants a mock ACK. A stale-data block is a real gate failure; do not loosen freshness, model, or interlock gates.

## Replay

Follow `docs/runbooks/local-replay.md`.

- Kafka replay: set `HVACPULSE_DATA_ROOT` to a new `.local-replay` directory, then bronze, silver, gold. Do not reuse the old checkpoint.
- Bronze replay: keep `bronze_telemetry`. Delete derived Delta tables and the `silver` checkpoint, then silver and gold.
- Do not delete the `bronze` checkpoint while keeping the Bronze table. That re-appends the topic.

## Stop

Do not run a long-running stream and a `-Task` drain against the same checkpoint. Do not commit `.local/` or `.local-replay/`.
