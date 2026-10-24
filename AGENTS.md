# AGENTS.md — HVACPulse streaming platform

HVACPulse is a streaming data platform for industrial HVAC telemetry. Simulated
air-handler and mock-plant events flow through Apache Kafka into Spark
Structured Streaming jobs that maintain a Delta Lake medallion architecture
(Bronze raw, validated Silver, Quarantine with typed error codes, versioned
Gold). On top of that foundation, the plant pipeline builds streaming features,
trains cooling-load models with chronological splits and gates, and evaluates
constrained control plans against a **mock** plant (shadow by default;
`--simulate` only acknowledges the mock). One shared PySpark codebase runs
locally (Windows + Docker Kafka) and on Azure Databricks via an Asset Bundle.

## Rule files

| File | Scope |
|---|---|
| `.cursor/rules/project-context.mdc` | Always-on: commands, targets, do-not-commit paths, environment traps |
| `.cursor/rules/python.mdc` | `**/*.py` — Ruff, `HVACPULSE_*`, pytest markers |
| `.cursor/rules/streaming.mdc` | `streaming/**` — dataset names, checkpoints, grain |
| `.cursor/rules/plant.mdc` | `plant/**`, plant ingestion, `mock_plant.json` — gates and plant commands |
| `.cursor/rules/contracts.mdc` | `contracts/**` — Avro families and compatibility checker |
| `.cursor/rules/quality.mdc` | `quality/**` — quarantine versus operational signals |
| `.cursor/rules/dashboard.mdc` | `dashboard/**` — delta-rs reads, no Spark session |
| `.cursor/rules/terraform.mdc` | `infra/terraform/**` — Azure names, fmt/validate, no committed tfvars |
| `.cursor/rules/databricks.mdc` | `databricks/**`, `databricks.yml` — bundle targets and cloud plant path |

## Common tasks

```powershell
# Setup (Python 3.10+, uv, Java 17, Docker Desktop)
uv sync --group dev --group dashboard

# Lint + offline tests + schema compatibility (matches CI)
uv run ruff check .
uv run pytest tests/unit -m unit
.\.venv\Scripts\python.exe contracts\check_compatibility.py contracts\avro
.\.venv\Scripts\python.exe contracts\check_compatibility.py contracts\plant

# Spark / Delta / dashboard tests
uv run pytest tests/spark -m spark
uv run pytest tests/delta -m delta
uv run --group dashboard pytest tests/dashboard -m dashboard

# Air-handler end-to-end demo (Kafka up → simulate → bronze → … → verify)
.\scripts\local\run_local_stack.ps1 -Task all

# Plant pipeline: backfill, train, evaluate shadow control; then a feedback cycle
.\scripts\local\run_local_stack.ps1 -Task plant-all
.\scripts\local\run_local_stack.ps1 -Task plant-cycle -SimulateControl

# Dashboard (Streamlit, reads Delta via delta-rs)
.\scripts\local\run_local_stack.ps1 -Task dashboard

# Opt-in live Kafka transport regression (broker must be running)
$env:HVACPULSE_TEST_KAFKA = "1"
uv run pytest tests/integration/test_plant_kafka.py -m integration

# Cloud deploy — see docs/runbooks/azure-databricks.md
databricks bundle deploy -t dev --var "catalog=<catalog>" --var "schema=hvacpulse" ...
```

## Do not

- Do not commit secrets, `terraform.tfvars`, machine-specific runtimes, or
  `.local/` Delta output.
- Do not enable Delta `autoMerge` or make non-additive Avro schema changes; CI
  enforces transitive backward compatibility across all retained versions.
- Do not quarantine valid-but-alarming measurements — quarantine is for corrupt
  data only; alarming data produces operational signals (ADR-003).
- Do not share checkpoint locations between streaming queries, or run a stream
  and a drain job against the same checkpoint concurrently.
- Do not set `SPARK_LOCAL_IP` on Windows (hangs Spark's BlockManager); CI sets
  it on Linux runners only.
- Do not weaken safety gates (freshness, model gates, interlocks) to make a
  demo pass, and never implement physical/BMS control — the platform is
  advisory with a mock plant only.
- Do not put equipment limits or plant constraints in code; they belong in
  `reference/`.

## Key references

- `README.md` — architecture, data model, idempotency design, run evidence
- `docs/decisions/ADR-001…005` — contracts, identity, quarantine, MERGE, recommendations
- `docs/runbooks/plant-pipeline.md` — plant contracts, commands, safety boundaries
- `docs/runbooks/azure-databricks.md` — cloud deploy; `docs/runbooks/local-replay.md` — replay
- `docs/checklists.md` — review checklists (data warehouse, Spark, Kafka)
