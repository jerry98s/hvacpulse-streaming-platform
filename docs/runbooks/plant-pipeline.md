# Plant pipeline: collect → features → train → control

## Purpose and scope

Use reliable streaming data to estimate plant cooling demand and test constrained
equipment operation. This implementation targets the requested **mock** plant.
It does not connect to a BMS, learn equipment dynamics, establish cleanroom safety,
or prove customer savings. The optimizer uses illustrative configured curves.

The existing air-handler simulator is a separate source profile, not a separate
platform. Both source profiles use `ingestion.producer`, Kafka, shared immutable
Bronze, the existing Spark/Delta runtime, and one read-only Streamlit app.

## Data contracts and ownership

| Stage | Input → durable output | Responsibility / problem solved |
|---|---|---|
| Collect | 24 mock tags → Avro `plant_snapshot_v1` on `hvac.telemetry` | Chiller supply/return temperature, water flow, power, state; tower/pump/VFD tags; weather/process context. No features or labels fabricated in transport. |
| Bronze | Kafka key/value/headers/offsets → `bronze_telemetry` | Retain raw payloads for audit and replay; shared with air-handler v1/v2. |
| Silver | Bronze plant records → `silver_plant_snapshots`, `quarantine_plant_snapshots` | Scope/config checks, source quality, calibration, unit conversion, alignment, range validation. Preserve rejected reasons. |
| Features | Valid Silver stream → `gold_plant_features` | One instantaneous feature/label row per scoped capture time; deterministic `plant-instant-v1` feature contract. |
| Train | Available, clean, scoped features → `plant_model_registry` | 60/20/20 chronological train/selection/test split; linear vs tanh candidates; lineage, artifacts and mock promotion gate. |
| Estimate/control | Fresh features + matching model → `gold_plant_predictions`, `gold_plant_control_decisions` | Estimate current demand, allocate load within configured constraints, record shadow blocks or mock ACKs durably. |
| Feedback | Last mock ACK → next `plant_pipeline cycle --simulate` | The mock source reflects acknowledged setpoint/allocation; resulting tags traverse the same pipeline again. |
| Observe | The above Delta tables → existing dashboard | Feature quality, measured load/power, model holdout, decision reasons and command audit. No JSON-run side channel. |

Plant records carry `event_type=plant_snapshot` and `schema_version=1` Kafka
headers. Their Avro schema lives in `contracts/plant/`, separate from the
air-handler schema family in `contracts/avro/`. Kafka key is tenant/site/plant.
Schema dispatch keeps plant payloads out of the air-handler parser and metrics.

Silver snapshot identity is **scope + capture time**. v1 snapshots are immutable:
same payload is a retry, different payload at that identity is quarantined.
Plant corrections are not silently restated like air-handler revision records.
Later quarantine entries exclude affected features from subsequent training and
revoke models whose recorded training lineage includes them. Previously emitted
decisions remain historical audit records; they are not erased.

Features use measured outdoor temperature, humidity, wet bulb, occupancy,
production heat and operating schedule plus cyclical hour-of-day values.
Cooling-load label is Σ `1.163 × flow_m3h × (return_c - supply_c)` for water loops.
Power label is the sum of chiller/tower/pump meters. Missing meters never become
zero. Snapshots are **instantaneous**, not integrated energy intervals.

Capture time and ingestion availability are distinct UTC fields. Training
filters by availability as of the training run, then splits by capture time.
At least 96 clean snapshots spanning two days are required; training is bounded
to 20,000 rows and a configurable 2–365-day window (default 30). A model is
approved for mock use only if untouched-test CVRMSE ≤15% and RMSE beats a
constant-last-training-load baseline. Registry records include feature IDs,
payload hashes, source Delta version, cutoffs and model parameters.

The learned model is a **conditional current-load estimate**, not a six-hour
forecast or learned control policy. The optimizer takes the larger of measured
and estimated demand as a safety floor. Forecast utilities remain domain-level
experiments; the runtime does not invent future weather inputs.

## Run locally

Prerequisites: the existing Python 3.10+ environment, Java 17, Docker Desktop.

```powershell
uv sync --group dev --group dashboard

# Starts the existing broker/topic; backfills 2,000 mock snapshots through Kafka;
# drains Bronze/Silver/features; trains; collects a fresh snapshot; evaluates shadow control.
.\scripts\local\run_local_stack.ps1 -Task plant-all

# Explicit mock ACKs and feedback. Run again for another observation/control cycle.
.\scripts\local\run_local_stack.ps1 -Task plant-cycle -SimulateControl
.\scripts\local\run_local_stack.ps1 -Task dashboard
```

Use `-PythonExecutable <path>` if the existing virtual environment is broken.
Do not commit machine-specific runtimes. `HVACPULSE_DATA_ROOT`, `HVACPULSE_TOPIC`,
`HVACPULSE_KAFKA_BOOTSTRAP`, `HVACPULSE_PLANT_CONFIG` and the existing UC settings
control all new jobs. The PowerShell wrapper passes its `-Topic` consistently.

Individual jobs, using the same environment and formatting conventions:

```powershell
python -m ingestion.producer --source plant --replay --count 2000 --rate 0
python -m streaming.jobs.bronze_ingest --once
python -m streaming.jobs.plant_silver --once
python -m streaming.jobs.plant_features --once
python -m streaming.jobs.plant_learning train
python -m streaming.jobs.plant_pipeline cycle
```

Omit `--once` on Bronze/Silver/features for long-running streams. Training is a
separate bounded job; control cycles must be serialized. Do not run a standalone
stream and a drain job against the same checkpoint concurrently.

## Safety and operational limits

- Default is shadow. `--simulate` / `-SimulateControl` acknowledges a mock command
  only; there is no physical actuator adapter or network write implementation.
- Scope/config/feature-version mismatch, bad or stale inputs, training overlap,
  failed model gates, model age over seven days, feature drift, no feasible plan,
  override, failed interlock, bounds/rate/dwell violations all prevent application.
- Freshness uses the plant's configured limit (60 seconds by default). Slow local
  drains may produce a legitimate stale-data block; do not weaken that gate to
  make a demo green. Recollect after the jobs are warm.
- State/last-application time comes from durable acknowledged decisions. A feature
  can be simulated at most once; retries return its existing terminal decision.
  Failed/blocked decisions are immutable; a new observation is required to retry.
- Local control uses a cross-process file lock. Databricks batch concurrency is limited to one and cloud simulation is rejected.
  This is not a distributed edge controller or an authenticated approval system.
- Mock feedback represents an illustrative steady-state response, not equipment
  dynamics or measured efficiency. It cannot establish real savings.
- Historical backfill always ends at the source's start time, never in the future.
  Replay is explicit; live producer mode captures at actual current time.

## Cloud and tests

The bundle syncs the shared `plant`, `ingestion`, contracts and streaming modules.
After Lakeflow refresh, `run_plant.py` runs the same `plant_silver` and
`plant_features` streaming jobs with `availableNow`, so each scheduled run reads
only new Bronze rows. Their checkpoints (`plant-silver`, `plant-features`) live on
the `checkpoints` Unity Catalog volume in the target schema.
Its control invocation is always shadow. Run the notebook with `action=train`
after collecting sufficient history. The notebook environment must contain the
repository dependencies, including fastavro. Cloud deployment needs actual
credentials, runtime validation and commissioning; configuration is not proof of deployment.

```powershell
uv run ruff check .
uv run pytest tests/unit -m unit
uv run pytest tests/spark tests/delta
uv run --group dashboard pytest tests/dashboard -m dashboard
```

The Delta regression writes production-format Avro Bronze records and runs real
Structured Streaming checkpoints, training, prediction, command retries and
late-conflict revocation. It does not by itself verify the Kafka transport.

With the local broker running, opt in to the isolated Kafka transport regression:

```powershell
$env:HVACPULSE_TEST_KAFKA = "1"
uv run pytest tests/integration/test_plant_kafka.py -m integration
```

This creates and deletes only its own uniquely named test topic.

### Local verification (2026-09-12)

- Unit/dashboard suite: 123 passed; the opt-in live Kafka test was skipped.
- Spark suite: 3 passed. Delta suite: 7 passed, including checkpoint replay,
  chronological training, durable mock ACK/retry and late-conflict revocation.
- Ruff passed; the running dashboard's charts, date filter, decision audit and
  revoked-model warning were checked against actual mock-test Delta tables.
- Windows Spark initialization was intermittent. The completed Delta run used
  process-local `spark.driver.host=127.0.0.1` and
  `spark.driver.bindAddress=127.0.0.1` overrides, with no permanent network change.
  Spark still logged transient startup and locked-JAR cleanup warnings.
- Docker's Linux engine was unavailable, so the Kafka transport regression was
  not run locally. Databricks deployment and real equipment control were not
  validated. These results demonstrate the mock pipeline, not production readiness.

## Migration

The isolated `python -m pilot` runner, artifact manifest/JSON readers and separate
dashboard tab were removed. Shared domain tests and model/control logic moved to
`plant/`; mock generation moved to `ingestion/`. Existing `.local/pilot-*` output
directories were left untouched as historical artifacts and are no longer read.
Air-handler tables/checkpoints and their jobs remain compatible. New plant tables
and checkpoints have distinct names, so no existing Delta schema is overwritten.
