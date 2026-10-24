# Runbook: Azure deployment (Event Hubs + ADLS + Azure Databricks)

Production-style path using **Azure services**: Event Hubs (Kafka endpoint),
ADLS Gen2 for Delta, and **Azure Databricks** for Lakeflow + jobs.

> If you only have **Databricks Free Edition** (no Azure), use
> [free-edition.md](./free-edition.md) instead.

---

## Prerequisites

- Azure subscription with permission to create Resource Groups
- [Terraform](https://developer.hashicorp.com/terraform/downloads) ≥ 1.6
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/) authenticated
- Azure CLI (`az login`) or environment auth for Terraform

---

## Step 1 — Provision Azure infrastructure

```powershell
cd infra\terraform
copy dev.tfvars.example terraform.tfvars   # edit prefix/region
terraform init
terraform plan
terraform apply
```

Capture outputs:

```powershell
terraform output -raw eventhub_kafka_endpoint   # <ns>.servicebus.windows.net:9093
terraform output -raw eventhub_name             # hvac-telemetry
terraform output -raw adls_account_name
```

Get the Event Hubs connection string:

```powershell
az eventhubs namespace authorization-rule keys list `
  --resource-group rg-hvacpulse `
  --namespace-name evhns-hvacpulse `
  --name RootManageSharedAccessKey `
  --query primaryConnectionString -o tsv
```

---

## Step 2 — Store secret in Databricks

Create a secret scope and add the connection string (do not commit it):

```powershell
databricks secrets create-scope hvacpulse
databricks secrets put-secret hvacpulse eventhubs-connection --string-value "<connection-string>"
```

The bundle expects the secret reference in the form `scope:key`, e.g.
`hvacpulse:eventhubs-connection`.

---

## Step 3 — Deploy the Databricks bundle

From the repo root:

```powershell
databricks auth login   # select your Azure Databricks workspace

databricks bundle deploy -t dev `
  --var "eventhubs_bootstrap=<namespace>.servicebus.windows.net:9093" `
  --var "eventhubs_connection_secret=hvacpulse:eventhubs-connection" `
  --var "catalog=<your-catalog>" `
  --var "schema=hvacpulse"
```

`catalog`/`schema` must exist in Unity Catalog (create once):

```sql
CREATE CATALOG IF NOT EXISTS <your-catalog>;
CREATE SCHEMA IF NOT EXISTS <your-catalog>.hvacpulse;
```

This deploys:

- **Pipeline** `hvacpulse-pipeline-dev` — serverless Lakeflow streaming table
  `bronze_telemetry`, reading Event Hubs over Kafka. Lakeflow manages its
  checkpoint. The table sets `pipelines.reset.allowed = false`, so a full
  pipeline refresh does not truncate Bronze (Event Hubs keeps only 7 days and
  could not rebuild older history).
- **Job** `hvacpulse-batch-dev` — refreshes the pipeline, then runs incremental
  Silver/quarantine (`run_silver.py`) and plant (`run_plant.py`) tasks, followed
  by Gold, quality, recommendation, and reconciliation notebooks against the
  same Unity Catalog tables.

The Silver and plant tasks create a Unity Catalog volume
`<catalog>.<schema>.checkpoints` on first run and keep their streaming
checkpoints there (`silver`, `plant-silver`, `plant-features`). The job identity
needs `CREATE VOLUME` on the schema. Do not delete that volume while keeping the
Silver tables; the next run would reprocess all of Bronze.

### One-time migration from the materialized-view Silver

Earlier versions created Silver as Lakeflow materialized views. The Silver task
now owns those table names, so remove the old views once, before the first
scheduled run of this version:

1. Pause the `hvacpulse-batch-dev` schedule.
2. Deploy, then run the pipeline once so it stops managing the old datasets:
   `databricks bundle run -t dev hvacpulse_pipeline`.
3. In a SQL editor, drop any that remain:

```sql
DROP MATERIALIZED VIEW IF EXISTS <your-catalog>.hvacpulse.parsed_telemetry;
DROP MATERIALIZED VIEW IF EXISTS <your-catalog>.hvacpulse.telemetry_idempotency_conflicts;
DROP MATERIALIZED VIEW IF EXISTS <your-catalog>.hvacpulse.canonical_telemetry_events;
DROP MATERIALIZED VIEW IF EXISTS <your-catalog>.hvacpulse.quarantine_events;
DROP MATERIALIZED VIEW IF EXISTS <your-catalog>.hvacpulse.silver_telemetry_history;
DROP MATERIALIZED VIEW IF EXISTS <your-catalog>.hvacpulse.silver_telemetry_current;
```

4. Run `databricks bundle run -t dev hvacpulse_batch` and resume the schedule.
   The first Silver run reads all of Bronze; later runs read only new rows.

---

## Step 4 — Run once, then let the schedule run

Trigger manually for the first run:

```powershell
databricks bundle run -t dev hvacpulse_batch
```

The job is scheduled every 5 minutes (see `databricks.yml`). Free/trial
workspaces may have quota limits; pause the schedule between demos.

---

## Step 5 — Point your producer at Azure

Use the same simulator, but target Event Hubs:

```powershell
$env:HVACPULSE_KAFKA_BOOTSTRAP = "<namespace>.servicebus.windows.net:9093"
$env:HVACPULSE_TOPIC = "hvac-telemetry"

# For Azure Kafka you need SASL_SSL; the local producer uses PLAINTEXT.
# Use the Databricks notebook or an Azure-native producer for cloud ingestion.
```

For a portfolio demo, it is often easier to keep ingestion local, upload Delta
tables (see [free-edition.md](./free-edition.md)), or use a small Azure Function
/ container app producer that supports SASL_SSL.

---

## Destroy (avoid ongoing cost)

```powershell
databricks bundle destroy -t dev
cd infra\terraform
terraform destroy
```

Event Hubs and ADLS are the main Azure cost drivers; Databricks serverless
usage depends on your workspace pricing tier.

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `secrets get: scope does not exist` | Re-run `databricks secrets create-scope hvacpulse` |
| Pipeline cannot reach Event Hubs | Check firewall/NSG and that the namespace allows Kafka (Standard SKU) |
| `TABLE_OR_VIEW_NOT_FOUND` | Run the import/pipeline first; verify catalog/schema widgets |
| Bundle var rejected | Ensure secret value is `scope:key`, not the raw connection string |
| Job fails on `source_path` | `databricks bundle deploy` syncs `contracts/`, `quality/`, `reference/`, `streaming/`, `plant/`, `ingestion/`; re-deploy after code changes |
| Silver task fails because `silver_telemetry_*` is a materialized view | Run the one-time migration above |
| `CREATE VOLUME` permission denied | Grant the job identity `CREATE VOLUME` on the schema, or create `<catalog>.<schema>.checkpoints` once as an admin |
| A Bronze change truly needs a reset | Remove `pipelines.reset.allowed` deliberately, and only once Bronze has been backed up; a reset rebuilds only the 7 days Event Hubs retains |
