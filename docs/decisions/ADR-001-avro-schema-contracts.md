# ADR-001: Avro wire contracts with version-tolerant local parsing

## Status

Accepted

## Context

Gateways on different firmware versions emit different payload shapes. The
platform must keep processing v1 and v2 events concurrently, quarantine
unsupported versions, and never let an unapproved field silently mutate Silver.

## Decision

- Wire format is Avro (schemaless binary) for both local Kafka and Azure Event
  Hubs. The JSON in the README is only the logical representation.
- Contracts are version-controlled under `contracts/avro/`.
- The schema version travels in the Kafka record header (`schema_version`), so
  Bronze stays byte-immutable and Silver picks the correct reader schema per
  record without a registry dependency locally.
- Azure deployment registers the same contracts in Event Hubs Schema Registry
  with `BACKWARD` compatibility; `contracts/check_compatibility.py` enforces
  transitive compatibility in CI across *all* retained versions (Azure's mode
  is not expressed as Confluent's `BACKWARD_TRANSITIVE`).
- `spark.databricks.delta.schema.autoMerge.enabled = false` everywhere;
  additive changes land via explicit DDL.

## Consequences

- Breaking changes (e.g. `temperature_c: DOUBLE` → `temperature_f: STRING`)
  require an explicit version-specific adapter (see `quality.rules.normalize_units`)
  and are never enabled automatically.
- Local development needs no Schema Registry container; CI still proves the
  compatibility invariant the registry would enforce in Azure.
