"""Runtime configuration. One shared codebase; environments differ by env vars."""

from __future__ import annotations

import os
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from urllib.parse import unquote
from urllib.parse import urlparse

REPO_ROOT = Path(__file__).resolve().parents[1]


def filesystem_path(spark_path: str) -> Path:
    """Convert a Spark file URI (or a plain path) back to a local Path."""
    if not spark_path.startswith("file:"):
        return Path(spark_path)
    parsed = urlparse(spark_path)
    path = unquote(parsed.path)
    if os.name == "nt" and path.startswith("/") and len(path) >= 3 and path[2] == ":":
        path = path.lstrip("/")
    return Path(path)


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str = "127.0.0.1:19092"
    topic: str = "hvac.telemetry"
    root: Path = field(default=REPO_ROOT / ".local")
    starting_offsets: str = "earliest"
    watermark: str = "10 minutes"
    max_offsets_per_trigger: int | None = None
    known_devices_csv: Path = field(default=REPO_ROOT / "reference" / "equipment_master.csv")
    catalog: str | None = None
    schema: str | None = None
    checkpoint_root: str | None = None
    plant_config: Path = field(default=REPO_ROOT / "reference" / "mock_plant.json")

    @property
    def plant_silver_path(self) -> str:
        return self._delta_target("silver_plant_snapshots")

    @property
    def plant_quarantine_path(self) -> str:
        return self._delta_target("quarantine_plant_snapshots")

    @property
    def plant_features_path(self) -> str:
        return self._delta_target("gold_plant_features")

    @property
    def plant_models_path(self) -> str:
        return self._delta_target("plant_model_registry")

    @property
    def plant_predictions_path(self) -> str:
        return self._delta_target("gold_plant_predictions")

    @property
    def plant_decisions_path(self) -> str:
        return self._delta_target("gold_plant_control_decisions")

    @property
    def uses_unity_catalog(self) -> bool:
        return self.catalog is not None and self.schema is not None

    def _uri(self, *parts: str) -> str:
        """Spark/Hadoop-safe file URI (file:///C:/... on Windows)."""
        path = self.root.joinpath(*parts).resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        return path.as_uri()

    def _delta_target(self, name: str) -> str:
        if self.uses_unity_catalog:
            return f"{self.catalog}.{self.schema}.{name}"
        return self._uri("delta", name)

    def table_target(self, name: str) -> str:
        """Unity Catalog name or local Delta URI for a table name."""
        return self._delta_target(name)

    def is_table_target(self, target: str) -> bool:
        prefix = f"{self.catalog}.{self.schema}." if self.uses_unity_catalog else None
        return prefix is not None and target.startswith(prefix)

    def delta_exists(self, spark, target: str) -> bool:
        if self.is_table_target(target):
            return spark.catalog.tableExists(target)
        return (filesystem_path(target) / "_delta_log").exists()

    def read_delta(self, spark, target: str):
        if self.is_table_target(target):
            return spark.table(target)
        return spark.read.format("delta").load(target)

    def read_delta_stream(self, spark, target: str):
        if self.is_table_target(target):
            return spark.readStream.table(target)
        return spark.readStream.format("delta").load(target)

    def write_delta(
        self,
        df,
        target: str,
        *,
        mode: str = "error",
        overwrite_schema: bool = False,
        partition_by: list[str] | None = None,
    ) -> None:
        writer = df.write.format("delta").mode(mode)
        if overwrite_schema:
            writer = writer.option("overwriteSchema", "true")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        if self.is_table_target(target):
            writer.saveAsTable(target)
        else:
            writer.save(target)

    def delta_table(self, spark, target: str):
        from delta.tables import DeltaTable

        if self.is_table_target(target):
            return DeltaTable.forName(spark, target)
        return DeltaTable.forPath(spark, target)

    @property
    def bronze_path(self) -> str:
        return self._delta_target("bronze_telemetry")

    @property
    def silver_history_path(self) -> str:
        return self._delta_target("silver_telemetry_history")

    @property
    def silver_current_path(self) -> str:
        return self._delta_target("silver_telemetry_current")

    @property
    def quarantine_path(self) -> str:
        return self._delta_target("quarantine_events")

    @property
    def gold_current_path(self) -> str:
        return self._delta_target("gold_hvac_metrics_current")

    @property
    def gold_history_path(self) -> str:
        return self._delta_target("gold_hvac_metrics_history")

    @property
    def gold_recommendations_path(self) -> str:
        return self._delta_target("gold_recommendations")

    @property
    def recommendation_events_path(self) -> str:
        return self._delta_target("recommendation_events")

    @property
    def late_corrections_path(self) -> str:
        return self._delta_target("late_corrections")

    @property
    def restatement_events_path(self) -> str:
        return self._delta_target("metric_restatement_events")

    @property
    def gold_dq_scorecard_path(self) -> str:
        return self._delta_target("gold_data_quality_scorecard")

    @property
    def gold_sensor_freshness_path(self) -> str:
        return self._delta_target("gold_sensor_freshness")

    @property
    def gold_compliance_path(self) -> str:
        return self._delta_target("gold_compliance")

    @property
    def gold_equipment_health_path(self) -> str:
        return self._delta_target("gold_equipment_health")

    def checkpoint(self, name: str) -> str:
        """Checkpoint location. On Databricks this MUST point at reliable
        storage (ADLS/Unity Catalog volume), never local disk — set
        HVACPULSE_CHECKPOINT_ROOT to e.g. abfss://.../checkpoints."""
        if self.checkpoint_root:
            return f"{self.checkpoint_root.rstrip('/')}/{name}"
        return self._uri("checkpoints", name)


def ensure_uc_checkpoint_root(spark, catalog: str, schema: str) -> str:
    """Unity Catalog volume that holds streaming checkpoints on Databricks."""
    if not catalog or not schema:
        raise ValueError("catalog and schema are required for a Unity Catalog checkpoint volume")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`checkpoints`")
    return f"/Volumes/{catalog}/{schema}/checkpoints"


def load_settings() -> Settings:
    max_offsets = os.environ.get("HVACPULSE_MAX_OFFSETS_PER_TRIGGER")
    return Settings(
        kafka_bootstrap=os.environ.get("HVACPULSE_KAFKA_BOOTSTRAP", "127.0.0.1:19092"),
        topic=os.environ.get("HVACPULSE_TOPIC", "hvac.telemetry"),
        root=Path(os.environ.get("HVACPULSE_DATA_ROOT", str(REPO_ROOT / ".local"))),
        starting_offsets=os.environ.get("HVACPULSE_STARTING_OFFSETS", "earliest"),
        watermark=os.environ.get("HVACPULSE_WATERMARK", "10 minutes"),
        max_offsets_per_trigger=int(max_offsets) if max_offsets else None,
        catalog=os.environ.get("HVACPULSE_CATALOG"),
        schema=os.environ.get("HVACPULSE_SCHEMA"),
        checkpoint_root=os.environ.get("HVACPULSE_CHECKPOINT_ROOT"),
        plant_config=Path(os.environ.get("HVACPULSE_PLANT_CONFIG", str(REPO_ROOT / "reference/mock_plant.json"))),
    )
