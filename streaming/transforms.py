"""Bronze→Silver DataFrame transforms: Avro parse, flatten, hard validation.

Validation here mirrors ``quality.rules`` (the pure-Python reference) using
Spark SQL expressions so the hot path stays free of Python UDFs. Error codes
and rule precedence are intentionally identical.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import DataFrame
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import coalesce
from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import expr
from pyspark.sql.functions import hex as hex_
from pyspark.sql.functions import lit
from pyspark.sql.functions import when

from quality import rules
from streaming.schemas import MEASUREMENT_COLUMNS
from streaming.schemas import SILVER_COLUMNS
from streaming.schemas import avro_schema_json

SCHEMA_VERSION_HEADER = "wire_schema_version"
_KAFKA_SCHEMA_VERSION_KEY = "schema_version"
_AVRO_OPTIONS = {"mode": "PERMISSIVE"}


def with_schema_version_header(df: DataFrame) -> DataFrame:
    """Extract the producer-set ``schema_version`` Kafka record header."""
    return df.withColumn(
        SCHEMA_VERSION_HEADER,
        expr(
            f"CASE WHEN size(filter(headers, h -> h.key = '{_KAFKA_SCHEMA_VERSION_KEY}')) = 1 "
            "THEN try_element_at(transform("
            f"filter(headers, h -> h.key = '{_KAFKA_SCHEMA_VERSION_KEY}'), "
            "h -> CAST(h.value AS STRING)), 1) ELSE NULL END"
        ),
    )


def parse_bronze(df: DataFrame) -> DataFrame:
    """Parse the Avro payload with the schema matching its header version.

    v1 and v2 are parsed into separate columns so the nested measurement
    structs do not have to unify. Unparseable payloads keep both null.
    """
    return (
        df.withColumn("parsed_v1", from_avro(col("value"), avro_schema_json(1), _AVRO_OPTIONS))
        .withColumn("parsed_v2", from_avro(col("value"), avro_schema_json(2), _AVRO_OPTIONS))
    )


def _from_parsed(field: str):
    v1 = col("parsed_v1").getField(field)
    v2 = col("parsed_v2").getField(field)
    return when(col(SCHEMA_VERSION_HEADER) == "1", v1).when(col(SCHEMA_VERSION_HEADER) == "2", v2)


def _measurement(name: str):
    if name == "co2_ppm":
        return when(
            col(SCHEMA_VERSION_HEADER) == "2",
            col("parsed_v2").getField("measurements").getField("co2_ppm"),
        )
    v1 = col("parsed_v1").getField("measurements").getField(name)
    v2 = col("parsed_v2").getField("measurements").getField(name)
    return when(col(SCHEMA_VERSION_HEADER) == "1", v1).when(col(SCHEMA_VERSION_HEADER) == "2", v2)


def flatten_parsed(df: DataFrame) -> DataFrame:
    """Flatten parsed Avro structs into canonical Silver columns."""
    selected = df.select(
        _from_parsed("event_id").alias("event_id"),
        _from_parsed("logical_event_id").alias("logical_event_id"),
        _from_parsed("revision_number").alias("revision_number"),
        _from_parsed("supersedes_event_id").alias("supersedes_event_id"),
        _from_parsed("schema_id").alias("schema_id"),
        _from_parsed("schema_version").alias("schema_version"),
        _from_parsed("firmware_version").alias("firmware_version"),
        _from_parsed("event_time").alias("event_time_str"),
        _from_parsed("gateway_id").alias("gateway_id"),
        _from_parsed("device_id").alias("device_id"),
        _from_parsed("boot_id").alias("boot_id"),
        _from_parsed("sequence_number").alias("sequence_number"),
        _from_parsed("site_id").alias("site_id"),
        _from_parsed("zone_id").alias("zone_id"),
        *[_measurement(m).alias(m) for m in MEASUREMENT_COLUMNS],
        when(col(SCHEMA_VERSION_HEADER) == "1", col("parsed_v1").getField("operating_state").getField("mode"))
        .when(col(SCHEMA_VERSION_HEADER) == "2", col("parsed_v2").getField("operating_state").getField("mode"))
        .alias("mode"),
        when(
            col(SCHEMA_VERSION_HEADER) == "1",
            col("parsed_v1").getField("operating_state").getField("temperature_setpoint_c"),
        )
        .when(
            col(SCHEMA_VERSION_HEADER) == "2",
            col("parsed_v2").getField("operating_state").getField("temperature_setpoint_c"),
        )
        .alias("temperature_setpoint_c"),
        when(
            col(SCHEMA_VERSION_HEADER) == "1",
            col("parsed_v1").getField("operating_state").getField("equipment_running"),
        )
        .when(
            col(SCHEMA_VERSION_HEADER) == "2",
            col("parsed_v2").getField("operating_state").getField("equipment_running"),
        )
        .alias("equipment_running"),
        col("payload_hash"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("kafka_timestamp"),
        col("ingest_ts").alias("bronze_ingest_ts"),
        col("ingest_ts").cast("date").alias("ingest_date"),
        col(SCHEMA_VERSION_HEADER),
        hex_(col("value")).alias("payload_hex"),
        (
            ((col(SCHEMA_VERSION_HEADER) == "1") & col("parsed_v1").isNotNull())
            | ((col(SCHEMA_VERSION_HEADER) == "2") & col("parsed_v2").isNotNull())
        ).alias("parse_ok"),
    )
    return selected.withColumn("event_time", expr("try_cast(event_time_str AS TIMESTAMP)"))


def _in_physical_range(field: str) -> Column:
    c = col(field)
    return c.isNull() | (
        (c >= lit(rules.PHYSICAL_TEMP_MIN_C)) & (c <= lit(rules.PHYSICAL_TEMP_MAX_C))
    )


def with_validation(
    df: DataFrame,
    *,
    known_devices: set[str] | None = None,
    max_lateness_days: int = 7,
) -> DataFrame:
    """Add ``error_code``: NULL for valid rows, first hard-failure code otherwise.

    Precedence mirrors ``quality.rules.validate_event``: structural → schema
    version → identity → timestamp → lateness → physical plausibility.
    """
    error: Column = when(
        ~coalesce(col(SCHEMA_VERSION_HEADER).isin("1", "2"), lit(False)),
        lit(rules.UNSUPPORTED_SCHEMA_VERSION),
    ).when(~col("parse_ok"), lit(rules.SCHEMA_INVALID))

    if known_devices:
        error = error.when(~col("device_id").isin(*sorted(known_devices)), lit(rules.EQUIPMENT_UNKNOWN))

    error = (
        error.when(col("event_time_str").isNull(), lit(rules.SCHEMA_INVALID))
        .when(col("event_time").isNull(), lit(rules.TIMESTAMP_INVALID))
        .when(
            col("event_time") < current_timestamp() - expr(f"INTERVAL {max_lateness_days} DAYS"),
            lit(rules.EVENT_TOO_LATE),
        )
        .when(col("relative_humidity_pct").isNull(), lit(rules.HUMIDITY_MISSING))
        .when(
            (col("relative_humidity_pct") < 0.0) | (col("relative_humidity_pct") > 100.0),
            lit(rules.HUMIDITY_OUT_OF_RANGE),
        )
        .when(col("power_kw") < 0.0, lit(rules.NEGATIVE_POWER))
        .when(
            (col("fan_speed_pct") < 0.0)
            | (col("fan_speed_pct") > 100.0)
            | (col("chilled_water_valve_pct") < 0.0)
            | (col("chilled_water_valve_pct") > 100.0),
            lit(rules.FAN_VALVE_OUT_OF_RANGE),
        )
        .when(
            ~(_in_physical_range("supply_air_temp_c"))
            | ~(_in_physical_range("return_air_temp_c"))
            | ~(_in_physical_range("zone_temp_c")),
            lit(rules.TEMPERATURE_PHYSICALLY_IMPOSSIBLE),
        )
        .otherwise(lit(None))
    )
    return df.withColumn("error_code", error)


def silver_projection(df: DataFrame) -> DataFrame:
    """Project a validated batch to exactly the Silver column contract.
    
    Adds silver_commit_ts to mark when each row becomes observable in Silver,
    distinct from bronze_ingest_ts (when it landed in Bronze).
    """
    return df.select(*[c for c in SILVER_COLUMNS if c != "silver_commit_ts"]).withColumn(
        "silver_commit_ts", current_timestamp()
    )
