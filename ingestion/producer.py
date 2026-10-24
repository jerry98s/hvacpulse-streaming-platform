"""Kafka producer CLI for the HVAC simulator.

Produces Avro-serialized telemetry (schemaless binary, schema version carried in
the ``schema_version`` record header) to a Kafka topic. Key is ``device_id`` so
per-equipment ordering is preserved within a partition.

Usage:
    python -m ingestion.producer --bootstrap-servers 127.0.0.1:19092 \
        --topic hvac.telemetry --rate 50 --duration-sec 60
"""

from __future__ import annotations

import argparse
import time
from collections import Counter

from ingestion.simulator import HvacSimulator


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="HVACPulse telemetry producer")
    parser.add_argument("--bootstrap-servers", default="127.0.0.1:19092")
    parser.add_argument("--topic", default="hvac.telemetry")
    parser.add_argument("--rate", type=float, default=50.0, help="events per second")
    parser.add_argument("--duration-sec", type=float, default=60.0)
    parser.add_argument("--count", type=int, default=None, help="override duration with a fixed count")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--anomaly-rate", type=float, default=0.15)
    parser.add_argument("--source", choices=["air", "plant"], default="air")
    parser.add_argument("--plant-config", default=None)
    parser.add_argument("--replay", action="store_true", help="plant only: emit historical 15-minute samples ending now")
    args = parser.parse_args(argv)
    if args.rate < 0 or args.duration_sec <= 0 or (args.count is not None and args.count <= 0):
        parser.error("rate must be nonnegative, duration and count must be positive")
    if not 0 <= args.anomaly_rate <= 1 or (args.replay and args.source != "plant"):
        parser.error("anomaly rate must be within [0,1]; replay requires --source plant")
    return args


def main(argv: list[str] | None = None) -> int:
    from kafka import KafkaProducer

    args = parse_args(argv)
    # Reliability (Kafka "three-tier defense", producer tier): acks=all waits
    # for all in-sync replicas; one in-flight request keeps ordering safe
    # under retries. kafka-python does not support enable.idempotence, so
    # exactly-once is enforced downstream (checkpoint + Delta MERGE), matching
    # the consumer tier where Spark never auto-commits offsets.
    # Performance: 64 KB batches, 50 ms linger, lz4 compression.
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        key_serializer=None,
        value_serializer=None,
        acks="all",
        retries=10,
        max_in_flight_requests_per_connection=1,
        batch_size=64 * 1024,
        linger_ms=50,
        compression_type="lz4",
    )
    if args.source == "plant":
        from pathlib import Path

        from ingestion.plant_events import PlantSimulator
        from plant.contracts import Plant
        from streaming.config import load_settings

        config = Path(args.plant_config) if args.plant_config else load_settings().plant_config
        simulator = PlantSimulator(Plant.load(config), seed=args.seed, replay=args.replay, anomaly_rate=args.anomaly_rate)
    else:
        simulator = HvacSimulator(seed=args.seed, anomaly_rate=args.anomaly_rate)
    total = args.count if args.count is not None else int(args.rate * args.duration_sec)
    interval = 1.0 / args.rate if args.rate > 0 else 0.0

    stats: Counter[str] = Counter()
    start = time.monotonic()
    pending = []
    try:
        for wire in simulator.events(total):
            pending.append(producer.send(args.topic, key=wire.key, value=wire.value, headers=list(wire.headers)))
            stats[wire.label] += 1
            if len(pending) >= 256:
                for future in pending:
                    future.get(timeout=60)
                pending.clear()
            if interval:
                elapsed = time.monotonic() - start
                target = sum(stats.values()) * interval
                if target > elapsed:
                    time.sleep(target - elapsed)
        for future in pending:
            future.get(timeout=60)
        producer.flush()
    finally:
        producer.close()

    sent = sum(stats.values())
    elapsed = time.monotonic() - start
    print(f"Produced {sent} events in {elapsed:.1f}s ({sent / max(elapsed, 1e-6):.1f} events/s)")
    for label, n in sorted(stats.items()):
        print(f"  {label:<22} {n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
