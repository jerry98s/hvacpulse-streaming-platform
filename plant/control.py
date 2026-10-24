"""Simulated command lifecycle. Deliberately has no BMS write adapter.

Approval actors and interlocks here are test inputs, not authenticated identities
or independently wired safety interlocks. Never connect this class to actuators.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict
from datetime import datetime

from plant.contracts import Plant
from plant.contracts import aware
from plant.optimization import Plan


class MockController:
    def __init__(self, plant: Plant, *, simulate_approval: bool = False):
        self.plant = plant
        self.simulate_approval = simulate_approval
        self.current_supply_c = plant.min_supply_c
        self.last_applied: datetime | None = None
        self.commands: dict[str, dict] = {}
        self.audit: list[dict] = []

    def _record(self, command_id: str, state: str, at: datetime, **detail) -> None:
        self.audit.append(
            {
                "command_id": command_id,
                "scope": asdict(self.plant.scope),
                "state": state,
                "at": at.isoformat(),
                "synthetic": True,
                **detail,
            }
        )

    def propose(self, plan: Plan, now: datetime) -> str:
        aware(now)
        if plan.scope != self.plant.scope or plan.plant_version != self.plant.version:
            raise ValueError("Command scope or configuration version mismatch")
        chillers = {item.equipment_id: item for item in self.plant.equipment if item.kind == "chiller"}
        for equipment_id, load in plan.allocations_kw.items():
            unit = chillers.get(equipment_id)
            if (
                unit is None
                or not math.isfinite(load)
                or not unit.capacity_kw * unit.min_load_fraction <= load <= unit.capacity_kw + 1e-9
            ):
                raise ValueError("Command violates equipment capacity or minimum loading")
        if (
            not math.isfinite(plan.required_load_kw)
            or plan.required_load_kw < 0
            or abs(sum(plan.allocations_kw.values()) - plan.required_load_kw) > 1e-6
            or not math.isfinite(plan.flow_m3h)
            or not 0 <= plan.flow_m3h <= self.plant.max_flow_m3h
            or abs(plan.flow_m3h * 1.163 * self.plant.design_delta_c - plan.required_load_kw) > 1e-6
        ):
            raise ValueError("Command violates load balance or hydraulic envelope")
        payload = {"plan": asdict(plan), "created_at": now.isoformat()}
        command_id = hashlib.sha256(json.dumps(payload, sort_keys=True, allow_nan=False).encode()).hexdigest()
        if command_id not in self.commands:
            self.commands[command_id] = {**payload, "state": "PROPOSED", "approved_by": None}
            self._record(command_id, "PROPOSED", now)
        return command_id

    def approve(self, command_id: str, actor: str, now: datetime) -> None:
        aware(now)
        command = self.commands[command_id]
        if not self.simulate_approval or not actor.strip():
            raise ValueError("Explicit simulated approval mode and actor are required")
        age = (now - datetime.fromisoformat(command["created_at"])).total_seconds()
        if command["state"] != "PROPOSED" or not 0 <= age <= 60:
            raise ValueError("Command is no longer approvable")
        command.update(state="APPROVED", approved_by=actor, approved_at=now.isoformat())
        self._record(command_id, "APPROVED", now, actor=actor)

    def execute(
        self,
        command_id: str,
        now: datetime,
        *,
        eligible: bool,
        interlock_ok: bool,
        manual_override: bool,
        ack: str = "match",
    ) -> str:
        aware(now)
        command = self.commands[command_id]
        if command["state"] not in {"PROPOSED", "APPROVED"}:
            return command["state"]  # Idempotent retry; never apply an ACKed command twice.
        if ack not in {"match", "timeout", "mismatch"}:
            raise ValueError("Unsupported mock acknowledgement")
        plan = command["plan"]
        created = datetime.fromisoformat(command["created_at"])
        elapsed = (now - created).total_seconds()
        target = plan["supply_c"]
        reason = None
        if not self.simulate_approval:
            reason = "SHADOW_ONLY"
        elif not command["approved_by"]:
            reason = "APPROVAL_REQUIRED"
        elif not 0 <= elapsed <= 60 or now < datetime.fromisoformat(command["approved_at"]):
            reason = "COMMAND_EXPIRED_OR_CLOCK_REVERSED"
        elif not eligible or not interlock_ok or manual_override:
            reason = "UNSAFE_INPUT_OR_OPERATOR_OVERRIDE"
        elif not math.isfinite(target) or not self.plant.min_supply_c <= target <= self.plant.max_supply_c:
            reason = "SETPOINT_BOUNDS"
        elif abs(target - self.current_supply_c) > self.plant.max_step_c + 1e-9:
            reason = "RATE_OF_CHANGE"
        elif self.last_applied is not None and (now - self.last_applied).total_seconds() < 900:
            reason = "MINIMUM_COMMAND_INTERVAL"
        if reason:
            command["state"] = "BLOCKED"
            self._record(command_id, "BLOCKED", now, reason=reason)
            return "BLOCKED"
        # No network operation occurs. Only this object's simulated setpoint can change.
        self._record(
            command_id, "SENT_TO_MOCK", now, previous_supply_c=self.current_supply_c, proposed_supply_c=target
        )
        if ack != "match":
            command["state"] = "TIMED_OUT" if ack == "timeout" else "ACK_MISMATCH"
            self._record(command_id, command["state"], now)
            self._record(command_id, "MOCK_ROLLBACK_CONFIRMED", now, supply_c=self.current_supply_c)
        else:
            self.current_supply_c = target
            self.last_applied = now
            command["state"] = "ACKNOWLEDGED"
            self._record(command_id, "ACKNOWLEDGED", now, supply_c=target)
        return command["state"]
