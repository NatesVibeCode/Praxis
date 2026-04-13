"""Closed-loop calibration engine for runtime parameter tuning."""

from __future__ import annotations  # PEP 604 union syntax on 3.9

import json
from collections import deque
from dataclasses import dataclass, asdict
from datetime import datetime, timezone


@dataclass(frozen=True)
class CalibratedParam:
    name: str
    current: float
    min_val: float
    max_val: float
    last_nudge: float
    prediction_accuracy: float | None


@dataclass(frozen=True)
class CalibrationOutcome:
    param_name: str
    decision_value: float
    actual_outcome: bool
    timestamp: datetime


class CalibrationEngine:
    """Maintains calibrated parameters and adjusts them based on recorded outcomes."""

    RING_BUFFER_SIZE = 200

    def __init__(self, params: dict[str, tuple[float, float, float]]) -> None:
        # params: name -> (initial, min, max)
        self._params: dict[str, CalibratedParam] = {}
        self._outcomes: dict[str, deque[CalibrationOutcome]] = {}
        for name, (initial, mn, mx) in params.items():
            self._params[name] = CalibratedParam(
                name=name,
                current=initial,
                min_val=mn,
                max_val=mx,
                last_nudge=0.0,
                prediction_accuracy=None,
            )
            self._outcomes[name] = deque(maxlen=self.RING_BUFFER_SIZE)

    def record_outcome(
        self, param_name: str, decision_value: float, actual_outcome: bool
    ) -> None:
        if param_name not in self._params:
            raise KeyError(f"Unknown parameter: {param_name}")
        outcome = CalibrationOutcome(
            param_name=param_name,
            decision_value=decision_value,
            actual_outcome=actual_outcome,
            timestamp=datetime.now(timezone.utc),
        )
        self._outcomes[param_name].append(outcome)

    def calibrate(
        self,
        param_name: str,
        perturbation_pct: float = 0.10,
        damping: float = 0.5,
    ) -> CalibratedParam:
        if param_name not in self._params:
            raise KeyError(f"Unknown parameter: {param_name}")

        param = self._params[param_name]
        outcomes = self._outcomes[param_name]

        if not outcomes:
            return param

        # Prediction accuracy: fraction of decisions above current threshold that succeeded
        above = [o for o in outcomes if o.decision_value >= param.current]
        if above:
            accuracy = sum(1 for o in above if o.actual_outcome) / len(above)
        else:
            # No decisions above threshold -- treat as too strict
            accuracy = 0.0

        # Determine nudge direction and magnitude
        nudge = 0.0
        if accuracy < 0.5:
            # Too strict: nudge threshold down
            nudge = -param.current * perturbation_pct * damping
        elif accuracy > 0.8:
            # Too lenient: nudge threshold up
            nudge = param.current * perturbation_pct * damping

        new_current = param.current + nudge
        new_current = max(param.min_val, min(param.max_val, new_current))

        updated = CalibratedParam(
            name=param.name,
            current=new_current,
            min_val=param.min_val,
            max_val=param.max_val,
            last_nudge=nudge,
            prediction_accuracy=accuracy,
        )
        self._params[param_name] = updated
        return updated

    def get(self, param_name: str) -> CalibratedParam:
        if param_name not in self._params:
            raise KeyError(f"Unknown parameter: {param_name}")
        return self._params[param_name]

    def all_params(self) -> dict[str, CalibratedParam]:
        return dict(self._params)

    def save(self, path: str) -> None:
        data: dict = {}
        for name, param in self._params.items():
            data[name] = asdict(param)
            data[name]["outcomes"] = [
                {
                    "param_name": o.param_name,
                    "decision_value": o.decision_value,
                    "actual_outcome": o.actual_outcome,
                    "timestamp": o.timestamp.isoformat(),
                }
                for o in self._outcomes[name]
            ]
        with open(path, "w") as f:
            json.dump(data, f, indent=2)

    def load(self, path: str) -> None:
        with open(path) as f:
            data = json.load(f)
        for name, blob in data.items():
            self._params[name] = CalibratedParam(
                name=blob["name"],
                current=blob["current"],
                min_val=blob["min_val"],
                max_val=blob["max_val"],
                last_nudge=blob["last_nudge"],
                prediction_accuracy=blob["prediction_accuracy"],
            )
            ring: deque[CalibrationOutcome] = deque(maxlen=self.RING_BUFFER_SIZE)
            for o in blob.get("outcomes", []):
                ring.append(
                    CalibrationOutcome(
                        param_name=o["param_name"],
                        decision_value=o["decision_value"],
                        actual_outcome=o["actual_outcome"],
                        timestamp=datetime.fromisoformat(o["timestamp"]),
                    )
                )
            self._outcomes[name] = ring
