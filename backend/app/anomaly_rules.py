from collections import defaultdict, deque
from time import time

THRESHOLD_RULES = [
    {"name": "高温告警", "field": "temperature", "threshold": 48, "op": "gt"},
    {"name": "振动超标", "field": "vibration", "threshold": 2.0, "op": "gt"},
    {"name": "压力异常", "field": "pressure", "threshold": 1.5, "op": "gt"},
]

TREND_WINDOW_SIZE = 10
TREND_MIN_SAMPLES = 8
TREND_GROUP_SIZE = 4
TREND_RISE_THRESHOLD = 3.0


class AnomalyRules:
    def __init__(self):
        self.rules = [rule.copy() for rule in THRESHOLD_RULES]
        self.windows = defaultdict(
            lambda: deque(maxlen=TREND_WINDOW_SIZE)
        )
        self.over_limit_cycles = defaultdict(
            lambda: defaultdict(int)
        )
        self.anomaly_log = []

    def _is_over_threshold(self, value, rule):
        if rule["op"] == "gt":
            return value > rule["threshold"]
        if rule["op"] == "lt":
            return value < rule["threshold"]
        return False

    def _check_thresholds(self, dev):
        triggers = []
        for rule in self.rules:
            value = getattr(dev, rule["field"])
            if self._is_over_threshold(value, rule):
                self.over_limit_cycles[dev.id][rule["field"]] += 1
                triggers.append({
                    "device_id": dev.id,
                    "rule": rule["name"],
                    "value": round(value, 3),
                    "threshold": rule["threshold"],
                })
            else:
                self.over_limit_cycles[dev.id][rule["field"]] = 0
        return triggers

    def _check_temperature_trend(self, dev):
        key = f"{dev.id}_temp"
        window = self.windows[key]
        window.append(dev.temperature)

        if len(window) < TREND_MIN_SAMPLES:
            return []

        values = list(window)
        latest_mean = sum(values[-TREND_GROUP_SIZE:]) / TREND_GROUP_SIZE
        previous_mean = sum(values[:TREND_GROUP_SIZE]) / TREND_GROUP_SIZE
        if latest_mean - previous_mean > TREND_RISE_THRESHOLD:
            return [{
                "device_id": dev.id,
                "rule": "温度趋势上升",
                "value": round(latest_mean, 2),
                "threshold": ">3°C/周期",
            }]
        return []

    def check(self, dev):
        triggers = self._check_thresholds(dev)
        triggers.extend(self._check_temperature_trend(dev))

        if triggers:
            self.anomaly_log.append({
                "timestamp": time(),
                "triggers": triggers,
                "device_type": dev.type,
            })
        return triggers

    def recent(self, limit):
        return self.anomaly_log[-limit:] if self.anomaly_log else []


rules_engine = AnomalyRules()
