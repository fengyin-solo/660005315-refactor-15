"""温度 / 振动 / 压力异常检测的共用实现。

模拟流程（app.main.simulate）每个周期通过 AnomalyEngine.evaluate() 驱动状态；
上报流程（WebSocket 推送与 REST 接口）通过 AnomalyEngine.recent() /
consecutive_counts() 只读取值。两段流程共用同一份阈值、连续超限周期累计与
走势上升判断口径，不再各写一遍。
"""
import time
from collections import defaultdict, deque

import numpy as np

# 三类阈值的唯一口径来源（模拟与上报共用）
THRESHOLD_RULES = (
    {"name": "高温告警", "field": "temperature", "threshold": 48, "op": "gt"},
    {"name": "振动超标", "field": "vibration", "threshold": 2.0, "op": "gt"},
    {"name": "压力异常", "field": "pressure", "threshold": 1.5, "op": "gt"},
)

# 温度走势上升判断口径
TREND_WINDOW = 10        # 每台设备保留的滑动窗口长度（周期）
TREND_MIN_SAMPLES = 8    # 样本达到该数量后才开始判断走势
TREND_SPAN = 4           # 窗口首段 / 末段各取的样本数
TREND_DELTA = 3          # 末段均值 - 首段均值 > 3°C 判定为趋势上升
TREND_RULE_NAME = "温度趋势上升"
TREND_THRESHOLD = ">3°C/周期"


def over_threshold(value, op, threshold):
    """按规则操作符判断是否超限，阈值校验与周期累计共用此比较。"""
    return (op == "gt" and value > threshold) or \
           (op == "lt" and value < threshold)


def check_thresholds(dev):
    """三类指标的纯阈值校验：返回当前周期的超限触发，不持有状态。"""
    triggers = []
    for rule in THRESHOLD_RULES:
        val = getattr(dev, rule["field"])
        if over_threshold(val, rule["op"], rule["threshold"]):
            triggers.append({
                "device_id": dev.id,
                "rule": rule["name"],
                "value": round(val, 3),
                "threshold": rule["threshold"],
            })
    return triggers


class AnomalyEngine:
    """持有跨周期状态（走势窗口、连续超限计数、异常日志）的共用引擎。"""

    def __init__(self):
        self.windows = defaultdict(lambda: deque(maxlen=TREND_WINDOW))
        self._consecutive = defaultdict(
            lambda: {rule["field"]: 0 for rule in THRESHOLD_RULES}
        )
        self.anomaly_log = []

    # ---- 模拟侧：每个周期对每台设备求值一次 ----
    def evaluate(self, dev):
        # 1) 三类阈值校验
        triggers = check_thresholds(dev)

        # 2) 连续超限的周期累计：超限 +1，恢复正常立即归零；
        #    与阈值校验使用同一个 over_threshold 比较口径
        counters = self._consecutive[dev.id]
        for rule in THRESHOLD_RULES:
            field = rule["field"]
            if over_threshold(getattr(dev, field), rule["op"], rule["threshold"]):
                counters[field] += 1
            else:
                counters[field] = 0

        # 3) 温度滑动窗口走势上升判断
        key = f"{dev.id}_temp"
        self.windows[key].append(dev.temperature)
        if len(self.windows[key]) >= TREND_MIN_SAMPLES:
            vals = list(self.windows[key])
            if np.mean(vals[-TREND_SPAN:]) - np.mean(vals[:TREND_SPAN]) > TREND_DELTA:
                triggers.append({
                    "device_id": dev.id,
                    "rule": TREND_RULE_NAME,
                    "value": round(np.mean(vals[-TREND_SPAN:]), 2),
                    "threshold": TREND_THRESHOLD,
                })

        if triggers:
            self.anomaly_log.append({
                "timestamp": time.time(),
                "triggers": triggers,
                "device_type": dev.type,
            })
        return triggers

    # ---- 上报侧：只读取值，不改变任何周期状态 ----
    def recent(self, limit):
        """最近若干条异常记录，供 WebSocket 推送与 REST 接口使用。"""
        return self.anomaly_log[-limit:] if self.anomaly_log else []

    def consecutive_counts(self, device_id):
        """某台设备三类指标当前连续超限的周期数。"""
        return dict(self._consecutive[device_id])
