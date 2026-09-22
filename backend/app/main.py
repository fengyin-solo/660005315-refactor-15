import asyncio, math, random, time, json, threading
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from .anomaly_rules import rules_engine

app = FastAPI(title="Digital Twin Factory Monitor")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

DEVICE_TYPES = ["CNC", "RobotArm", "Conveyor", "AGV", "InjectionMolding", "QCStation"]
STATUSES = ["RUNNING", "IDLE", "FAULT", "OFFLINE"]
ACTIVE_CLIENTS: list[WebSocket] = []
SIMULATOR_RUNNING = True

class DeviceState:
    def __init__(self, did: int, dtype: str, x: float, y: float, z: float):
        self.id = did
        self.type = dtype
        self.status = "RUNNING"
        self.position = [x, y, z]
        self.temperature = random.uniform(35, 45)
        self.vibration = random.uniform(0.1, 1.5)
        self.pressure = random.uniform(0.8, 1.2)
        self.production_count = 0
        self.fault_count = 0
        self.uptime = 0.0
        self.cycle_time = random.uniform(2, 8)
        self.quality_rate = random.uniform(0.95, 0.995)

    def to_dict(self):
        return {
            "id": self.id, "type": self.type, "status": self.status,
            "position": self.position, "temperature": round(self.temperature, 2),
            "vibration": round(self.vibration, 3), "pressure": round(self.pressure, 2),
            "production_count": self.production_count, "fault_count": self.fault_count,
            "uptime": round(self.uptime, 2), "quality_rate": round(self.quality_rate, 3)
        }

devices = {i: DeviceState(i, random.choice(DEVICE_TYPES),
                          random.uniform(-5, 5), 0.5, random.uniform(-5, 5)) for i in range(1, 13)}

production_log = []

def simulate():
    while SIMULATOR_RUNNING:
        for dev in devices.values():
            drift = 0.1 * math.sin(time.time() * 0.5 + dev.id)
            noise = random.gauss(0, 0.3)
            dev.temperature = max(25, min(65, dev.temperature + drift + noise))

            v_drift = 0.02 * math.sin(time.time() * 0.3 + dev.id * 0.7)
            dev.vibration = max(0, min(3, dev.vibration + v_drift + random.gauss(0, 0.05)))

            dev.pressure = max(0.5, min(2, dev.pressure + random.gauss(0, 0.02)))

            if random.random() < 0.015:
                dev.status = "FAULT"
                dev.fault_count += 1
            elif random.random() < 0.03 and dev.status == "FAULT":
                dev.status = "RUNNING"

            if dev.status == "RUNNING":
                if random.random() < 0.4:
                    dev.production_count += 1
                dev.uptime += 1

            triggers = rules_engine.check(dev)
            if triggers and dev.status != "FAULT" and random.random() < 0.3:
                dev.status = "FAULT"

        production_log.append({"timestamp": time.time(), "count": sum(d.production_count for d in devices.values())})

        try:
            payload = {
                "devices": [d.to_dict() for d in devices.values()],
                "production": sum(d.production_count for d in devices.values()),
                "anomalies": rules_engine.recent(5),
                "oee": calculate_oee()
            }
            msg = json.dumps(payload)
        except:
            continue

        dead = []
        for ws in ACTIVE_CLIENTS:
            try:
                asyncio.run_coroutine_threadsafe(ws.send_text(msg), asyncio.get_event_loop())
            except:
                dead.append(ws)
        for ws in dead:
            if ws in ACTIVE_CLIENTS:
                ACTIVE_CLIENTS.remove(ws)

        time.sleep(1)


def calculate_oee():
    oee_list = []
    for dev in devices.values():
        if dev.uptime == 0:
            continue
        availability = min(1.0, dev.uptime / max(1, dev.uptime + dev.fault_count))
        performance = min(1.0, dev.production_count / max(1, dev.uptime / 2))
        quality = dev.quality_rate
        oee = round(availability * performance * quality * 100, 1)
        oee_list.append({"id": dev.id, "type": dev.type, "oee": oee,
                         "availability": round(availability * 100, 1),
                         "performance": round(performance * 100, 1),
                         "quality": round(quality * 100, 1)})
    return oee_list


class OEEAnalysis(BaseModel):
    availability: float
    performance: float
    quality: float


@app.on_event("startup")
async def startup():
    t = threading.Thread(target=simulate, daemon=True)
    t.start()


@app.get("/api/devices")
def get_devices():
    return {"devices": [d.to_dict() for d in devices.values()], "anomalies": rules_engine.recent(10)}


@app.get("/api/oee")
def get_oee():
    return {"oee": calculate_oee()}


@app.get("/api/production")
def get_production():
    return {"log": production_log[-60:]}


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    ACTIVE_CLIENTS.append(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        if websocket in ACTIVE_CLIENTS:
            ACTIVE_CLIENTS.remove(websocket)


@app.on_event("shutdown")
async def shutdown():
    global SIMULATOR_RUNNING
    SIMULATOR_RUNNING = False