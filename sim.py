import os
import time
import random
import json
from datetime import datetime, timezone

import requests

ES_URL = os.environ.get("ES_URL")          # e.g. https://elastic.example.com
ES_API_KEY = os.environ.get("ES_API_KEY")  # Base64 API key
INDEX_NAME = os.environ.get("ES_INDEX", "agv_telemetry")
TICK_INTERVAL_SEC = float(os.environ.get("SIM_TICK_SEC", "1.0"))
DURATION_SEC = int(os.environ.get("SIM_DURATION_SEC", "600"))

AGV_IDS = [f"AGV-{i:02d}" for i in range(1, 11)]
YARD_BLOCKS = ["YB10", "YB11", "YB12"]
LANES_BY_BLOCK = {
    "YB10": ["L41", "L42"],
    "YB11": ["L43", "L44"],
    "YB12": ["L45", "L46"],
}


class AGVState:
    def __init__(self, agv_id: str):
        self.agv_id = agv_id
        self.yard_block = random.choice(YARD_BLOCKS)
        self.lane_id = random.choice(LANES_BY_BLOCK[self.yard_block])
        self.position_m = random.uniform(0, 1000)  # arbitrary lane length
        self.speed_kph = random.uniform(8, 14)     # typical BAU speed
        self.soc_pct = random.uniform(60, 100)
        self.job_id = self._new_job_id()
        self.load_status = random.choice(["EMPTY", "LOADED"])

    def _new_job_id(self) -> str:
        return f"JOB-{random.randint(100000, 999999)}"

    def tick(self, dt_sec: float):
        # Randomly drift speed within a normal range
        self.speed_kph += random.uniform(-1.0, 1.0)
        self.speed_kph = max(4.0, min(self.speed_kph, 18.0))  # keep reasonable

        # Move along lane
        m_per_sec = (self.speed_kph * 1000.0) / 3600.0
        self.position_m += m_per_sec * dt_sec

        # If we reach end of lane, "finish job" and pick a new one
        if self.position_m >= 1000.0:
            self.position_m = 0.0
            self.job_id = self._new_job_id()
            # Flip load status sometimes at the end of a trip
            if random.random() < 0.5:
                self.load_status = "LOADED" if self.load_status == "EMPTY" else "EMPTY"
            # Occasionally move to a different block/lane for variety
            if random.random() < 0.3:
                self.yard_block = random.choice(YARD_BLOCKS)
                self.lane_id = random.choice(LANES_BY_BLOCK[self.yard_block])

        # Battery drains slowly
        self.soc_pct -= random.uniform(0.01, 0.05) * dt_sec
        if self.soc_pct < 30:
            # Simulate occasional "charging" behaviour: jump back up
            if random.random() < 0.01:
                self.soc_pct = random.uniform(70, 100)

    def to_doc(self):
        return {
            "@timestamp": datetime.now(timezone.utc).isoformat(),
            "agv_id": self.agv_id,
            "yard_block": self.yard_block,
            "lane_id": self.lane_id,
            "position_m": round(self.position_m, 2),
            "speed_kph": round(self.speed_kph, 2),
            "soc_pct": round(self.soc_pct, 1),
            "job_id": self.job_id,
            "load_status": self.load_status,
            "mode": "BAU",
        }


def bulk_index(index: str, docs):
    if not docs:
        return
    lines = []
    for doc in docs:
        lines.append(json.dumps({"index": {}}))
        lines.append(json.dumps(doc))
    body = "\n".join(lines) + "\n"
    headers = {
        "Content-Type": "application/x-ndjson",
        "Authorization": f"ApiKey {ES_API_KEY}",
    }
    resp = requests.post(f"{ES_URL}/{index}/_bulk", data=body, headers=headers, timeout=10)
    resp.raise_for_status()


def main():
    if not ES_URL or not ES_API_KEY:
        raise SystemExit("ES_URL and ES_API_KEY must be set as environment variables")

    agvs = [AGVState(agv_id) for agv_id in AGV_IDS]
    start = time.time()
    tick = 0

    while time.time() - start < DURATION_SEC:
        docs = []
        for agv in agvs:
            agv.tick(TICK_INTERVAL_SEC)
            docs.append(agv.to_doc())

        bulk_index(INDEX_NAME, docs)
        tick += 1
        time.sleep(TICK_INTERVAL_SEC)

    print(f"Simulation finished after {tick} ticks")


if __name__ == "__main__":
    main()
