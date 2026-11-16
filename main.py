import asyncio
import math
import random
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

DB_PATH = "ssens.db"


# БД
def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT, sim_day REAL, temperature REAL, humidity REAL
        );
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT, sim_day REAL, temperature REAL, humidity REAL,
            level TEXT, message TEXT, irrigation INTEGER
        );
    """)
    conn.commit()
    conn.close()


class SeasonSimulator:
    def __init__(self, start_month=3, seconds_per_day=1):
        self.seconds_per_day = seconds_per_day
        self.day_of_year = (start_month - 1) * 30.4
        self.cumulative_seconds = 0.0
        self.triggered = set()            
        self.last_irrigation_day = -1     
        self.irrigation_active_days = 0.0 
        self.temperature = 0.0
        self.humidity = 60.0

    def _base_temp(self, day):
        # Среднегодовая 16 градусов, амплитуда 15, диапазон 1-31
        return 16.0 + 15.0 * math.sin(2 * math.pi * day / 365.0 - math.pi / 2)

    def step(self, dt):
        self.cumulative_seconds += dt
        days_adv = dt / self.seconds_per_day
        self.day_of_year += days_adv
        if self.day_of_year >= 365:
            self.day_of_year -= 365
            self.triggered.clear()

        # суточное колебание +-2 градуса + небольшой шум
        hour = (self.cumulative_seconds % self.seconds_per_day) / self.seconds_per_day
        daily = 2.0 * math.sin(2 * math.pi * hour - math.pi / 2)
        noise = random.uniform(-0.5, 0.5)

        cooling = 0.0
        if self.irrigation_active_days > 0:
            cooling = -6.0
            self.irrigation_active_days -= days_adv

        self.temperature = self._base_temp(self.day_of_year) + daily + noise + cooling

        # Базовые 60%. Чем жарче почва (выше 15 градусов), тем быстрее она сохнет.
        self.humidity = 60.0 - (self.temperature - 15.0) * 1.5 + noise
        
        # Если включен автополив, влажность резко подскакивает
        if self.irrigation_active_days > 0:
            self.humidity = max(self.humidity, 85.0) 
            
        # Ограничиваем влажность
        self.humidity = max(10.0, min(100.0, self.humidity))


        return self.temperature

    def activate_irrigation(self, days=0.5):
        self.irrigation_active_days = max(self.irrigation_active_days, days)

    def reset_season(self):
        self.day_of_year = 60.0  
        self.triggered.clear()
        self.irrigation_active_days = 0.0
        self.last_irrigation_day = -1



ALERT_RULES = [
    ("seed",         8,  "info",     " Посевная: почва прогрелась до +8 °C",        True,  False),
    ("pest_prep",    10, "info",     " Пора обработать почву от вредителей (+10 °C)", True,  False),
    ("pest_active",  15, "warning",  " Вредители могут начинать активность (+15 °C)", True,  False),
    ("pest_high",    20, "warning",  " Высокая активность вредителей (+20 °C)",      True,  False),
    ("drought",      28, "critical", " Потенциальная засуха — включить автополив (+28 °C)", True, False),
]


def evaluate_alerts(sim, temp):
    fired = []
    day_int = int(sim.day_of_year)
    for name, thresh, level, msg, once, wants_irr in ALERT_RULES:
        if temp >= thresh:
            if once:
                if name in sim.triggered:
                    continue
                sim.triggered.add(name)
                fired.append((name, level, msg, wants_irr))
        else:
            if once and name in sim.triggered and temp < thresh - 5:
                sim.triggered.discard(name)


    if sim.humidity < 20.0:
        if "dry_soil" not in sim.triggered:
            sim.triggered.add("dry_soil")
            fired.append(("dry_soil", "critical", " Критически низкая влажность почвы (<20%). Требуется полив!", False))
    
    # Если влажность восстановилась выше 30% 
    elif sim.humidity > 30.0:
        if "dry_soil" in sim.triggered:
            sim.triggered.discard("dry_soil")
    return fired



class ConnectionManager:
    def __init__(self):
        self.active = []

    async def connect(self, ws):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws):
        if ws in self.active:
            self.active.remove(ws)

    async def broadcast(self, data):
        for ws in list(self.active):
            try:
                await ws.send_json(data)
            except Exception:
                self.disconnect(ws)


manager = ConnectionManager()
sim: SeasonSimulator | None = None


async def simulation_loop():
    dt = 0.5
    while True:
        temp = sim.step(dt)
        ts = datetime.now().isoformat(timespec="seconds")
        sim_day = round(sim.day_of_year, 1)
        hum = round(sim.humidity, 1) 

        conn = get_db()
        
        conn.execute(
            "INSERT INTO readings (timestamp, sim_day, temperature, humidity) VALUES (?, ?, ?, ?)",
            (ts, sim_day, round(temp, 2), hum),
        )
        alerts = evaluate_alerts(sim, temp)
        for _, level, msg, irr in alerts:
            conn.execute(
                "INSERT INTO alerts (timestamp, sim_day, temperature, humidity, level, message, irrigation) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (ts, sim_day, round(temp, 2), hum, level, msg, 1 if irr else 0),
            )
        conn.commit()
        conn.close()

        await manager.broadcast({
            "type": "reading",
            "timestamp": ts,
            "sim_day": sim_day,
            "temperature": round(temp, 2),
            "humidity": hum, 
            "is_watering": sim.irrigation_active_days > 0,
            "alerts": [{"level": l, "message": m, "irrigation": i} for _, l, m, i in alerts],
        })
        await asyncio.sleep(dt)


@asynccontextmanager
async def lifespan(app):
    global sim
    init_db()
    # С какого месяца стартуем в симуляции и сколько реальных секунд = 1 симулированный день
    sim = SeasonSimulator(start_month=3, seconds_per_day=1)
    task = asyncio.create_task(simulation_loop())
    try:
        yield
    finally:
        task.cancel()


app = FastAPI(lifespan=lifespan)


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(ws)


@app.post("/harvest")
async def harvest():
    sim.reset_season()
    return {"status": "ok", "message": "Урожай собран. Новый сезон (1 марта)."}


@app.get("/history")
async def history(limit: int = 200):
    conn = get_db()
    readings = conn.execute(
        "SELECT * FROM readings ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()
    alerts = conn.execute(
        "SELECT * FROM alerts ORDER BY id DESC LIMIT 30"
    ).fetchall()
    conn.close()
    return {
        "readings": [dict(r) for r in reversed(readings)],
        "alerts":   [dict(a) for a in alerts],
    }

@app.post("/irrigation/on")
async def irrigation_on():
    # Включаем охлаждение почвы на 5 симулированных дней
    sim.activate_irrigation(5)
    ts = datetime.now().isoformat(timespec="seconds")
    msg = " Оператор включил автополив. Почва остывает."
    
    # Записываем в БД как подтвержденное действие
    conn = get_db()
    conn.execute(
        "INSERT INTO alerts (timestamp, sim_day, temperature, level, message, irrigation) "
        "VALUES (?, ?, ?, 'info', ?, 1)",
        (ts, round(sim.day_of_year, 1), round(sim.temperature, 2), msg),
    )
    conn.commit()
    conn.close()

    # Отправляем сообщение на фронтенд
    await manager.broadcast({
        "type": "reading", "timestamp": ts,
        "sim_day": round(sim.day_of_year, 1), "temperature": round(sim.temperature, 2),
        "alerts": [{"level": "info", "message": msg, "irrigation": False}],
    })
    return {"status": "ok"}

@app.get("/", response_class=HTMLResponse)
async def index():
    with open("static/index.html", "r", encoding="utf-8") as f:
        return f.read()