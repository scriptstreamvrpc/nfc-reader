import asyncio
import sqlite3
import random
from datetime import datetime
from typing import List
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Body, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# smartcard imports (works if pyscard installed and reader present)
from smartcard.System import readers
from smartcard.Exceptions import NoCardException, CardConnectionException

DB_FILE = "visitors.db"

# ---------- DB init ----------
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS visitors (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        card_uid TEXT UNIQUE NOT NULL
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS visits (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        visitor_id INTEGER NOT NULL,
        checkin_time TEXT NOT NULL,
        checkout_time TEXT,
        FOREIGN KEY(visitor_id) REFERENCES visitors(id)
    )
    """)
    conn.commit()
    conn.close()

init_db()

# ---------- Lifespan events ----------
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    asyncio.create_task(reader_polling_loop(poll_interval=0.6))
    yield
    # Shutdown (if needed)
    pass

# ---------- App setup ----------
app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# ---------- global state ----------
last_uid: str | None = None   # UID terakhir yang tersedia untuk UI
_prev_uid: str | None = None  # internal debounce

# ---------- WebSocket manager ----------
class ConnectionManager:
    def __init__(self):
        self.active: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active:
            self.active.remove(websocket)

    async def broadcast(self, message: dict):
        living: List[WebSocket] = []
        for ws in self.active:
            try:
                await ws.send_json(message)
                living.append(ws)
            except Exception:
                # drop bad connection
                pass
        self.active = living

manager = ConnectionManager()

# ---------- Helpers ----------
def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def get_visitor_by_uid(uid: str):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM visitors WHERE card_uid=?", (uid,))
    row = cur.fetchone()
    conn.close()
    return row  # (id, name) or None

def insert_visit(visitor_id: int):
    now = datetime.now().isoformat()
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("INSERT INTO visits (visitor_id, checkin_time) VALUES (?, ?)", (visitor_id, now))
    conn.commit()
    visit_id = cur.lastrowid
    conn.close()
    return visit_id, now

def checkout_last_visit(visitor_id: int):
    now = datetime.now().isoformat()
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        UPDATE visits
        SET checkout_time=?
        WHERE visitor_id=? AND checkout_time IS NULL
    """, (now, visitor_id))
    conn.commit()
    conn.close()
    return now

# ---------- Routes / API ----------
@app.get("/last_uid")
def api_last_uid():
    return JSONResponse({"uid": last_uid})

@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        SELECT visits.id, visitors.name, visitors.card_uid, visits.checkin_time, visits.checkout_time
        FROM visits
        JOIN visitors ON visitors.id = visits.visitor_id
        ORDER BY visits.checkin_time DESC
    """)
    logs = cur.fetchall()
    conn.close()
    return templates.TemplateResponse("dashboard-realtime.html", {"request": request, "logs": logs, "last_uid": last_uid})

# Register expects JSON or form-data; we'll support JSON via fetch() from frontend
@app.post("/register")
async def api_register(request: Request):
    global last_uid
    # parse JSON or form
    content_type = request.headers.get("content-type", "")
    if "application/json" in content_type:
        data = await request.json()
        name = data.get("name")
    else:
        form = await request.form()
        name = form.get("name")

    if not last_uid:
        return JSONResponse({"error": "No card tapped"}, status_code=400)
    if not name:
        return JSONResponse({"error": "Name required"}, status_code=400)

    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    try:
        cur.execute("INSERT INTO visitors (name, card_uid) VALUES (?, ?)", (name, last_uid))
        conn.commit()
        visitor_id = cur.lastrowid
    except sqlite3.IntegrityError:
        conn.close()
        return JSONResponse({"error": "Visitor with this card already exists"}, status_code=400)
    conn.close()

    # broadcast register event with visitor info
    visitor_info = {"id": visitor_id, "name": name, "card_uid": last_uid}
    await manager.broadcast({"type": "register", "visitor": visitor_info})

    # reset last_uid to avoid accidental reuse
    last_uid = None
    return JSONResponse({"message": "Visitor registered successfully", "visitor": visitor_info})

@app.post("/checkin")
async def api_checkin():
    global last_uid
    if not last_uid:
        return JSONResponse({"error": "No card tapped"}, status_code=400)
    row = get_visitor_by_uid(last_uid)
    if not row:
        return JSONResponse({"error": "Card not registered"}, status_code=404)
    visitor_id, name = row
    visit_id, now = insert_visit(visitor_id)

    # broadcast checkin event
    visit_info = {"id": visit_id, "visitor_id": visitor_id, "name": name, "card_uid": last_uid, "checkin_time": now}
    await manager.broadcast({"type": "checkin", "visit": visit_info})

    last_uid = None
    return JSONResponse({"message": f"{name} checked in at {now}", "visit": visit_info})

@app.post("/checkout")
async def api_checkout(data: dict = Body(...)):
    uid = data.get("uid")
    if not uid:
        return JSONResponse({"error": "UID required"}, status_code=400)
    row = get_visitor_by_uid(uid)
    if not row:
        return JSONResponse({"error": "Card not registered"}, status_code=404)
    visitor_id, name = row
    now = checkout_last_visit(visitor_id)

    # broadcast checkout event
    await manager.broadcast({"type": "checkout", "visitor_id": visitor_id, "card_uid": uid, "checkout_time": now, "name": name})
    return JSONResponse({"message": f"{name} checked out at {now}"})

@app.get("/visitors")
def api_visitors():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = dict_factory
    cur = conn.cursor()
    cur.execute("""
        SELECT v.id, v.name, v.card_uid,
               (SELECT checkin_time FROM visits vs WHERE vs.visitor_id = v.id ORDER BY vs.id DESC LIMIT 1) as checkin_time,
               (SELECT checkout_time FROM visits vs WHERE vs.visitor_id = v.id ORDER BY vs.id DESC LIMIT 1) as checkout_time
        FROM visitors v
        ORDER BY v.id DESC
    """)
    rows = cur.fetchall()
    conn.close()
    return rows

@app.post("/reset_visits")
async def api_reset_visits():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("DELETE FROM visits")
    conn.commit()
    conn.close()
    await manager.broadcast({"type": "reset_visits"})
    return JSONResponse({"message": "All visits have been reset"})

# ---------- WebSocket endpoint ----------
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            # keep connection alive, optionally read incoming messages
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(ws)
    except Exception:
        manager.disconnect(ws)

# ---------- Smartcard reader loop (pyscard) ----------
GET_UID_APDU = [0xFF, 0xCA, 0x00, 0x00, 0x00]  # common for ACR122U

async def reader_polling_loop(poll_interval: float = 0.6):
    global last_uid, _prev_uid
    while True:
        try:
            r = readers()
            if not r:
                # no readers -> wait
                await asyncio.sleep(poll_interval)
                continue
            reader = r[0]
            try:
                connection = reader.createConnection()
                connection.connect()
                data, sw1, sw2 = connection.transmit(GET_UID_APDU)
                if sw1 == 0x90 and sw2 == 0x00:
                    uid = ''.join(format(x, '02X') for x in data)
                    if uid != _prev_uid:
                        _prev_uid = uid
                        last_uid = uid
                        print(f"[READER] Card presented UID: {uid} at {datetime.now().isoformat()}")
                        # broadcast card_present for UI auto-fill
                        await manager.broadcast({"type": "card_present", "uid": uid})
                else:
                    # APDU unsuccessful - treat as no card
                    if _prev_uid is not None:
                        _prev_uid = None
                        print("[READER] APDU returned non-success")
                try:
                    connection.disconnect()
                except Exception:
                    pass
            except NoCardException:
                # no card
                if _prev_uid is not None:
                    print("[READER] Card removed")
                _prev_uid = None
            except CardConnectionException:
                _prev_uid = None
            except Exception as e:
                print("[READER] Exception:", e)
                _prev_uid = None
        except Exception as e:
            print("[READER] Poll loop error:", e)
        await asyncio.sleep(poll_interval)

