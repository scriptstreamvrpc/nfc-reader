import asyncio
import sqlite3
from datetime import datetime
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Form, Body
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# --- smartcard imports ---
from smartcard.System import readers
from smartcard.Exceptions import NoCardException, CardConnectionException

# ==== Database setup ====
DB_FILE = "visitors.db"

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

# ==== Lifespan events ====
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    asyncio.create_task(reader_polling_loop(poll_interval=0.6))
    yield
    # Shutdown (if needed)
    pass

# ==== FastAPI setup ====
app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# global state
last_uid: str | None = None   # UID terakhir yang terbaca (yang akan dipoll oleh frontend)
_prev_uid: str | None = None  # internal debounce: uid sekarang pada reader loop

# ---- helper: dict factory for /visitors ----
def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

# ==== Routes ====
@app.get("/last_uid")
def get_last_uid():
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
    return templates.TemplateResponse("index.html", {"request": request, "logs": logs, "last_uid": last_uid})

@app.post("/register")
def register(name: str = Form(...)):
    global last_uid
    if not last_uid:
        return JSONResponse({"error": "No card tapped"}, status_code=400)

    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    try:
        cur.execute("INSERT INTO visitors (name, card_uid) VALUES (?, ?)", (name, last_uid))
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        return JSONResponse({"error": "Visitor with this card already exists"}, status_code=400)
    conn.close()

    # Reset last_uid so UI won't re-use it accidentally
    last_uid = None
    return JSONResponse({"message": "Visitor registered successfully"})

@app.post("/checkin")
def checkin():
    global last_uid
    if not last_uid:
        return JSONResponse({"error": "No card tapped"}, status_code=400)

    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM visitors WHERE card_uid=?", (last_uid,))
    row = cur.fetchone()
    if row:
        visitor_id, name = row
        now = datetime.now().isoformat()
        cur.execute("INSERT INTO visits (visitor_id, checkin_time) VALUES (?, ?)", (visitor_id, now))
        conn.commit()
        conn.close()
        last_uid = None
        return JSONResponse({"message": f"{name} checked in at {now}"})
    else:
        conn.close()
        return JSONResponse({"error": "Card not registered"}, status_code=404)

@app.post("/checkout")
def checkout_by_uid(data: dict = Body(...)):
    uid = data.get("uid")
    if not uid:
        return JSONResponse({"error": "UID required"}, status_code=400)

    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM visitors WHERE card_uid=?", (uid,))
    visitor = cur.fetchone()
    if not visitor:
        conn.close()
        return JSONResponse({"error": f"Card {uid} not registered"}, status_code=404)

    visitor_id, name = visitor
    now = datetime.now().isoformat()
    cur.execute("""
        UPDATE visits
        SET checkout_time=?
        WHERE visitor_id=? AND checkout_time IS NULL
    """, (now, visitor_id))
    conn.commit()
    conn.close()

    return JSONResponse({"message": f"{name} checked out at {now}"})

@app.get("/reset_visits")
def reset_visits():
    try:
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        cur.execute("DELETE FROM visits")  # hapus semua data visit
        conn.commit()
        conn.close()
        return JSONResponse({"message": "All visits have been reset"})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/visitors")
def get_visitors():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = dict_factory
    cur = conn.cursor()
    cur.execute("""
        SELECT v.id, v.name, v.card_uid, vs.checkin_time, vs.checkout_time
        FROM visitors v
        LEFT JOIN visits vs ON v.id = vs.visitor_id
        ORDER BY v.id DESC
    """)
    rows = cur.fetchall()
    conn.close()
    return rows

# ==== Smartcard Reader Loop (pyscard) ====
# Uses APDU to get UID: often FF CA 00 00 00 works with ACR122U. Adjust if needed.
GET_UID_APDU = [0xFF, 0xCA, 0x00, 0x00, 0x00]

async def reader_polling_loop(poll_interval: float = 0.5):
    """
    Poll the first available PC/SC reader, attempt to read UID.
    Debounce: update global last_uid only when card newly presented (uid != _prev_uid).
    When card removed, reset _prev_uid to None.
    """
    global last_uid, _prev_uid
    while True:
        try:
            r = readers()
            if not r:
                # no readers found — set prev uid to None and wait
                if _prev_uid is not None:
                    _prev_uid = None
                    # do NOT clear last_uid here; last_uid is used by UI until consumed
                await asyncio.sleep(poll_interval)
                continue

            # use first reader (or extend to choose specific reader)
            reader = r[0]
            try:
                connection = reader.createConnection()
                # try to connect; if no card present, this raises exception
                connection.connect()
                # send APDU to get UID
                data, sw1, sw2 = connection.transmit(GET_UID_APDU)
                if sw1 == 0x90 and sw2 == 0x00:
                    uid = ''.join(format(x, '02X') for x in data)
                    # if card newly presented (or different uid), update state
                    if uid != _prev_uid:
                        _prev_uid = uid
                        last_uid = uid
                        print(f"[READER] Card presented UID: {uid} at {datetime.now().isoformat()}")
                else:
                    # APDU failed — treat as no card
                    if _prev_uid is not None:
                        _prev_uid = None
                        print("[READER] APDU returned non-success")
                # disconnect cleanly
                try:
                    connection.disconnect()
                except Exception:
                    pass
            except NoCardException:
                # no card present in reader
                if _prev_uid is not None:
                    print("[READER] Card removed")
                _prev_uid = None
            except CardConnectionException:
                # connection problems — treat as no card
                _prev_uid = None
            except Exception as e:
                # catch-all for other reader errors but don't crash loop
                print("[READER] Exception:", e)
                _prev_uid = None

        except Exception as e:
            # top-level safeguard
            print("[READER] Poll loop error:", e)

        await asyncio.sleep(poll_interval)


