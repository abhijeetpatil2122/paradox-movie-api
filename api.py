import sqlite3
import math
import os
import requests
from fastapi import FastAPI, Query, HTTPException

# ───────────────── CONFIG ─────────────────

BLOB_DB_URL = os.getenv("MOVIES_DB_URL")
LOCAL_DB_PATH = "/tmp/movies.db"

if not BLOB_DB_URL:
    raise RuntimeError("MOVIES_DB_URL environment variable is not set")

app = FastAPI(
    title="Paradox Movie API",
    version="3.1",
    docs_url=None,
    redoc_url=None
)

# ───────────────── DB HELPERS ─────────────────

def get_db():
    return sqlite3.connect(LOCAL_DB_PATH, check_same_thread=False)


def download_db_once():
    if os.path.exists(LOCAL_DB_PATH):
        print("📦 SQLite DB already exists, skipping download")
        return

    print("⬇️ Downloading SQLite DB from Blob...")
    r = requests.get(BLOB_DB_URL, timeout=60)
    r.raise_for_status()

    with open(LOCAL_DB_PATH, "wb") as f:
        f.write(r.content)

    print(f"✅ DB downloaded to {LOCAL_DB_PATH}")


# ───────────────── STARTUP ─────────────────

@app.on_event("startup")
def startup():
    try:
        download_db_once()

        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM movies")
        total = cur.fetchone()[0]
        conn.close()

        print(f"🚀 SQLite DB ready: {total} records")

    except Exception as e:
        print("❌ Startup failure:", e)
        raise e


# ───────────────── ROUTES ─────────────────

# 1️⃣ Health
@app.get("/")
def health():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM movies")
    total = cur.fetchone()[0]
    conn.close()

    return {
        "success": True,
        "api": "Paradox Movie API",
        "status": "online",
        "total_movies": total
    }


# 2️⃣ Search (Indexed, Paginated)
@app.get("/search")
def search(
    q: str = Query(..., min_length=1),
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=20)
):
    query = f"%{q.lower()}%"
    offset = (page - 1) * limit

    conn = get_db()
    cur = conn.cursor()

    # total matches
    cur.execute(
        """
        SELECT COUNT(*)
        FROM movies
        WHERE lower(title) LIKE ?
           OR lower(file_name) LIKE ?
        """,
        (query, query)
    )
    total_results = cur.fetchone()[0]

    total_pages = max(1, math.ceil(total_results / limit))

    if page > total_pages:
        page = total_pages
        offset = (page - 1) * limit

    # page results
    cur.execute(
        """
        SELECT uid, title, duration, size, type
        FROM movies
        WHERE lower(title) LIKE ?
           OR lower(file_name) LIKE ?
        ORDER BY post_id DESC
        LIMIT ? OFFSET ?
        """,
        (query, query, limit, offset)
    )

    rows = cur.fetchall()
    conn.close()

    return {
        "success": True,
        "query": q,
        "page": page,
        "limit": limit,
        "total_results": total_results,
        "total_pages": total_pages,
        "results": [
            {
                "uid": r[0],
                "title": r[1],
                "duration": r[2],
                "size": r[3],
                "type": r[4]
            }
            for r in rows
        ]
    }


# 3️⃣ UID → File Resolver (ULTRA FAST)
@app.get("/file")
def resolve(uid: str = Query(..., min_length=1)):
    conn = get_db()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT uid, post_id, channel_id, title
        FROM movies
        WHERE uid = ?
        LIMIT 1
        """,
        (uid,)
    )

    row = cur.fetchone()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Invalid UID")

    return {
        "success": True,
        "uid": row[0],
        "post_id": row[1],
        "channel_id": row[2],
        "title": row[3]
    }