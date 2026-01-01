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
    version="4.0",
    docs_url=None,
    redoc_url=None
)

# ───────────────── DB HELPERS ─────────────────

def download_db_once():
    if os.path.exists(LOCAL_DB_PATH):
        print("📦 DB already exists, skipping download")
        return

    print("⬇️ Downloading SQLite DB from Blob...")
    r = requests.get(BLOB_DB_URL, timeout=120)
    r.raise_for_status()

    with open(LOCAL_DB_PATH, "wb") as f:
        f.write(r.content)

    print("✅ DB downloaded")


def get_db():
    return sqlite3.connect(LOCAL_DB_PATH, check_same_thread=False)


def sanitize_query(q: str) -> str:
    """
    Prepare query for FTS5:
    - lowercase
    - remove dangerous chars
    - tokenize for intent-based search
    """
    q = q.lower().strip()
    q = q.replace("'", " ")
    q = q.replace('"', " ")
    tokens = [t for t in q.split() if len(t) > 1]
    return " ".join(tokens)


# ───────────────── STARTUP ─────────────────

@app.on_event("startup")
def startup():
    download_db_once()

    conn = get_db()
    cur = conn.cursor()

    # sanity check
    cur.execute("SELECT COUNT(*) FROM movies")
    total = cur.fetchone()[0]

    conn.close()
    print(f"🚀 SQLite DB ready: {total} records")


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


# 2️⃣ SEARCH — FTS5 (FAST + RANKED + CORRECT)
@app.get("/search")
def search(
    q: str = Query(..., min_length=1),
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=20)
):
    query = sanitize_query(q)

    if not query:
        raise HTTPException(status_code=400, detail="Invalid search query")

    offset = (page - 1) * limit

    conn = get_db()
    cur = conn.cursor()

    # Count matches (FTS5)
    cur.execute(
        """
        SELECT COUNT(*)
        FROM movies_fts
        WHERE movies_fts MATCH ?
        """,
        (query,)
    )
    total_results = cur.fetchone()[0]

    if total_results == 0:
        conn.close()
        return {
            "success": True,
            "query": q,
            "page": 1,
            "limit": limit,
            "total_results": 0,
            "total_pages": 0,
            "results": []
        }

    total_pages = math.ceil(total_results / limit)

    if page > total_pages:
        page = total_pages
        offset = (page - 1) * limit

    # Fetch ranked results
    cur.execute(
        """
        SELECT m.uid,
               m.title,
               m.duration,
               m.size,
               m.type
        FROM movies_fts f
        JOIN movies m ON m.rowid = f.rowid
        WHERE f MATCH ?
        ORDER BY bm25(f) ASC
        LIMIT ? OFFSET ?
        """,
        (query, limit, offset)
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


# 3️⃣ UID → FILE RESOLVER (ULTRA FAST)
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