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
    version="2.0",
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


# ───────────────── STARTUP ─────────────────

@app.on_event("startup")
def startup():
    download_db_once()

    conn = get_db()
    cur = conn.cursor()
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


# 2️⃣ STRICT MOVIE SEARCH (NO SPAM RESULTS)
@app.get("/search")
def search(
    q: str = Query(..., min_length=1),
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=20)
):
    tokens = [t.lower().strip() for t in q.split() if len(t) > 1]
    if not tokens:
        raise HTTPException(status_code=400, detail="Invalid query")

    offset = (page - 1) * limit

    conn = get_db()
    cur = conn.cursor()

    # Build strict token conditions (ALL tokens must match)
    conditions = []
    params = []

    for t in tokens:
        conditions.append("""
        (
            lower(title) = ?
            OR lower(title) LIKE ?
            OR lower(title) LIKE ?
            OR lower(title) LIKE ?
            OR lower(file_name) = ?
            OR lower(file_name) LIKE ?
            OR lower(file_name) LIKE ?
            OR lower(file_name) LIKE ?
        )
        """)
        params.extend([
            t,
            f"{t} %",
            f"% {t} %",
            f"% {t}",
            t,
            f"{t} %",
            f"% {t} %",
            f"% {t}",
        ])

    where_clause = " AND ".join(conditions)

    # Count matches
    cur.execute(
        f"""
        SELECT COUNT(*)
        FROM movies
        WHERE {where_clause}
        """,
        params
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

    # Fetch results (rank exact title matches higher)
    cur.execute(
        f"""
        SELECT uid, title, duration, size, type
        FROM movies
        WHERE {where_clause}
        ORDER BY
            CASE
                WHEN lower(title) = ? THEN 0
                WHEN lower(title) LIKE ? THEN 1
                ELSE 2
            END,
            post_id DESC
        LIMIT ? OFFSET ?
        """,
        params + [tokens[0], f"{tokens[0]} %", limit, offset]
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


# 3️⃣ UID → FILE RESOLVER (FAST & SAFE)
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