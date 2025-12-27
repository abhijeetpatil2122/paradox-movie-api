import sqlite3
import math
from fastapi import FastAPI, Query, HTTPException

DB_PATH = "movies.db"

app = FastAPI(
    title="Paradox Movie API",
    version="3.0",
    docs_url=None,
    redoc_url=None
)

# ───────────────── DB HELPERS ─────────────────

def get_db():
    # check_same_thread=False is IMPORTANT for FastAPI
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def row_to_dict(row):
    return {
        "uid": row[0],
        "post_id": row[1],
        "channel_id": row[2],
        "title": row[3],
        "file_name": row[4],
        "duration": row[5],
        "size": row[6],
        "mime": row[7],
        "type": row[8]
    }


# ───────────────── STARTUP ─────────────────

@app.on_event("startup")
def startup():
    # Simple DB sanity check
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM movies")
        total = cur.fetchone()[0]
        conn.close()
        print(f"✅ SQLite DB loaded: {total} records")
    except Exception as e:
        print("❌ Database error:", e)
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

    # Count total matches
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

    # Fetch page results
    cur.execute(
        """
        SELECT uid, post_id, channel_id, title, file_name,
               duration, size, mime, type
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

    results = [
        {
            "uid": r[0],
            "title": r[3],
            "size": r[6],
            "duration": r[5],
            "type": r[8]
        }
        for r in rows
    ]

    return {
        "success": True,
        "query": q,
        "page": page,
        "limit": limit,
        "total_results": total_results,
        "total_pages": total_pages,
        "results": results
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