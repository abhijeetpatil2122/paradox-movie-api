import sqlite3
import math
import os
import re
import requests
from fastapi import FastAPI, Query, HTTPException

# ───────────────── CONFIG ─────────────────

BLOB_DB_URL = os.getenv("MOVIES_DB_URL")
LOCAL_DB_PATH = "/tmp/movies.db"

if not BLOB_DB_URL:
    raise RuntimeError("MOVIES_DB_URL environment variable is not set")

app = FastAPI(
    title="Paradox Movie API",
    version="3.2",
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
    r = requests.get(BLOB_DB_URL, timeout=120)
    r.raise_for_status()

    with open(LOCAL_DB_PATH, "wb") as f:
        f.write(r.content)

    print("✅ DB downloaded successfully")


# ───────────────── SEARCH HELPERS ─────────────────

def normalize(text: str) -> str:
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def extract_year(text: str):
    m = re.search(r"\b(19|20)\d{2}\b", text)
    return m.group(0) if m else None


def score_item(query_words, query_year, text):
    score = 0

    # Name match (MOST IMPORTANT)
    for w in query_words:
        if w.isdigit():
            continue
        if w in text:
            score += 50

    # Optional year boost
    if query_year:
        if query_year in text:
            score += 25
        else:
            score -= 10

    # Quality boosts
    if "1080p" in text:
        score += 10
    elif "720p" in text:
        score += 5
    elif "480p" in text:
        score += 2

    # Language boosts
    if "hindi" in text:
        score += 5
    if "english" in text:
        score += 3

    return score


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


# 2️⃣ SMART SEARCH (Ranked + Paginated)
@app.get("/search")
def search(
    q: str = Query(..., min_length=1),
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=20)
):
    q_norm = normalize(q)
    query_words = q_norm.split()
    query_year = extract_year(q_norm)

    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT uid, title, file_name, duration, size, type
        FROM movies
        WHERE title IS NOT NULL OR file_name IS NOT NULL
    """)

    scored = []

    for uid, title, fname, duration, size, mtype in cur.fetchall():
        combined = normalize((title or "") + " " + (fname or ""))
        score = score_item(query_words, query_year, combined)

        if score > 0:
            scored.append({
                "uid": uid,
                "title": title or fname,
                "duration": duration,
                "size": size,
                "type": mtype,
                "_score": score
            })

    conn.close()

    scored.sort(key=lambda x: x["_score"], reverse=True)

    total_results = len(scored)
    total_pages = max(1, math.ceil(total_results / limit))

    if page > total_pages:
        page = total_pages

    start = (page - 1) * limit
    end = start + limit
    page_results = scored[start:end]

    # Remove internal score
    for r in page_results:
        r.pop("_score", None)

    return {
        "success": True,
        "query": q,
        "page": page,
        "limit": limit,
        "total_results": total_results,
        "total_pages": total_pages,
        "results": page_results
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