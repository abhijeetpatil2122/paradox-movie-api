import json
import math
from fastapi import FastAPI, Query, HTTPException

app = FastAPI(
    title="Paradox Movie API",
    version="2.0",
    docs_url=None,
    redoc_url=None
)

MOVIES = []
UID_INDEX = {}
TOTAL_MOVIES = 0


# ───────────────── STARTUP ─────────────────
@app.on_event("startup")
def load_database():
    global MOVIES, UID_INDEX, TOTAL_MOVIES

    with open("movie.json", "r", encoding="utf-8") as f:
        MOVIES = json.load(f)

    UID_INDEX = {item["uid"]: item for item in MOVIES}
    TOTAL_MOVIES = len(MOVIES)

    print(f"✅ Loaded {TOTAL_MOVIES} movies into memory")


# ───────────────── HELPERS ─────────────────
def normalize(text: str) -> str:
    return (text or "").lower()


# ───────────────── ROUTES ─────────────────

# 1️⃣ Health
@app.get("/")
def health():
    return {
        "success": True,
        "api": "Paradox Movie API",
        "status": "online",
        "total_movies": TOTAL_MOVIES
    }


# 2️⃣ Search (Paginated)
@app.get("/search")
def search(
    q: str = Query(..., min_length=1),
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=20)
):
    query = normalize(q)
    matched = []

    for item in MOVIES:
        title = normalize(item.get("title"))
        fname = normalize(item.get("file_name"))

        if query in title or query in fname:
            matched.append({
                "uid": item["uid"],
                "title": item.get("title"),
                "size": item.get("size"),
                "duration": item.get("duration"),
                "type": item.get("type")
            })

    total_results = len(matched)
    total_pages = max(1, math.ceil(total_results / limit))

    if page > total_pages:
        page = total_pages

    start = (page - 1) * limit
    end = start + limit

    return {
        "success": True,
        "query": q,
        "page": page,
        "limit": limit,
        "total_results": total_results,
        "total_pages": total_pages,
        "results": matched[start:end]
    }


# 3️⃣ UID → File Resolver (ULTRA FAST)
@app.get("/file")
def resolve(uid: str = Query(..., min_length=1)):
    item = UID_INDEX.get(uid)

    if not item:
        raise HTTPException(status_code=404, detail="Invalid UID")

    return {
        "success": True,
        "uid": uid,
        "post_id": item["post_id"],
        "channel_id": item["channel_id"],
        "title": item.get("title")
    }