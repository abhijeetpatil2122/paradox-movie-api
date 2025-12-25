import json
import math
from flask import Flask, request, jsonify

app = Flask(__name__)

# ───────────────── LOAD DATABASE ─────────────────
with open("movie.json", "r", encoding="utf-8") as f:
    MOVIES = json.load(f)

TOTAL_MOVIES = len(MOVIES)

# UID → item lookup (fast O(1))
UID_INDEX = {item["uid"]: item for item in MOVIES}

# ───────────────── HELPERS ─────────────────
def normalize(text):
    return (text or "").lower()

# ───────────────── ROUTES ─────────────────

# 1️⃣ Health / Status
@app.route("/", methods=["GET"])
def health():
    return jsonify({
        "success": True,
        "api": "Paradox Movie API",
        "status": "online",
        "total_movies": TOTAL_MOVIES
    })


# 2️⃣ Search Endpoint (Paginated)
@app.route("/search", methods=["GET"])
def search():
    query = request.args.get("q", "").strip()
    page = int(request.args.get("page", 1))
    limit = int(request.args.get("limit", 10))

    # Safety limits
    if limit < 1:
        limit = 10
    if limit > 20:
        limit = 20

    if not query:
        return jsonify({
            "success": False,
            "message": "Query parameter 'q' is required"
        }), 400

    q = normalize(query)
    matched = []

    # Full DB scan (safe for ~20k items)
    for item in MOVIES:
        title = normalize(item.get("title"))
        fname = normalize(item.get("file_name"))

        if q in title or q in fname:
            matched.append({
                "uid": item["uid"],
                "title": item.get("title"),
                "size": item.get("size"),
                "duration": item.get("duration"),
                "type": item.get("type")
            })

    total_results = len(matched)
    total_pages = max(1, math.ceil(total_results / limit))

    # Clamp page
    if page < 1:
        page = 1
    if page > total_pages:
        page = total_pages

    start = (page - 1) * limit
    end = start + limit
    page_results = matched[start:end]

    return jsonify({
        "success": True,
        "query": query,
        "page": page,
        "limit": limit,
        "total_results": total_results,
        "total_pages": total_pages,
        "results": page_results
    })


# 3️⃣ UID → File Resolver
@app.route("/file", methods=["GET"])
def file_lookup():
    uid = request.args.get("uid", "").strip()

    if not uid:
        return jsonify({
            "success": False,
            "message": "UID is required"
        }), 400

    item = UID_INDEX.get(uid)

    if not item:
        return jsonify({
            "success": False,
            "message": "Invalid UID"
        }), 404

    return jsonify({
        "success": True,
        "uid": uid,
        "post_id": item["post_id"],
        "channel_id": item["channel_id"],
        "title": item.get("title")
    })