import json
from flask import Flask, request, jsonify
import math

app = Flask(__name__)

# ───────── LOAD DATA ─────────
with open("movie.json", "r", encoding="utf-8") as f:
    MOVIES = json.load(f)

TOTAL_MOVIES = len(MOVIES)

# Build UID index (O(1) lookup)
UID_INDEX = {item["uid"]: item for item in MOVIES}

# ───────── HELPERS ─────────
def normalize(text):
    return (text or "").lower()

# ───────── ROUTES ─────────

@app.route("/", methods=["GET"])
def health():
    return jsonify({
        "success": True,
        "api": "Paradox Movie API",
        "status": "online",
        "total_movies": TOTAL_MOVIES
    })

@app.route("/search", methods=["GET"])
def search():
    query = request.args.get("q", "").strip()
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 10))

    if not query:
        return jsonify({
            "success": False,
            "message": "Query parameter 'q' is required"
        }), 400

    q = normalize(query)
    matched = []

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
    total_pages = max(1, math.ceil(total_results / per_page))

    if page < 1:
        page = 1
    if page > total_pages:
        page = total_pages

    start = (page - 1) * per_page
    end = start + per_page
    page_results = matched[start:end]

    return jsonify({
        "success": True,
        "query": query,
        "page": page,
        "per_page": per_page,
        "total_results": total_results,
        "total_pages": total_pages,
        "results": page_results
    })

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
