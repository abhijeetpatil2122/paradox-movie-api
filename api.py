import json
from flask import Flask, request, jsonify

app = Flask(__name__)

# ───────── LOAD DATA ─────────
with open("movie.json", "r", encoding="utf-8") as f:
    MOVIES = json.load(f)

TOTAL_MOVIES = len(MOVIES)

# Build UID index for fast lookup
UID_INDEX = {item["uid"]: item for item in MOVIES}

# ───────── HELPERS ─────────
def normalize(text):
    return text.lower()

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
    limit = int(request.args.get("limit", 10))

    if not query:
        return jsonify({
            "success": False,
            "message": "Query parameter 'q' is required"
        }), 400

    q = normalize(query)
    results = []

    for item in MOVIES:
        title = normalize(item.get("title", ""))
        fname = normalize(item.get("file_name", ""))

        if q in title or q in fname:
            results.append({
                "uid": item["uid"],
                "title": item.get("title"),
                "size": item.get("size"),
                "duration": item.get("duration"),
                "type": item.get("type")
            })

        if len(results) >= limit:
            break

    return jsonify({
        "success": True,
        "count": len(results),
        "results": results
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