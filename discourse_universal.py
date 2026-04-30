"""
Universal Discourse Knowledge Base Query Engine
IIT Madras BS Degree Discourse Forum — discourse.onlinedegree.iitm.ac.in

USAGE
─────
1. Run once to build the local cache (takes ~30–60 min):
       python discourse_universal.py --fetch

2. Answer a batch of questions from a JSON file:
       python discourse_universal.py --answer questions.json

3. Interactive single question:
       python discourse_universal.py --query

4. Run the built-in validation suite against known answers:
       python discourse_universal.py --validate

QUESTION FORMAT
───────────────
Each question is a dict with these keys:

  {
    "id":       "q1",                          # any unique label
    "category": "System Commands",             # exact name from CATEGORY_SLUGS
    "type":     "ACCEPTED_POST_ID",            # see QUESTION_TYPES below
    "params": {                                # keys depend on type
      ...
    }
  }

QUESTION TYPES & REQUIRED PARAMS
──────────────────────────────────
  ACCEPTED_POST_ID        title, date (YYYY-MM-DD)
  REPLY_COUNT_COMPOUND    title, date (YYYY-MM-DD)
  TOTAL_POSTS             start, end  (ISO datetime strings)
  AGGREGATE_LIKES         start, end
  TOP_LIKED_USER          start, end
  TOP_ANSWER_AUTHOR       start, end
  TOP_REPLIER             start, end
  UNIQUE_CREATORS         start, end
  UNIQUE_CREATORS_COMPOUND start, end → "<count>-<latest_topic_id>"
  TAG_COUNT               tag, start, end
  TAG_COUNT_COMPOUND      tag, start, end   → "<count>-<latest_topic_id>"

All answers are frozen as of 2026-04-25.
"""

import requests
import json
import time
import os
import sys
from datetime import datetime, timezone
from collections import defaultdict
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

BASE_URL   = "https://discourse.onlinedegree.iitm.ac.in"
CACHE_DIR  = "discourse_cache"
DATA_DIR   = "discourse_data"        # pre-built per-category JSON dumps
DELAY      = 1.1
FROZEN_DATE = "2026-04-25T23:59:59Z"

LOG_FILE   = "query_log.txt"
_log_fh    = open(LOG_FILE, "w", buffering=1, encoding="utf-8")

CATEGORY_SLUGS = {
    "System Commands":                             "sc-kb",
    "Programming in Python":                       "python-kb",
    "Machine Learning Practices":                  "mlp-kb",
    "Statistics for Data Science II":              "stats2-kb",
    "Machine Learning Techniques":                 "mlt-kb",
    "Database Management Systems":                 "dbms-kb",
    "Tools in Data Science":                       "tds-kb",
    "Modern Application Development I":            "mad1-kb",
    "Mathematics for Data Science II":             "maths2-kb",
    "Programming Concepts using Java":             "java-kb",
    "Machine Learning Foundations":                "mlf-kb",
    "Programming, Data Structures and Algorithms": "pdsa-kb",
    "Modern Application Development II":           "mad2-kb",
    "English II":                                  "english2-kb",
}

QUESTION_TYPES = {
    "ACCEPTED_POST_ID",
    "REPLY_COUNT_COMPOUND",
    "TOTAL_POSTS",
    "AGGREGATE_LIKES",
    "TOP_LIKED_USER",
    "TOP_ANSWER_AUTHOR",
    "TOP_REPLIER",
    "UNIQUE_CREATORS",
    "TAG_COUNT",
    "TAG_COUNT_COMPOUND",
    "UNIQUE_CREATORS_COMPOUND",
}

# ─────────────────────────────────────────────────────────────
# LOGGING / SESSION
# ─────────────────────────────────────────────────────────────

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(DATA_DIR,  exist_ok=True)

session = requests.Session()
session.headers.update({"Accept": "application/json"})

# ── Paste your session cookies here ──────────────────────────
session.cookies.set(
    "_t",
    "kmymxLL0DVz6ZpnLHTEaRSGgyQWLKbkNwaSz1Z%2By9HwM6565BdF1mbovg3NsuwT6rG1r87I2Sn81ekDsskLypLEmhZ9D3MYv%2FzPGyLtGNaGxqJdwad4JEAosl6SpLFwu0lo3rMOPgs6Rh9TwtOEZAtTrzhsxdgfMImEOEVJgDntrILVGXocx23aI7biwx8v8ML7IMD88EqfdxNVORspRIg9Ak4IC2BvS2BhWces4Cf%2FUpxL3vYNZIcBOLSB%2FMlzjaKKgAtxOUvGfDsp%2F%2FZ2xlpmcGWV74VCc--Ki5oeRTXZX35bEvz--YrRrYgb2s4V34WJmW55DhA%3D%3D",
    domain="discourse.onlinedegree.iitm.ac.in",
)
session.cookies.set(
    "_forum_session",
    "zWNRdfRpui2X9kRavZl0GYYc5isfp735HoSgtPSz75wVe6whlJ%2FPlcgbzY5BsnweD7NvBFBskEkidWSL9%2B%2BncvW2A7cG7v1Ys74kduBHEqe%2Fgn30I5HbZc5GOtdiShvIiYvfZOLkmpUMJ0qnOa%2FRsaoKdy9jwkBNv5xCR%2F6kmr9wTEXl6BtaOM4mEfxcJ3Pif660XmmmnzE%2BS5akgJ0u0FBStjdSGfHB9%2FtsNJe%2B5G0Q4IYIqHthl3sc8CZQciqifBD3zYGHL8ekpXmplsoEQpw3lqxet4EXSZIEBOD3iToFAZZCwwx0As4%2Bb%2B5zcrho2Whma8olXhvyYjMCx164oi%2BNylD7hJn3vhT5B3TJ4%2BYlNt59scjrZOUwBlcH%2F9GbAekl6koQGtXOCiwh9qeEMyCggWBTB8oDcZXWAnoiJAaAr6JGXdddTvYbA%2Bpvtet0QAhXcP%2BN3fK250vIxSmXbvQhGysRuA%3D%3D--avstJCVahSs%2FHyKS--2FeEaNtbfnXux9pyteh8zQ%3D%3D",
    domain="discourse.onlinedegree.iitm.ac.in",
)
# ─────────────────────────────────────────────────────────────


def log(msg=""):
    print(msg)
    _log_fh.write(str(msg) + "\n")
    _log_fh.flush()


# ─────────────────────────────────────────────────────────────
# DATE UTILITIES
# ─────────────────────────────────────────────────────────────

def parse_dt(s):
    if not s:
        return None
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def cap_end(end_str):
    """Clamp end date to FROZEN_DATE so post-snapshot data is excluded."""
    frozen = parse_dt(FROZEN_DATE)
    end    = parse_dt(end_str)
    return FROZEN_DATE if end > frozen else end_str


def in_range(date_str, start, end):
    d = parse_dt(date_str)
    if d is None:
        return False
    s = parse_dt(start)
    e = parse_dt(end)
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return s <= d <= e


def norm_end(end_str):
    """Convert a bare date like '2025-12-31' to '2025-12-31T23:59:59Z'."""
    if "T" not in end_str:
        end_str = end_str + "T23:59:59Z"
    if not end_str.endswith("Z") and "+" not in end_str:
        end_str += "Z"
    return end_str


def norm_start(start_str):
    if "T" not in start_str:
        start_str = start_str + "T00:00:00Z"
    if not start_str.endswith("Z") and "+" not in start_str:
        start_str += "Z"
    return start_str


# ─────────────────────────────────────────────────────────────
# HTTP / CACHING
# ─────────────────────────────────────────────────────────────

def _safe_key(url, params):
    raw = url + str(sorted((params or {}).items()))
    return (
        raw.replace("/", "_")
           .replace(":", "_")
           .replace("?", "_")
           .replace("&", "_")
           .replace("=", "_")
           .replace(" ", "_")[:200]
    )


def get(url, params=None, max_retries=5):
    cache_file = os.path.join(CACHE_DIR, _safe_key(url, params) + ".json")
    if os.path.exists(cache_file):
        with open(cache_file) as f:
            return json.load(f)

    for attempt in range(max_retries):
        try:
            time.sleep(DELAY)
            resp = session.get(url, params=params, timeout=30)
            if resp.status_code == 429:
                wait = 60 * (attempt + 1)
                log(f"  Rate limited — waiting {wait}s...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            data = resp.json()
            with open(cache_file, "w") as f:
                json.dump(data, f)
            return data
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ChunkedEncodingError,
        ) as e:
            wait = 10 * (2 ** attempt)
            log(f"  Connection error (attempt {attempt+1}/{max_retries}): {e}")
            log(f"  Retrying in {wait}s...")
            time.sleep(wait)

    raise RuntimeError(f"Failed after {max_retries} attempts: {url}")


# ─────────────────────────────────────────────────────────────
# DATA FETCHING
# ─────────────────────────────────────────────────────────────

def fetch_solved_topics(slug):
    topics, page = [], 0
    log(f"  Fetching topics [{slug}]...")
    while True:
        data       = get(f"{BASE_URL}/filter.json",
                         {"q": f"category:{slug} status:solved", "page": page})
        batch      = data.get("topic_list", {}).get("topics", [])
        if not batch:
            break
        topics.extend(batch)
        log(f"    page {page}: +{len(batch)} (total {len(topics)})")
        page += 1
    return topics


def fetch_topic_posts(topic_id):
    """
    Paginate all posts for a topic.
    KEY FIX: `stream` is only populated on page 1; use its length as the
    authoritative total rather than checking `stream` on subsequent pages.
    """
    url  = f"{BASE_URL}/t/{topic_id}.json"
    data = get(url, {"page": 1})

    all_posts = list(data.get("post_stream", {}).get("posts", []))
    stream    = data.get("post_stream", {}).get("stream", [])
    total     = len(stream)
    seen      = {p["id"] for p in all_posts}
    page      = 2

    while len(all_posts) < total:
        data  = get(url, {"page": page})
        posts = data.get("post_stream", {}).get("posts", [])
        if not posts:
            break
        new = [p for p in posts if p["id"] not in seen]
        if not new:
            break
        all_posts.extend(new)
        seen.update(p["id"] for p in new)
        page += 1

    if len(all_posts) < total:
        log(f"    ⚠ topic {topic_id}: expected {total}, got {len(all_posts)}")
    return all_posts


def fetch_category(name):
    """
    Fetch ALL solved topics + their posts for `name`.
    Saves to DATA_DIR/<name>.json so subsequent runs skip fetching entirely.
    """
    data_file = os.path.join(DATA_DIR, name.replace(" ", "_") + ".json")
    if os.path.exists(data_file):
        log(f"  Loading cached data for [{name}]")
        with open(data_file) as f:
            saved = json.load(f)
        return saved["topics"], {int(k): v for k, v in saved["posts"].items()}

    slug   = CATEGORY_SLUGS[name]
    topics = fetch_solved_topics(slug)
    posts_by_topic = {}
    for i, t in enumerate(topics):
        tid = t["id"]
        log(f"  [{name}] topic {i+1}/{len(topics)}: {tid} {t.get('title','')[:55]}")
        posts_by_topic[tid] = fetch_topic_posts(tid)

    with open(data_file, "w") as f:
        json.dump({"topics": topics, "posts": posts_by_topic}, f)
    log(f"  Saved {name} → {data_file}")
    return topics, posts_by_topic


# ─────────────────────────────────────────────────────────────
# IN-MEMORY CATEGORY STORE
# ─────────────────────────────────────────────────────────────

_store = {}   # name → (topics, posts_by_topic)


def get_category(name):
    if name not in _store:
        if name not in CATEGORY_SLUGS:
            raise ValueError(f"Unknown category: '{name}'. "
                             f"Valid names: {sorted(CATEGORY_SLUGS)}")
        log(f"\n=== {name} ===")
        _store[name] = fetch_category(name)
    return _store[name]


def fetch_all_categories():
    """Pre-fetch every category. Call this with --fetch."""
    for name in CATEGORY_SLUGS:
        get_category(name)
    log("\nAll categories fetched and saved.")


# ─────────────────────────────────────────────────────────────
# LOW-LEVEL AGGREGATORS
# ─────────────────────────────────────────────────────────────

def get_likes(post):
    for a in post.get("actions_summary", []):
        if a.get("id") == 2:
            return a.get("count", 0)
    return 0


def find_topic(topics, title, date_prefix):
    title_l = title.lower()
    # exact title + date
    for t in topics:
        if title_l in t.get("title", "").lower() and t.get("created_at", "").startswith(date_prefix):
            return t
    # title only
    for t in topics:
        if title_l in t.get("title", "").lower():
            return t
    return None


# ─────────────────────────────────────────────────────────────
# QUERY ENGINE — one method per question type
# ─────────────────────────────────────────────────────────────

class QueryEngine:
    """
    answer(q) dispatches on q["type"] and returns the answer as a string.

    q = {
      "id":       "...",        # any label
      "category": "...",        # must match CATEGORY_SLUGS key
      "type":     "...",        # one of QUESTION_TYPES
      "params":   { ... }       # see each method below
    }
    """

    def answer(self, q):
        qtype = q["type"].upper().replace(" ", "_")
        if qtype not in QUESTION_TYPES:
            return f"ERROR: unknown type '{qtype}'"
        topics, posts = get_category(q["category"])
        p = q.get("params", {})
        try:
            return getattr(self, "_" + qtype.lower())(topics, posts, p)
        except Exception as e:
            return f"ERROR: {e}"

    # ── per-topic lookups ──────────────────────────────────────

    def _accepted_post_id(self, topics, posts, p):
        t = find_topic(topics, p["title"], p["date"])
        if not t:
            return f"TOPIC NOT FOUND: {p['title']}"
        for post in posts.get(t["id"], []):
            if post.get("accepted_answer"):
                return str(post["id"])
        return "NO ACCEPTED ANSWER"

    def _reply_count_compound(self, topics, posts, p):
        cutoff = cap_end("2026-12-31T23:59:59Z")   # always frozen
        t = find_topic(topics, p["title"], p["date"])
        if not t:
            return f"TOPIC NOT FOUND: {p['title']}"
        replies = [
            post for post in posts.get(t["id"], [])
            if post.get("post_number", 1) > 1
            and in_range(post.get("created_at", ""), "2000-01-01T00:00:00Z", cutoff)
        ]
        if not replies:
            return "0-None"
        latest = max(replies, key=lambda p: p.get("created_at", ""))
        return f"{len(replies)}-{latest['id']}"

    # ── aggregate across all solved topics ────────────────────

    def _total_posts(self, topics, posts, p):
        start = norm_start(p["start"])
        end   = cap_end(norm_end(p["end"]))
        count = sum(
            1
            for t in topics
            for post in posts.get(t["id"], [])
            if in_range(post.get("created_at", ""), start, end)
        )
        return str(count)

    def _aggregate_likes(self, topics, posts, p):
        start = norm_start(p["start"])
        end   = cap_end(norm_end(p["end"]))
        total = sum(
            get_likes(post)
            for t in topics
            for post in posts.get(t["id"], [])
            if in_range(post.get("created_at", ""), start, end)
        )
        return str(total)

    def _top_liked_user(self, topics, posts, p):
        start = norm_start(p["start"])
        end   = cap_end(norm_end(p["end"]))
        acc   = defaultdict(int)
        for t in topics:
            for post in posts.get(t["id"], []):
                if in_range(post.get("created_at", ""), start, end):
                    acc[post.get("username", "")] += get_likes(post)
        return max(acc, key=acc.get) if acc else "NONE"

    def _top_answer_author(self, topics, posts, p):
        """
        Username with the most accepted-answer posts where the *topic* was
        created in [start, end].  (The accepted post itself can be anywhere.)
        """
        start = norm_start(p["start"])
        end   = cap_end(norm_end(p["end"]))
        acc   = defaultdict(int)
        for t in topics:
            if not in_range(t.get("created_at", ""), start, end):
                continue
            for post in posts.get(t["id"], []):
                if post.get("accepted_answer"):
                    acc[post.get("username", "")] += 1
        return max(acc, key=acc.get) if acc else "NONE"

    def _top_replier(self, topics, posts, p):
        """Username who posted the most non-OP replies in [start, end]."""
        start = norm_start(p["start"])
        end   = cap_end(norm_end(p["end"]))
        acc   = defaultdict(int)
        for t in topics:
            for post in posts.get(t["id"], []):
                if post.get("post_number", 1) <= 1:
                    continue
                if in_range(post.get("created_at", ""), start, end):
                    acc[post.get("username", "")] += 1
        return max(acc, key=acc.get) if acc else "NONE"

    def _unique_creators(self, topics, posts, p):
        """Count unique OP usernames for topics created in [start, end]."""
        start = norm_start(p["start"])
        end   = cap_end(norm_end(p["end"]))
        creators = set()
        for t in topics:
            if not in_range(t.get("created_at", ""), start, end):
                continue
            for post in posts.get(t["id"], []):
                if post.get("post_number") == 1:
                    creators.add(post.get("username", "").lower())
                    break
        return str(len(creators))

    def _tag_count(self, topics, posts, p):
        start = norm_start(p["start"])
        end   = cap_end(norm_end(p["end"]))
        tag   = p["tag"].lower()
        count = sum(
            1
            for t in topics
            if in_range(t.get("created_at", ""), start, end)
            and tag in [
                (tg["name"] if isinstance(tg, dict) else tg).lower()
                for tg in t.get("tags", [])
            ]
        )
        return str(count)

    def _tag_count_compound(self, topics, posts, p):
        """Returns '<count>-<latest_topic_id>'."""
        start = norm_start(p["start"])
        end   = cap_end(norm_end(p["end"]))
        tag   = p["tag"].lower()
        matched = [
            t for t in topics
            if in_range(t.get("created_at", ""), start, end)
            and tag in [
                (tg["name"] if isinstance(tg, dict) else tg).lower()
                for tg in t.get("tags", [])
            ]
        ]
        if not matched:
            return "0-None"
        latest = max(matched, key=lambda t: t.get("created_at", ""))
        return f"{len(matched)}-{latest['id']}"
    
    def _unique_creators_compound(self, topics, posts, p):
        """
        Returns '<count>-<latest_topic_id>' for unique OPs in [start, end].
        latest_topic_id is the topic ID of the most recently created matching topic.
        """
        start = norm_start(p["start"])
        end   = cap_end(norm_end(p["end"]))
        
        matched_topics = []
        for t in topics:
            if not in_range(t.get("created_at", ""), start, end):
                continue
            # check has at least one post (to get OP username)
            for post in posts.get(t["id"], []):
                if post.get("post_number") == 1:
                    matched_topics.append(t)
                    break
        
        if not matched_topics:
            return "0-None"
        
        # Count unique creators
        creators = set()
        for t in matched_topics:
            for post in posts.get(t["id"], []):
                if post.get("post_number") == 1:
                    creators.add(post.get("username", "").lower())
                    break
        
        # Find latest topic by created_at
        latest = max(matched_topics, key=lambda t: t.get("created_at", ""))
        return f"{len(creators)}-{latest['id']}"


# ─────────────────────────────────────────────────────────────
# BATCH RUNNER
# ─────────────────────────────────────────────────────────────

def run_questions(questions):
    """
    Run a list of question dicts and return {id: answer}.
    Also prints a formatted table.
    """
    engine  = QueryEngine()
    results = {}
    log(f"\n{'ID':<30} {'CATEGORY':<40} {'TYPE':<25} ANSWER")
    log("-" * 130)
    for q in questions:
        qid    = q.get("id", "?")
        answer = engine.answer(q)
        results[qid] = answer
        log(f"{qid:<30} {q['category']:<40} {q['type']:<25} {answer}")
    return results


# ─────────────────────────────────────────────────────────────
# VALIDATION (known-answer spot-checks)
# ─────────────────────────────────────────────────────────────

VALIDATION_SUITE = [
    # System Commands
    {"id": "v01", "category": "System Commands", "type": "ACCEPTED_POST_ID",
     "params": {"title": "Link for the vm", "date": "2025-01-21"},
     "expected": "582635"},
    {"id": "v02", "category": "System Commands", "type": "REPLY_COUNT_COMPOUND",
     "params": {"title": "VM - Passwordless Access - Email Not Allowed Error", "date": "2025-09-25"},
     "expected": "22-680140"},
    {"id": "v03", "category": "System Commands", "type": "TOP_ANSWER_AUTHOR",
     "params": {"start": "2025-01-01", "end": "2025-12-31"},
     "expected": "sayan"},
    {"id": "v04", "category": "System Commands", "type": "AGGREGATE_LIKES",
     "params": {"start": "2025-01-01", "end": "2025-03-31"},
     "expected": "182"},
    {"id": "v05", "category": "System Commands", "type": "TAG_COUNT",
     "params": {"tag": "term2-2025", "start": "2025-07-01", "end": "2025-12-31"},
     "expected": "215"},
    # Programming in Python
    {"id": "v06", "category": "Programming in Python", "type": "AGGREGATE_LIKES",
     "params": {"start": "2025-10-01", "end": "2025-12-31"},
     "expected": "194"},
    {"id": "v07", "category": "Programming in Python", "type": "TOP_LIKED_USER",
     "params": {"start": "2026-01-01", "end": "2026-04-30"},
     "expected": "hx_xa"},
    {"id": "v08", "category": "Programming in Python", "type": "UNIQUE_CREATORS",
     "params": {"start": "2025-01-01", "end": "2025-06-30"},
     "expected": "143"},
    # MLT (pagination bug stress-test)
    {"id": "v09", "category": "Machine Learning Techniques", "type": "TOTAL_POSTS",
     "params": {"start": "2025-01-01", "end": "2025-12-31"},
     "expected": "1082"},
    # MDS2 (pagination bug stress-test)
    {"id": "v10", "category": "Mathematics for Data Science II", "type": "TOTAL_POSTS",
     "params": {"start": "2025-01-01", "end": "2025-06-30"},
     "expected": "412"},
    # MLP
    {"id": "v11", "category": "Machine Learning Practices", "type": "TAG_COUNT",
     "params": {"tag": "clarification", "start": "2025-04-01", "end": "2025-06-30"},
     "expected": "19"},
    # DBMS
    {"id": "v12", "category": "Database Management Systems", "type": "TOP_ANSWER_AUTHOR",
     "params": {"start": "2025-01-01", "end": "2025-03-31"},
     "expected": "22f3000605"},
    # Stats2
    {"id": "v13", "category": "Statistics for Data Science II", "type": "TAG_COUNT_COMPOUND",
     "params": {"tag": "clarification", "start": "2025-04-01", "end": "2025-12-31"},
     "expected": "88"},          # compound answer starts with "88-"
    # English II
    {"id": "v14", "category": "English II", "type": "ACCEPTED_POST_ID",
     "params": {"title": "Adjuncts Question Doubt", "date": "2026-03-09"},
     "expected": "736265"},
]


def run_validation():
    engine = QueryEngine()
    passed = failed = 0
    log("\n" + "=" * 80)
    log("VALIDATION")
    log("=" * 80)
    for v in VALIDATION_SUITE:
        got      = engine.answer(v)
        exp      = v["expected"]
        # For TAG_COUNT_COMPOUND expected may be just the count prefix
        ok = got == exp or got.startswith(exp + "-") or got.lower() == exp.lower()
        status   = "PASS" if ok else "FAIL"
        if ok:
            passed += 1
        else:
            failed += 1
        log(f"  [{status}] {v['id']:5s} {v['category'][:35]:<35} "
            f"got={got!r:25} exp={exp!r}")
    log(f"\n{passed}/{passed+failed} passed")
    return failed == 0


# ─────────────────────────────────────────────────────────────
# UNANSWERED QUESTIONS (Document 7)
# ─────────────────────────────────────────────────────────────

UNANSWERED_QUESTIONS = [
    # ── System Commands ──────────────────────────────────────────────────────
    {"id": "sc_accepted_env_setup",        "category": "System Commands", "type": "ACCEPTED_POST_ID",
     "params": {"title": "Clarification Regarding Environment Setup for System Commands Course", "date": "2026-02-26"}},
    {"id": "sc_tag_term2_2025_H2",         "category": "System Commands", "type": "TAG_COUNT",
     "params": {"tag": "term2-2025", "start": "2025-07-01", "end": "2025-12-31"}},
    {"id": "sc_agg_likes_jan_sep25",       "category": "System Commands", "type": "AGGREGATE_LIKES",
     "params": {"start": "2025-01-01", "end": "2025-09-30"}},
    {"id": "sc_reply_oppe_vm_lagging",     "category": "System Commands", "type": "REPLY_COUNT_COMPOUND",
     "params": {"title": "Oppe VM is Lagging", "date": "2025-08-14"}},
    {"id": "sc_top_liked_q1_2025",         "category": "System Commands", "type": "TOP_LIKED_USER",
     "params": {"start": "2025-01-01", "end": "2025-03-31"}},
    {"id": "sc_reply_assignment1_absent",  "category": "System Commands", "type": "REPLY_COUNT_COMPOUND",
     "params": {"title": "Assignment_1 marked Absent despite successful submission", "date": "2025-10-15"}},
    {"id": "sc_accepted_weightage_oppe",   "category": "System Commands", "type": "ACCEPTED_POST_ID",
     "params": {"title": "Weightage of each question in OPPE", "date": "2025-12-08"}},
    {"id": "sc_total_posts_2026_q1q2",     "category": "System Commands", "type": "TOTAL_POSTS",
     "params": {"start": "2026-01-01", "end": "2026-04-30"}},
    {"id": "sc_total_posts_h2_2025",       "category": "System Commands", "type": "TOTAL_POSTS",
     "params": {"start": "2025-07-01", "end": "2025-12-31"}},
    {"id": "sc_reply_quiz1_pyq_doubts",    "category": "System Commands", "type": "REPLY_COUNT_COMPOUND",
     "params": {"title": "Quiz 1 PYQ Doubts", "date": "2025-07-03"}},
    {"id": "sc_accepted_bpt_grades",       "category": "System Commands", "type": "ACCEPTED_POST_ID",
     "params": {"title": "BPT Grades", "date": "2025-06-29"}},
    {"id": "sc_unique_creators_apr_dec25", "category": "System Commands", "type": "UNIQUE_CREATORS",
     "params": {"start": "2025-04-01", "end": "2025-12-31"}},
    {"id": "sc_reply_bpt_clarification_26","category": "System Commands", "type": "REPLY_COUNT_COMPOUND",
     "params": {"title": "BPT clarification term-1 2026", "date": "2026-02-18"}},
    {"id": "sc_top_liked_h2_2025",         "category": "System Commands", "type": "TOP_LIKED_USER",
     "params": {"start": "2025-07-01", "end": "2025-12-31"}},
    {"id": "sc_top_replier_h1_2025",       "category": "System Commands", "type": "TOP_REPLIER",
     "params": {"start": "2025-01-01", "end": "2025-06-30"}},
    {"id": "sc_reply_combined_sheet",      "category": "System Commands", "type": "REPLY_COUNT_COMPOUND",
     "params": {"title": "Combined Sheet with recording link of all TA sessions", "date": "2026-02-19"}},
    {"id": "sc_accepted_regarding_ta",     "category": "System Commands", "type": "ACCEPTED_POST_ID",
     "params": {"title": "Regarding ta", "date": "2025-01-31"}},
    {"id": "sc_unique_creators_h2_2025",   "category": "System Commands", "type": "UNIQUE_CREATORS",
     "params": {"start": "2025-07-01", "end": "2025-12-31"}},
    {"id": "sc_accepted_vmt_questions",    "category": "System Commands", "type": "ACCEPTED_POST_ID",
     "params": {"title": "Regarding VMT questions", "date": "2025-11-12"}},
    {"id": "sc_reply_oppe_rescheduling",   "category": "System Commands", "type": "REPLY_COUNT_COMPOUND",
     "params": {"title": "OPPE Rescheduling Requests", "date": "2025-12-05"}},
    {"id": "sc_accepted_no_such_file",     "category": "System Commands", "type": "ACCEPTED_POST_ID",
     "params": {"title": "No such file or directory (os error 2) in BPT1_problem_1", "date": "2026-02-27"}},
    {"id": "sc_reply_ppa_white_bg",        "category": "System Commands", "type": "REPLY_COUNT_COMPOUND",
     "params": {"title": "PPA/GRPA not visible on VM due to white background color", "date": "2026-02-17"}},
    {"id": "sc_top_liked_apr_dec25",       "category": "System Commands", "type": "TOP_LIKED_USER",
     "params": {"start": "2025-04-01", "end": "2025-12-31"}},
    {"id": "sc_top_answer_apr_dec25",      "category": "System Commands", "type": "TOP_ANSWER_AUTHOR",
     "params": {"start": "2025-04-01", "end": "2025-12-31"}},
    {"id": "sc_reply_fn_et_error",         "category": "System Commands", "type": "REPLY_COUNT_COMPOUND",
     "params": {"title": "FN ET Question error", "date": "2025-12-22"}},
    {"id": "sc_accepted_vm_access_doubt",  "category": "System Commands", "type": "ACCEPTED_POST_ID",
     "params": {"title": "VM Access Doubt", "date": "2026-02-04"}},
    {"id": "sc_agg_likes_2026_q1q2",       "category": "System Commands", "type": "AGGREGATE_LIKES",
     "params": {"start": "2026-01-01", "end": "2026-04-30"}},
    {"id": "sc_accepted_not_able_acces_vm","category": "System Commands", "type": "ACCEPTED_POST_ID",
     "params": {"title": "Not able to acces VM", "date": "2025-07-18"}},

    # ── Programming in Python ─────────────────────────────────────────────────
    {"id": "py_total_posts_2025",          "category": "Programming in Python", "type": "TOTAL_POSTS",
     "params": {"start": "2025-01-01", "end": "2025-12-31"}},
    {"id": "py_tag_clarification_jan_sep25","category": "Programming in Python", "type": "TAG_COUNT",
     "params": {"tag": "clarification", "start": "2025-01-01", "end": "2025-09-30"}},
    {"id": "py_accepted_oppe_fail_diploma", "category": "Programming in Python", "type": "ACCEPTED_POST_ID",
     "params": {"title": "If I fail in both the OPPEs but passed in ESE and cleared all foundation level courses", "date": "2025-04-02"}},
    {"id": "py_accepted_oppe1_experience",  "category": "Programming in Python", "type": "ACCEPTED_POST_ID",
     "params": {"title": "OPPE 1 exam experience", "date": "2025-03-02"}},
    {"id": "py_top_answer_h2_2025",         "category": "Programming in Python", "type": "TOP_ANSWER_AUTHOR",
     "params": {"start": "2025-07-01", "end": "2025-12-31"}},
    {"id": "py_reply_end_term_eligibility", "category": "Programming in Python", "type": "REPLY_COUNT_COMPOUND",
     "params": {"title": "Doubt Regarding End Term Eligibility for Python course", "date": "2026-04-06"}},
    {"id": "py_reply_discrepancy_oppe1",    "category": "Programming in Python", "type": "REPLY_COUNT_COMPOUND",
     "params": {"title": "Discrepancy regarding OPPE 1 conducted in Jan 2025", "date": "2025-03-18"}},
    {"id": "py_tag_clarification_2025",     "category": "Programming in Python", "type": "TAG_COUNT",
     "params": {"tag": "clarification", "start": "2025-01-01", "end": "2025-12-31"}},
    {"id": "py_reply_1_course_per_term",    "category": "Programming in Python", "type": "REPLY_COUNT_COMPOUND",
     "params": {"title": "1 course/term", "date": "2026-01-04"}},
    {"id": "py_accepted_missing_lecture",   "category": "Programming in Python", "type": "ACCEPTED_POST_ID",
     "params": {"title": "Missing Lecture Videos - Course Structure Query", "date": "2026-03-07"}},
    {"id": "py_reply_oppe_repeat",          "category": "Programming in Python", "type": "REPLY_COUNT_COMPOUND",
     "params": {"title": "OPPE reapeat", "date": "2025-01-29"}},
    {"id": "py_reply_missing_lecture",      "category": "Programming in Python", "type": "REPLY_COUNT_COMPOUND",
     "params": {"title": "Missing Lecture Videos - Course Structure Query", "date": "2026-03-07"}},
    {"id": "py_accepted_public_cases_marks","category": "Programming in Python", "type": "ACCEPTED_POST_ID",
     "params": {"title": "How much marks will we get for passsing 3/4 public cases", "date": "2025-12-14"}},
    {"id": "py_accepted_query_on_day_oppe", "category": "Programming in Python", "type": "ACCEPTED_POST_ID",
     "params": {"title": "Query regarding on the day of OPPE exam", "date": "2026-04-02"}},
    {"id": "py_reply_w3_grpa2_conflict",    "category": "Programming in Python", "type": "REPLY_COUNT_COMPOUND",
     "params": {"title": "Python w-3, Grpa2: Conflicting Instructions", "date": "2026-02-28"}},
    {"id": "py_accepted_live_session_spreadsheet","category": "Programming in Python", "type": "ACCEPTED_POST_ID",
     "params": {"title": "Live session spreadsheet and how to view the google Collab python notes?", "date": "2026-02-20"}},
    {"id": "py_top_answer_apr_jun25",       "category": "Programming in Python", "type": "TOP_ANSWER_AUTHOR",
     "params": {"start": "2025-04-01", "end": "2025-06-30"}},
    {"id": "py_tag_clarification_q1_2025",  "category": "Programming in Python", "type": "TAG_COUNT",
     "params": {"tag": "clarification", "start": "2025-01-01", "end": "2025-03-31"}},
    {"id": "py_accepted_mlp_oppe_score",    "category": "Machine Learning Practices", "type": "ACCEPTED_POST_ID",
     "params": {"title": "Mlp oppe score", "date": "2025-11-17"}},
    {"id": "py_accepted_query_grade_w2",    "category": "Programming in Python", "type": "ACCEPTED_POST_ID",
     "params": {"title": "Query Regarding Grade for Graded Assignment Week 2 q.25", "date": "2025-10-11"}},
    {"id": "py_accepted_sct_date_discrepancy","category": "Programming in Python", "type": "ACCEPTED_POST_ID",
     "params": {"title": "SCT document vs Form date discrepancy", "date": "2026-01-27"}},
    {"id": "py_agg_likes_q1_2025",          "category": "Programming in Python", "type": "AGGREGATE_LIKES",
     "params": {"start": "2025-01-01", "end": "2025-03-31"}},
    {"id": "py_agg_likes_2025",             "category": "Programming in Python", "type": "AGGREGATE_LIKES",
     "params": {"start": "2025-01-01", "end": "2025-12-31"}},
    {"id": "py_accepted_jan2026_oppe1_paper","category": "Programming in Python", "type": "ACCEPTED_POST_ID",
     "params": {"title": "JANUARY 2026 Term Candidate Here", "date": "2026-04-22"}},
    {"id": "py_tag_clarification_2026_q1q2","category": "Programming in Python", "type": "TAG_COUNT",
     "params": {"tag": "clarification", "start": "2026-01-01", "end": "2026-04-30"}},
    {"id": "py_accepted_missed_oppe1",      "category": "Programming in Python", "type": "ACCEPTED_POST_ID",
     "params": {"title": "Missed OPPE-1", "date": "2025-03-07"}},
    {"id": "py_accepted_cant_understand_grpa5","category": "Programming in Python", "type": "ACCEPTED_POST_ID",
     "params": {"title": "Can't understand the GrPA5", "date": "2025-01-16"}},
    {"id": "py_top_answer_apr_dec25",       "category": "Programming in Python", "type": "TOP_ANSWER_AUTHOR",
     "params": {"start": "2025-04-01", "end": "2025-12-31"}},
    {"id": "py_reply_doubt_spacing",        "category": "Programming in Python", "type": "REPLY_COUNT_COMPOUND",
     "params": {"title": "Doubt Related to Spacing Issue in Week 2 Code", "date": "2025-10-08"}},
    {"id": "py_accepted_some_glitch_oppe1", "category": "Programming in Python", "type": "ACCEPTED_POST_ID",
     "params": {"title": "Some glitch in oppe1 marks", "date": "2025-09-02"}},

    # ── Machine Learning Practices ────────────────────────────────────────────
    {"id": "mlp_accepted_ga81_q1",         "category": "Machine Learning Practices", "type": "ACCEPTED_POST_ID",
     "params": {"title": "Ga 8.1 q1", "date": "2026-04-06"}},
    {"id": "mlp_top_replier_2025",         "category": "Machine Learning Practices", "type": "TOP_REPLIER",
     "params": {"start": "2025-01-01", "end": "2025-12-31"}},
    {"id": "mlp_total_posts_apr_dec25",    "category": "Machine Learning Practices", "type": "TOTAL_POSTS",
     "params": {"start": "2025-04-01", "end": "2025-12-31"}},
    {"id": "mlp_accepted_important_opp1",  "category": "Machine Learning Practices", "type": "ACCEPTED_POST_ID",
     "params": {"title": "Important resources for the OPP 1 preparation of MLP", "date": "2025-02-24"}},
    {"id": "mlp_accepted_week5_ga",        "category": "Machine Learning Practices", "type": "ACCEPTED_POST_ID",
     "params": {"title": "Regarding Week 5 GA", "date": "2026-03-20"}},
    {"id": "mlp_accepted_week2_ga_data",   "category": "Machine Learning Practices", "type": "ACCEPTED_POST_ID",
     "params": {"title": "Week 2 GA data", "date": "2025-05-21"}},
    {"id": "mlp_total_posts_jan_sep25",    "category": "Machine Learning Practices", "type": "TOTAL_POSTS",
     "params": {"start": "2025-01-01", "end": "2025-09-30"}},
    {"id": "mlp_reply_unable_access",      "category": "Machine Learning Practices", "type": "REPLY_COUNT_COMPOUND",
     "params": {"title": "Unable to access MLP Project in Discourse", "date": "2025-01-18"}},
    {"id": "mlp_top_liked_apr_dec25",      "category": "Machine Learning Practices", "type": "TOP_LIKED_USER",
     "params": {"start": "2025-04-01", "end": "2025-12-31"}},
    {"id": "mlp_accepted_ga9_incorrect",   "category": "Machine Learning Practices", "type": "ACCEPTED_POST_ID",
     "params": {"title": "Ga-9 incorrect answers for ques 4 and 5", "date": "2025-08-11"}},
    {"id": "mlp_accepted_ga_ka_repeat",    "category": "Machine Learning Practices", "type": "ACCEPTED_POST_ID",
     "params": {"title": "GA and KA for Repeat OPPE", "date": "2026-02-19"}},
    {"id": "mlp_tag_compound_diploma_q1",  "category": "Machine Learning Practices", "type": "TAG_COUNT_COMPOUND",
     "params": {"tag": "diploma-level", "start": "2025-01-01", "end": "2025-03-31"}},
    {"id": "mlp_reply_course_restructured","category": "Machine Learning Practices", "type": "REPLY_COUNT_COMPOUND",
     "params": {"title": "Course restructured", "date": "2025-06-20"}},

    # ── Statistics for Data Science II ───────────────────────────────────────
    {"id": "st2_top_liked_h2_2025",        "category": "Statistics for Data Science II", "type": "TOP_LIKED_USER",
     "params": {"start": "2025-07-01", "end": "2025-12-31"}},
    {"id": "st2_unique_creators_2025",     "category": "Statistics for Data Science II", "type": "UNIQUE_CREATORS",
     "params": {"start": "2025-01-01", "end": "2025-12-31"}},
    {"id": "st2_top_answer_2026_q1q2",     "category": "Statistics for Data Science II", "type": "TOP_ANSWER_AUTHOR",
     "params": {"start": "2026-01-01", "end": "2026-04-30"}},
    {"id": "st2_accepted_aq5_2_q5",        "category": "Statistics for Data Science II", "type": "ACCEPTED_POST_ID",
     "params": {"title": "Doubt in AQ5.2 | Q 5", "date": "2025-02-15"}},
    {"id": "st2_accepted_ga_w4_change",    "category": "Statistics for Data Science II", "type": "ACCEPTED_POST_ID",
     "params": {"title": "Change in Graded Assignment Questions (Week-4 Stats-2)", "date": "2025-07-03"}},
    {"id": "st2_accepted_peer_review_access","category": "Statistics for Data Science II", "type": "ACCEPTED_POST_ID",
     "params": {"title": "Extra Activity – Peer Review Access Issue", "date": "2025-06-28"}},
    {"id": "st2_top_replier_2025",         "category": "Statistics for Data Science II", "type": "TOP_REPLIER",
     "params": {"start": "2025-01-01", "end": "2025-12-31"}},
    {"id": "st2_accepted_explanation_slide","category": "Statistics for Data Science II", "type": "ACCEPTED_POST_ID",
     "params": {"title": "Explanation for slide", "date": "2025-12-08"}},
    {"id": "st2_accepted_doubt_aq91",      "category": "Statistics for Data Science II", "type": "ACCEPTED_POST_ID",
     "params": {"title": "Doubt in AQ 9.1", "date": "2026-04-14"}},
    {"id": "st2_accepted_bayesian_interp", "category": "Statistics for Data Science II", "type": "ACCEPTED_POST_ID",
     "params": {"title": "Interpretation for Bayesian Estimation", "date": "2025-08-11"}},

    # ── Machine Learning Techniques ───────────────────────────────────────────
    {"id": "mlt_reply_week9_pa_due_date",  "category": "Machine Learning Techniques", "type": "REPLY_COUNT_COMPOUND",
     "params": {"title": "Week 9 Programming Assignment due date passed before GA", "date": "2025-03-20"}},
    {"id": "mlt_accepted_pyq_doubt_326",   "category": "Machine Learning Techniques", "type": "ACCEPTED_POST_ID",
     "params": {"title": "PYQ Doubt Question Number : 326", "date": "2025-08-22"}},
    {"id": "mlt_reply_week2_pa2_q4",       "category": "Machine Learning Techniques", "type": "REPLY_COUNT_COMPOUND",
     "params": {"title": "Week 2 Practice Assignment 2 question 4", "date": "2025-05-20"}},
    {"id": "mlt_tag_clarification_q3_2025","category": "Machine Learning Techniques", "type": "TAG_COUNT",
     "params": {"tag": "clarification", "start": "2025-07-01", "end": "2025-09-30"}},
    {"id": "mlt_reply_why_ridge",          "category": "Machine Learning Techniques", "type": "REPLY_COUNT_COMPOUND",
     "params": {"title": "Why the term \"Ridge\" in Ridge Regression?", "date": "2025-11-11"}},
    {"id": "mlt_top_answer_q3_2025",       "category": "Machine Learning Techniques", "type": "TOP_ANSWER_AUTHOR",
     "params": {"start": "2025-07-01", "end": "2025-09-30"}},
    {"id": "mlt_top_answer_h1_2025",       "category": "Machine Learning Techniques", "type": "TOP_ANSWER_AUTHOR",
     "params": {"start": "2025-01-01", "end": "2025-06-30"}},
    {"id": "mlt_reply_did_not_understand",  "category": "Machine Learning Techniques", "type": "REPLY_COUNT_COMPOUND",
     "params": {"title": "Did not understand what is xi and xj means lec2.4", "date": "2025-07-02"}},
    {"id": "mlt_accepted_doubt_lec15",     "category": "Machine Learning Techniques", "type": "ACCEPTED_POST_ID",
     "params": {"title": "Doubt in Lecture 1.5", "date": "2025-05-11"}},

    # ── Database Management Systems ───────────────────────────────────────────
    {"id": "dbms_reply_doubt_pyq",         "category": "Database Management Systems", "type": "REPLY_COUNT_COMPOUND",
     "params": {"title": "Doubt in D.B.M.S PYQ", "date": "2025-06-30"}},
    {"id": "dbms_accepted_incorrect_avg",  "category": "Database Management Systems", "type": "ACCEPTED_POST_ID",
     "params": {"title": "Incorrect Average Assignment Score", "date": "2025-04-18"}},
    {"id": "dbms_reply_w7_grpa",           "category": "Database Management Systems", "type": "REPLY_COUNT_COMPOUND",
     "params": {"title": "Regarding W7 GRPAs score update on dashboard", "date": "2025-07-30"}},
    {"id": "dbms_tag_clarification_2026",  "category": "Database Management Systems", "type": "TAG_COUNT",
     "params": {"tag": "clarification", "start": "2026-01-01", "end": "2026-04-30"}},
    {"id": "dbms_reply_2024_iit_fn_q20",   "category": "Database Management Systems", "type": "REPLY_COUNT_COMPOUND",
     "params": {"title": "2024 Apr28: IIT M FN EXAM QDF1 Question 20", "date": "2025-04-06"}},
    {"id": "dbms_accepted_sql_q28",        "category": "Database Management Systems", "type": "ACCEPTED_POST_ID",
     "params": {"title": "SQL Practice Questions - Q:28", "date": "2025-03-28"}},
    {"id": "dbms_total_posts_2026_q1q2",   "category": "Database Management Systems", "type": "TOTAL_POSTS",
     "params": {"start": "2026-01-01", "end": "2026-04-30"}},

    # ── Tools in Data Science ─────────────────────────────────────────────────
    {"id": "tids_total_posts_q1_2025",     "category": "Tools in Data Science", "type": "TOTAL_POSTS",
     "params": {"start": "2025-01-01", "end": "2025-03-31"}},
    {"id": "tids_reply_tds_not_submitting","category": "Tools in Data Science", "type": "REPLY_COUNT_COMPOUND",
     "params": {"title": "Tds: assignment is not submitting", "date": "2025-02-03"}},
    {"id": "tids_accepted_ga3_tds",        "category": "Tools in Data Science", "type": "ACCEPTED_POST_ID",
     "params": {"title": "Ga3 _tds", "date": "2026-02-27"}},

    # ── Modern Application Development I ─────────────────────────────────────
    {"id": "mad1_agg_likes_q3_2025",       "category": "Modern Application Development I", "type": "AGGREGATE_LIKES",
     "params": {"start": "2025-07-01", "end": "2025-09-30"}},
    {"id": "mad1_reply_lab_test_run",      "category": "Modern Application Development I", "type": "REPLY_COUNT_COMPOUND",
     "params": {"title": "MAD I - Lab Assignment Test Run Failure", "date": "2025-05-19"}},
    {"id": "mad1_accepted_submission_query","category": "Modern Application Development I", "type": "ACCEPTED_POST_ID",
     "params": {"title": "Submission query on mad 1 project", "date": "2025-11-28"}},
    {"id": "mad1_tag_clarification_h2_2025","category": "Modern Application Development I", "type": "TAG_COUNT",
     "params": {"tag": "clarification", "start": "2025-07-01", "end": "2025-12-31"}},
    {"id": "mad1_accepted_class_not_reg",  "category": "Modern Application Development I", "type": "ACCEPTED_POST_ID",
     "params": {"title": "Class not registered", "date": "2025-05-12"}},
    {"id": "mad1_reply_need_help_project", "category": "Modern Application Development I", "type": "REPLY_COUNT_COMPOUND",
     "params": {"title": "Need help with MAD 1 Project", "date": "2026-03-02"}},
    {"id": "mad1_unique_creators_2025",    "category": "Modern Application Development I", "type": "UNIQUE_CREATORS",
     "params": {"start": "2025-01-01", "end": "2025-12-31"}},
    {"id": "mad1_reply_lab1_image_problem","category": "Modern Application Development I", "type": "REPLY_COUNT_COMPOUND",
     "params": {"title": "Lab 1 Image problem", "date": "2025-01-17"}},
    {"id": "mad1_top_liked_apr_dec25",     "category": "Modern Application Development I", "type": "TOP_LIKED_USER",
     "params": {"start": "2025-04-01", "end": "2025-12-31"}},
    {"id": "mad1_accepted_end_term_doubt", "category": "Modern Application Development I", "type": "ACCEPTED_POST_ID",
     "params": {"title": "End Term Doubt Question Id : 6406531252013", "date": "2025-04-15"}},
    {"id": "mad1_reply_quiz1_paper_may25", "category": "Modern Application Development I", "type": "REPLY_COUNT_COMPOUND",
     "params": {"title": "Where can I find the May Term Quiz 1 App dev question paper and ansker key?", "date": "2025-07-18"}},
    {"id": "mad1_tag_clarification_h1_2025","category": "Modern Application Development I", "type": "TAG_COUNT",
     "params": {"tag": "clarification", "start": "2025-01-01", "end": "2025-06-30"}},

    # ── Mathematics for Data Science II ──────────────────────────────────────
    {"id": "mds2_top_replier_apr_jun25",   "category": "Mathematics for Data Science II", "type": "TOP_REPLIER",
     "params": {"start": "2025-04-01", "end": "2025-06-30"}},
    {"id": "mds2_tag_clarification_2026",  "category": "Mathematics for Data Science II", "type": "TAG_COUNT",
     "params": {"tag": "clarification", "start": "2026-01-01", "end": "2026-04-30"}},
    {"id": "mds2_reply_doubt_pyq_quiz1",   "category": "Mathematics for Data Science II", "type": "REPLY_COUNT_COMPOUND",
     "params": {"title": "Doubt in PYQ Quiz-1", "date": "2025-10-23"}},
    {"id": "mds2_accepted_quiz1_b2b3b5",   "category": "Mathematics for Data Science II", "type": "ACCEPTED_POST_ID",
     "params": {"title": "Quiz 1: why is (b2,b3,b5) not a valid span", "date": "2025-02-26"}},
    {"id": "mds2_reply_regarding_mock2",   "category": "Mathematics for Data Science II", "type": "REPLY_COUNT_COMPOUND",
     "params": {"title": "Regarding Mock - 2", "date": "2026-04-09"}},
    {"id": "mds2_tag_clarification_q1_2025","category": "Mathematics for Data Science II", "type": "TAG_COUNT",
     "params": {"tag": "clarification", "start": "2025-01-01", "end": "2025-03-31"}},
    {"id": "mds2_reply_doubt_in_question", "category": "Mathematics for Data Science II", "type": "REPLY_COUNT_COMPOUND",
     "params": {"title": "Doubt in the question!", "date": "2025-05-21"}},
    {"id": "mds2_accepted_maths2_aq52_q6", "category": "Mathematics for Data Science II", "type": "ACCEPTED_POST_ID",
     "params": {"title": "Maths-2 doubt: AQ5.2, Q6", "date": "2026-03-20"}},

    # ── Programming Concepts using Java ──────────────────────────────────────
    {"id": "java_reply_week8_grpa2",       "category": "Programming Concepts using Java", "type": "REPLY_COUNT_COMPOUND",
     "params": {"title": "Week 8 - Grpa 2", "date": "2026-04-06"}},
    {"id": "java_agg_likes_q3_2025",       "category": "Programming Concepts using Java", "type": "AGGREGATE_LIKES",
     "params": {"start": "2025-07-01", "end": "2025-09-30"}},
    {"id": "java_accepted_is_subjective",  "category": "Programming Concepts using Java", "type": "ACCEPTED_POST_ID",
     "params": {"title": "Is java a subjective paper?", "date": "2025-02-17"}},
    {"id": "java_accepted_oppe2_mock",     "category": "Programming Concepts using Java", "type": "ACCEPTED_POST_ID",
     "params": {"title": "JAVA OPPE-2 Mock test", "date": "2026-04-24"}},
    {"id": "java_total_posts_jan_sep25",   "category": "Programming Concepts using Java", "type": "TOTAL_POSTS",
     "params": {"start": "2025-01-01", "end": "2025-09-30"}},
    {"id": "java_reply_no_one_meeting",    "category": "Programming Concepts using Java", "type": "REPLY_COUNT_COMPOUND",
     "params": {"title": "No one in the meeting", "date": "2026-02-06"}},
    {"id": "java_total_posts_h2_2025",     "category": "Programming Concepts using Java", "type": "TOTAL_POSTS",
     "params": {"start": "2025-07-01", "end": "2025-12-31"}},
    {"id": "java_tag_clarification_q3_2025","category": "Programming Concepts using Java", "type": "TAG_COUNT",
     "params": {"tag": "clarification", "start": "2025-07-01", "end": "2025-09-30"}},
    {"id": "java_reply_is_oppe2_mandatory","category": "Programming Concepts using Java", "type": "REPLY_COUNT_COMPOUND",
     "params": {"title": "Is OPPE2 mandatory?", "date": "2025-12-07"}},
    {"id": "java_accepted_not_received_sct","category": "Programming Concepts using Java", "type": "ACCEPTED_POST_ID",
     "params": {"title": "Not received SCT proctor meet", "date": "2025-05-24"}},

    # ── Machine Learning Foundations ──────────────────────────────────────────
    {"id": "mlf_tag_clarification_q1_2025","category": "Machine Learning Foundations", "type": "TAG_COUNT",
     "params": {"tag": "clarification", "start": "2025-01-01", "end": "2025-03-31"}},
    {"id": "mlf_agg_likes_h2_2025",        "category": "Machine Learning Foundations", "type": "AGGREGATE_LIKES",
     "params": {"start": "2025-07-01", "end": "2025-12-31"}},
    {"id": "mlf_accepted_doubt_mock_q11",  "category": "Machine Learning Foundations", "type": "ACCEPTED_POST_ID",
     "params": {"title": "Doubt in end term mock Q11 to 13", "date": "2025-04-11"}},
    {"id": "mlf_tag_diploma_h2_2025",      "category": "Machine Learning Foundations", "type": "TAG_COUNT",
     "params": {"tag": "diploma-level", "start": "2025-07-01", "end": "2025-12-31"}},
    {"id": "mlf_tag_clarification_h1_2025","category": "Machine Learning Foundations", "type": "TAG_COUNT",
     "params": {"tag": "clarification", "start": "2025-01-01", "end": "2025-06-30"}},
    {"id": "mlf_reply_mock_q3",            "category": "Machine Learning Foundations", "type": "REPLY_COUNT_COMPOUND",
     "params": {"title": "Mock Q-3", "date": "2025-07-12"}},
    {"id": "mlf_agg_likes_2025",           "category": "Machine Learning Foundations", "type": "AGGREGATE_LIKES",
     "params": {"start": "2025-01-01", "end": "2025-12-31"}},

    # ── Programming, Data Structures and Algorithms ───────────────────────────
    {"id": "pdsa_accepted_sct_mails",      "category": "Programming, Data Structures and Algorithms", "type": "ACCEPTED_POST_ID",
     "params": {"title": "Clarification regarding SCT mails", "date": "2025-09-29"}},
    {"id": "pdsa_tag_clarification_q4_2025","category": "Programming, Data Structures and Algorithms", "type": "TAG_COUNT",
     "params": {"tag": "clarification", "start": "2025-10-01", "end": "2025-12-31"}},
    {"id": "pdsa_accepted_oppe_mock_ppa6", "category": "Programming, Data Structures and Algorithms", "type": "ACCEPTED_POST_ID",
     "params": {"title": "Oppe mock question ppa 6", "date": "2025-08-16"}},
    {"id": "pdsa_top_replier_2026_q1q2",   "category": "Programming, Data Structures and Algorithms", "type": "TOP_REPLIER",
     "params": {"start": "2026-01-01", "end": "2026-04-30"}},
    {"id": "pdsa_unique_creators_h2_2025", "category": "Programming, Data Structures and Algorithms", "type": "UNIQUE_CREATORS",
     "params": {"start": "2025-07-01", "end": "2025-12-31"}},
    {"id": "pdsa_accepted_disc_w8_score",  "category": "Programming, Data Structures and Algorithms", "type": "ACCEPTED_POST_ID",
     "params": {"title": "Discrepancy in Week-8 Score Calculation", "date": "2026-04-15"}},
    {"id": "pdsa_agg_likes_2025",          "category": "Programming, Data Structures and Algorithms", "type": "AGGREGATE_LIKES",
     "params": {"start": "2025-01-01", "end": "2025-12-31"}},
    {"id": "pdsa_top_answer_h1_2025",      "category": "Programming, Data Structures and Algorithms", "type": "TOP_ANSWER_AUTHOR",
     "params": {"start": "2025-01-01", "end": "2025-06-30"}},
    {"id": "pdsa_tag_diploma_q1_2025",     "category": "Programming, Data Structures and Algorithms", "type": "TAG_COUNT",
     "params": {"tag": "diploma-level", "start": "2025-01-01", "end": "2025-03-31"}},
    {"id": "pdsa_agg_likes_h1_2025",       "category": "Programming, Data Structures and Algorithms", "type": "AGGREGATE_LIKES",
     "params": {"start": "2025-01-01", "end": "2025-06-30"}},

    # ── Modern Application Development II ────────────────────────────────────
    {"id": "mad2_unique_creators_q3_2025", "category": "Modern Application Development II", "type": "UNIQUE_CREATORS",
     "params": {"start": "2025-07-01", "end": "2025-09-30"}},
    {"id": "mad2_tag_compound_diploma_q1", "category": "Modern Application Development II", "type": "TAG_COUNT_COMPOUND",
     "params": {"tag": "diploma-level", "start": "2025-01-01", "end": "2025-03-31"}},
    {"id": "mad2_total_posts_h1_2025",     "category": "Modern Application Development II", "type": "TOTAL_POSTS",
     "params": {"start": "2025-01-01", "end": "2025-06-30"}},
    {"id": "mad2_top_liked_jan_sep25",     "category": "Modern Application Development II", "type": "TOP_LIKED_USER",
     "params": {"start": "2025-01-01", "end": "2025-09-30"}},

    # ── English II ────────────────────────────────────────────────────────────
    {"id": "eng2_top_replier_h1_2025",     "category": "English II", "type": "TOP_REPLIER",
     "params": {"start": "2025-01-01", "end": "2025-06-30"}},
    {"id": "eng2_top_answer_apr_dec25",    "category": "English II", "type": "TOP_ANSWER_AUTHOR",
     "params": {"start": "2025-04-01", "end": "2025-12-31"}},
    {"id": "eng2_tag_clarification_2025",  "category": "English II", "type": "TAG_COUNT",
     "params": {"tag": "clarification", "start": "2025-01-01", "end": "2025-12-31"}},
    {"id": "eng2_top_answer_oct_dec25",    "category": "English II", "type": "TOP_ANSWER_AUTHOR",
     "params": {"start": "2025-10-01", "end": "2025-12-31"}},
    {"id": "eng2_reply_doubt_quiz2",       "category": "English II", "type": "REPLY_COUNT_COMPOUND",
     "params": {"title": "Doubt in quiz 2 question in English II", "date": "2026-04-15"}},
    {"id": "eng2_accepted_aq2_1_5",        "category": "English II", "type": "ACCEPTED_POST_ID",
     "params": {"title": "Aq2.1-5", "date": "2025-01-20"}},
]


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    args = sys.argv[1:]

    if "--fetch" in args:
        log("Fetching all categories...")
        fetch_all_categories()

    elif "--validate" in args:
        ok = run_validation()
        sys.exit(0 if ok else 1)

    elif "--answer" in args:
        idx  = args.index("--answer")
        path = args[idx + 1] if idx + 1 < len(args) else None
        if not path:
            log("Usage: python discourse_universal.py --answer questions.json")
            sys.exit(1)
        with open(path) as f:
            questions = json.load(f)
        results = run_questions(questions)
        out = path.replace(".json", "_answers.json")
        with open(out, "w") as f:
            json.dump(results, f, indent=2)
        log(f"\nSaved answers → {out}")

    elif "--unanswered" in args:
        log(f"\nAnswering {len(UNANSWERED_QUESTIONS)} unanswered questions...\n")
        results = run_questions(UNANSWERED_QUESTIONS)
        with open("unanswered_answers.json", "w") as f:
            json.dump(results, f, indent=2)
        log("\nSaved → unanswered_answers.json")

    else:
        # Default: run both validation + unanswered questions
        log("Running validation first...")
        run_validation()

        log(f"\nAnswering {len(UNANSWERED_QUESTIONS)} unanswered questions...")
        results = run_questions(UNANSWERED_QUESTIONS)
        with open("unanswered_answers.json", "w") as f:
            json.dump(results, f, indent=2)
        log("\nSaved → unanswered_answers.json")

    _log_fh.close()
