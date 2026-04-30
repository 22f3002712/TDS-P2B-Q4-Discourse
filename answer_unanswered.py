"""
answer_unanswered.py
═══════════════════════════════════════════════════════════════════════════════
Reads the "Unanswered Questions" tab of the Google Sheet, answers every row
that has an empty Manual Answer column (column D), and writes the answer back.

After this script finishes, run buildMasterTable() in Apps Script so the new
answers propagate to everyone's "Best Matched Answers" column.

ONE-TIME SETUP
──────────────
1. Enable APIs at https://console.cloud.google.com
       Google Sheets API
       Google Drive API

2. Create a Service Account → download JSON key → save as credentials.json
   next to this script.

3. Share your Google Sheet with the service account email (Editor access).

4. Set SPREADSHEET_NAME below to the exact title of your Google Sheet.

5. Install dependencies:
       pip install gspread google-auth

6. Make sure discourse_universal.py is in the same directory AND you have
   already run:
       python discourse_universal.py --fetch

USAGE
─────
   python answer_unanswered.py            # answer all empty rows
   python answer_unanswered.py --dry-run  # print answers without writing

CRON (run every hour to stay current)
──────────────────────────────────────
   0 * * * * cd /path/to/scripts && python answer_unanswered.py >> cron.log 2>&1

UNANSWERED QUESTIONS SHEET COLUMNS
─────────────────────────────────────────────────────────────────────────────
  A  Categories
  B  Type
  C  Question
  D  Manual Answer       ← this script writes here
  E  Verified ✅         ← if filled, row is skipped (already confirmed correct)
  F  Wrong ❌            ← comma-separated wrong answers; script will not
                            re-enter any answer that appears here
"""

import os
import re
import sys
import time
import traceback
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials

# discourse_universal.py must be in the same directory.
from discourse_universal import (
    QueryEngine,
    get_category,
    CATEGORY_SLUGS,
    QUESTION_TYPES,
    log,
)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION  ←  edit these
# ─────────────────────────────────────────────────────────────────────────────

SPREADSHEET_NAME = "Discourse KB Analysis spreadsheet"   # Exact title of your Google Sheet
CREDENTIALS_FILE = "credentials.json"                    # Service account JSON key

UNANSWERED_TAB   = "Unanswered Questions"

# Column indices (0-based)
COL_CATEGORY = 0   # A
COL_TYPE     = 1   # B
COL_QUESTION = 2   # C
COL_ANSWER   = 3   # D  ← this script writes here
COL_VERIFIED = 4   # E  ← if filled, skip this row (already confirmed)
COL_WRONG    = 5   # F  ← comma-separated wrong answers to never re-enter

# Categories for which you have no Discourse access — rows are logged but skipped
SKIP_CATEGORIES: set[str] = set()

# Retry settings for the Sheets API
SHEETS_MAX_RETRIES = 3
SHEETS_RETRY_WAIT  = 5   # seconds between retries

# ─────────────────────────────────────────────────────────────────────────────
# QUESTION TEXT PARSER
# Maps a free-text question + category + type → QueryEngine params dict
# ─────────────────────────────────────────────────────────────────────────────

# ── Title-extraction regexes ──────────────────────────────────────────────────
# IMPORTANT: we use .+? (non-greedy) instead of [^""]+ so that titles
# containing inner double quotes (e.g. "Class not registered" ERROR ...) are
# matched correctly.  The terminator (what follows the closing quote) is what
# forces the regex engine to skip over inner quotes and find the right boundary.

# "the solved topic "TITLE" (posted by USER on YYYY-MM-DD)"
# Terminator: literal "(posted by" after the closing quote.
_TOPIC_POSTED_RE = re.compile(
    r"""solved\s+topic\s+["\u201c\u2018](.+?)["\u201d\u2019]\s*\(posted\s+by""",
    re.IGNORECASE | re.DOTALL,
)
_POSTED_DATE_RE = re.compile(
    r"""on\s+(\d{4}-\d{2}-\d{2})\)""",
    re.IGNORECASE,
)

# "titled "TITLE" in the ... Discourse category was posted by USER on YYYY-MM-DD"
# Terminator: "in the" after the closing quote.
_TOPIC_TITLED_RE = re.compile(
    r"""titled\s+["\u201c\u2018](.+?)["\u201d\u2019]\s*in\s+the""",
    re.IGNORECASE | re.DOTALL,
)
_TITLED_DATE_RE = re.compile(
    r"""posted\s+by\s+\S+\s+on\s+(\d{4}-\d{2}-\d{2})""",
    re.IGNORECASE,
)

# "between YYYY-MM-DD and YYYY-MM-DD"
_BETWEEN_RE = re.compile(
    r"between\s+(\d{4}-\d{2}-\d{2})\s+and\s+(\d{4}-\d{2}-\d{2})",
    re.IGNORECASE,
)

# "created between YYYY-MM-DD and YYYY-MM-DD"
_CREATED_BETWEEN_RE = re.compile(
    r"created\s+between\s+(\d{4}-\d{2}-\d{2})\s+and\s+(\d{4}-\d{2}-\d{2})",
    re.IGNORECASE,
)

# "tagged with 'TAG'" or tagged with "TAG"
_TAG_RE = re.compile(
    r"""tagged\s+with\s+['"]([^'"]+)['"]""",
    re.IGNORECASE,
)


def _normalise_type(raw: str) -> str:
    return raw.strip().upper().replace(" ", "_")


def _extract_topic_params(text: str) -> tuple[str | None, str | None]:
    """
    Extract (title, date) from a question string.
    Handles titles with inner double-quote characters by using non-greedy
    matching anchored to the specific text that follows the closing quote.
    """
    # Form 1: solved topic "TITLE" (posted by USER on DATE)
    m = _TOPIC_POSTED_RE.search(text)
    if m:
        dm = _POSTED_DATE_RE.search(text, m.end())
        if dm:
            return m.group(1).strip(), dm.group(1)

    # Form 2: titled "TITLE" in the ... was posted by USER on DATE
    m = _TOPIC_TITLED_RE.search(text)
    if m:
        dm = _TITLED_DATE_RE.search(text, m.end())
        if dm:
            return m.group(1).strip(), dm.group(1)

    return None, None


def _extract_date_range(text: str) -> tuple[str | None, str | None]:
    m = _BETWEEN_RE.search(text)
    return (m.group(1), m.group(2)) if m else (None, None)


def _extract_created_range(text: str) -> tuple[str | None, str | None]:
    m = _CREATED_BETWEEN_RE.search(text) or _BETWEEN_RE.search(text)
    return (m.group(1), m.group(2)) if m else (None, None)


def build_query_dict(category: str, raw_type: str, text: str) -> dict | None:
    """
    Parse a free-text question into a QueryEngine-compatible dict.
    Returns None if the question cannot be parsed.
    """
    qtype = _normalise_type(raw_type)
    if qtype not in QUESTION_TYPES:
        log(f"    Unknown type: '{raw_type}'")
        return None

    params: dict = {}

    if qtype in ("ACCEPTED_POST_ID", "REPLY_COUNT_COMPOUND"):
        title, date = _extract_topic_params(text)
        if not title or not date:
            log(f"    Could not extract title/date from: {text[:80]}…")
            return None
        params = {"title": title, "date": date}

    elif qtype in ("TOTAL_POSTS", "AGGREGATE_LIKES", "TOP_LIKED_USER", "TOP_ANSWER_AUTHOR"):
        start, end = _extract_date_range(text)
        if not start:
            log(f"    Could not extract date range from: {text[:80]}…")
            return None
        params = {"start": start, "end": end}

    elif qtype in ("UNIQUE_CREATORS", "TOP_REPLIER"):
        start, end = _extract_created_range(text)
        if not start:
            log(f"    Could not extract date range from: {text[:80]}…")
            return None
        params = {"start": start, "end": end}

    elif qtype in ("TAG_COUNT", "TAG_COUNT_COMPOUND"):
        tm = _TAG_RE.search(text)
        dm = _BETWEEN_RE.search(text)
        if not tm or not dm:
            log(f"    Could not extract tag/dates from: {text[:80]}…")
            return None
        params = {
            "tag":   tm.group(1).strip(),
            "start": dm.group(1),
            "end":   dm.group(2),
        }
        
    elif qtype == "UNIQUE_CREATORS_COMPOUND":
        start, end = _extract_created_range(text)
        if not start:
            log(f"    Could not extract date range from: {text[:80]}…")
            return None
        params = {"start": start, "end": end}

    else:
        log(f"    Unhandled type: '{qtype}'")
        return None

    return {"category": category, "type": qtype, "params": params}


# ─────────────────────────────────────────────────────────────────────────────
# GOOGLE SHEETS CLIENT
# ─────────────────────────────────────────────────────────────────────────────

def connect(spreadsheet_name: str) -> gspread.Spreadsheet:
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds  = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    return client.open(spreadsheet_name)


def _retry_sheet_op(fn):
    for attempt in range(SHEETS_MAX_RETRIES):
        try:
            return fn()
        except gspread.exceptions.APIError as e:
            if attempt < SHEETS_MAX_RETRIES - 1:
                log(f"  Sheets API error (attempt {attempt+1}): {e}. Retrying in {SHEETS_RETRY_WAIT}s…")
                time.sleep(SHEETS_RETRY_WAIT)
            else:
                raise


# ─────────────────────────────────────────────────────────────────────────────
# CATEGORY PRELOADER
# ─────────────────────────────────────────────────────────────────────────────

def preload_needed_categories(categories: set[str]) -> dict[str, bool]:
    available = {}
    for cat in sorted(categories):
        data_file = os.path.join("discourse_data", cat.replace(" ", "_") + ".json")
        if not os.path.exists(data_file):
            log(f"  ✗ {cat}  (no cache file — run: python discourse_universal.py --fetch)")
            available[cat] = False
        else:
            try:
                get_category(cat)
                log(f"  ✓ {cat}")
                available[cat] = True
            except Exception as e:
                log(f"  ✗ {cat}  (load error: {e})")
                available[cat] = False
    return available


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main(dry_run: bool = False) -> None:
    log("=" * 70)
    log(f"answer_unanswered.py  started  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log(f"dry_run={dry_run}")
    log("=" * 70)

    # ── 1. Connect to the sheet ───────────────────────────────────────────────
    if not os.path.exists(CREDENTIALS_FILE):
        log(f"ERROR: {CREDENTIALS_FILE!r} not found. Follow SETUP steps.")
        sys.exit(1)

    log(f"\nConnecting to sheet: {SPREADSHEET_NAME!r}…")
    try:
        spreadsheet = connect(SPREADSHEET_NAME)
    except Exception as e:
        log(f"ERROR connecting to Google Sheets: {e}")
        sys.exit(1)

    try:
        ws = spreadsheet.worksheet(UNANSWERED_TAB)
    except gspread.WorksheetNotFound:
        log(
            f"ERROR: Tab {UNANSWERED_TAB!r} not found.\n"
            "Run buildMasterTable() in Apps Script first to create it."
        )
        sys.exit(1)

    # ── 2. Read all rows ──────────────────────────────────────────────────────
    all_rows = _retry_sheet_op(ws.get_all_values)
    if len(all_rows) <= 1:
        log("No question rows found in the sheet. Nothing to do.")
        return

    data_rows = all_rows[1:]
    log(f"\n{len(data_rows)} total rows in {UNANSWERED_TAB!r}.\n")

    # ── 3. Identify rows that still need answers ──────────────────────────────
    # A row is skipped if:
    #   - col E (Verified) is already filled — answer is confirmed, don't touch
    #   - col D (Manual Answer) is already filled — already has a manual answer
    # A computed answer is rejected if it appears in col F (Wrong answers).

    pending = []  # (sheet_row_number, category, qtype, qtext, wrong_set)
    for i, row in enumerate(data_rows):
        while len(row) < 6:
            row.append("")

        category = row[COL_CATEGORY].strip()
        qtype    = row[COL_TYPE].strip()
        qtext    = row[COL_QUESTION].strip()
        existing = row[COL_ANSWER].strip()
        verified = row[COL_VERIFIED].strip()
        wrong_str= row[COL_WRONG].strip()

        if verified:
            continue   # already confirmed correct — never overwrite
        if existing:
            continue   # already has a manual answer — don't overwrite
        if not category or not qtype or not qtext:
            continue   # incomplete or footer row

        wrong_set = {w.strip().lower() for w in wrong_str.split(",") if w.strip()}
        pending.append((i + 2, category, qtype, qtext, wrong_set))

    log(f"{len(pending)} rows still need answers.\n")
    if not pending:
        log("All rows already have answers or verified answers. Nothing to do.")
        return

    # ── 4. Group by category ─────────────────────────────────────────────────
    by_category: dict[str, list] = {}
    for sheet_row, category, qtype, qtext, wrong_set in pending:
        by_category.setdefault(category, []).append((sheet_row, qtype, qtext, wrong_set))

    needed_cats = set(by_category) - SKIP_CATEGORIES

    # ── 5. Pre-load only needed categories ───────────────────────────────────
    log("Pre-loading required category caches…")
    available = preload_needed_categories(needed_cats)
    log("")

    # ── 6. Answer each question ───────────────────────────────────────────────
    engine    = QueryEngine()
    answered  = 0
    not_found = 0
    skipped   = 0
    errors    = 0
    rejected  = 0   # answer found but was in the wrong list

    all_updates: list[gspread.Cell] = []

    for category, questions in sorted(by_category.items()):
        log(f"── {category} ({len(questions)} pending) ──")

        if category in SKIP_CATEGORIES:
            log(f"   Skipped — in SKIP_CATEGORIES.")
            skipped += len(questions)
            continue

        if category not in CATEGORY_SLUGS:
            log(f"   WARNING: not in CATEGORY_SLUGS — skipping.")
            skipped += len(questions)
            continue

        if not available.get(category, False):
            log(f"   No cached data available — skipping.")
            skipped += len(questions)
            continue

        for sheet_row, qtype, qtext, wrong_set in questions:
            short = qtext[:70] + ("…" if len(qtext) > 70 else "")

            q_dict = build_query_dict(category, qtype, qtext)
            if q_dict is None:
                log(f"   Row {sheet_row:>4}  PARSE FAIL  [{qtype}]  {short}")
                errors += 1
                continue

            try:
                answer = engine.answer(q_dict)
            except Exception as e:
                log(f"   Row {sheet_row:>4}  ENGINE ERROR  {e}")
                log("   " + traceback.format_exc().splitlines()[-1])
                errors += 1
                continue

            # Reject sentinel strings that are not real answers
            is_valid = (
                answer
                and not answer.startswith("ERROR")
                and not answer.startswith("TOPIC NOT FOUND")
                and not answer.startswith("NO ACCEPTED")
                and answer.lower() != "none"
            )

            if not is_valid:
                log(f"   Row {sheet_row:>4}  ✗  [{qtype:<25}]  {answer!r}  | {short}")
                not_found += 1
                continue

            # Reject if this exact answer was previously marked wrong
            if answer.lower() in wrong_set:
                log(f"   Row {sheet_row:>4}  REJECTED (wrong list)  [{qtype:<25}]  {answer!r}")
                rejected += 1
                continue

            log(f"   Row {sheet_row:>4}  ✓  [{qtype:<25}]  {answer}")
            if not dry_run:
                all_updates.append(gspread.Cell(sheet_row, COL_ANSWER + 1, answer))
            answered += 1

        log("")

    # ── 7. Batch-write all answers ────────────────────────────────────────────
    if all_updates and not dry_run:
        log(f"Writing {len(all_updates)} answers to the sheet…")
        _retry_sheet_op(lambda: ws.update_cells(all_updates, value_input_option="RAW"))
        log("Done.\n")
    elif dry_run:
        log("[dry-run] No changes written to the sheet.\n")

    # ── 8. Summary ────────────────────────────────────────────────────────────
    log("=" * 70)
    log(
        f"Summary:  answered={answered}  not_found={not_found}"
        f"  rejected_wrong={rejected}  skipped={skipped}  parse_errors={errors}"
    )
    if answered > 0 and not dry_run:
        log(
            "\nNext step: run buildMasterTable() in Apps Script so these new"
            "\nanswers appear in everyone's Best Matched Answers column."
        )
    log("=" * 70)


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    main(dry_run=dry_run)
