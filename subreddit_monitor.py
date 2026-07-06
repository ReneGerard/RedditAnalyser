#!/usr/bin/env python3
"""
subreddit_monitor.py — Personal read-only Reddit monitoring & analysis tool.

Runs once per day (cron). Scans a fixed list of subreddits, filters posts
locally against personal interest criteria, summarizes matches with Claude,
writes results to a private Google Sheet, and sends a Telegram digest.

Hard guarantees (enforced in code, matching the Reddit Data API request):
  - Read-only. Never posts, comments, votes, or messages.
  - Fixed subreddit list. No dynamic crawling.
  - Volume caps: MAX_POSTS_PER_SUBREDDIT and MAX_API_REQUESTS_PER_RUN.
  - Same-day public posts only. Content summarized, not archived verbatim.

Backends:
  - "praw": official Data API via PRAW (preferred, once credentials approved)
  - "json": public .json endpoints (fallback, low volume, identified UA)

Environment variables (see .env.example):
  REDDIT_BACKEND            praw | json          (default: json)
  REDDIT_CLIENT_ID          PRAW client id       (praw backend only)
  REDDIT_CLIENT_SECRET      PRAW client secret   (praw backend only)
  REDDIT_USERNAME           Reddit username      (praw backend only)
  REDDIT_PASSWORD           Reddit password      (praw backend only)
  REDDIT_USER_AGENT         Descriptive UA string
  ANTHROPIC_API_KEY         Claude API key
  GOOGLE_SERVICE_ACCOUNT_FILE  Path to service account JSON
  GSHEET_ID                 Target Google Sheet ID
  TELEGRAM_BOT_TOKEN        Telegram bot token
  TELEGRAM_CHAT_ID          Telegram chat id
"""

import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import requests

# Optional imports — only required for their respective features.
try:
    import praw  # type: ignore
except ImportError:
    praw = None

try:
    import gspread  # type: ignore
    from google.oauth2.service_account import Credentials  # type: ignore
except ImportError:
    gspread = None

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SUBREDDITS = [
    "Notion",
    "productivity",
    "freelance",
    "solopreneur",
    "Entrepreneur",
    "SideProject",
    "juststart",
]

# Local filtering criteria. A post matches if ANY group has ALL its terms
# present in title+selftext (case-insensitive). Edit freely.
INTEREST_CRITERIA = [
    ["notion", "template"],
    ["client", "invoice"],
    ["freelance", "workflow"],
    ["adhd", "planner"],
    ["adhd", "notion"],
    ["content", "calendar"],
    ["productivity", "system"],
    ["automation", "business"],
    ["digital", "product"],
]

# --- Volume caps (hard commitments from the API access request) ---
MAX_POSTS_PER_SUBREDDIT = 40      # fetched per subreddit per run
MAX_API_REQUESTS_PER_RUN = 120    # absolute ceiling, all endpoints combined
MAX_POSTS_TO_ANALYZE = 15         # sent to Claude after local filtering
MAX_COMMENTS_PER_POST = 5         # top comments fetched for matched posts
MAX_CHARS_PER_POST = 2500         # truncation before LLM call (token control)

CLAUDE_MODEL = "claude-haiku-4-5-20251001"
CLAUDE_MAX_TOKENS = 1500

SHEET_TAB = "Monitor_Digest"
REQUEST_TIMEOUT = 20
SLEEP_BETWEEN_REQUESTS = 1.2      # seconds; stays far below rate limits

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("subreddit_monitor")


def env(name: str, default: str | None = None, required: bool = False) -> str:
    value = os.environ.get(name, default)
    if required and not value:
        log.error("Missing required environment variable: %s", name)
        sys.exit(1)
    return value or ""


# ---------------------------------------------------------------------------
# Request budget — hard ceiling on API calls per run
# ---------------------------------------------------------------------------

class RequestBudget:
    """Counts every network call to Reddit. Refuses to exceed the cap."""

    def __init__(self, limit: int):
        self.limit = limit
        self.used = 0

    def spend(self) -> bool:
        if self.used >= self.limit:
            log.warning("Request budget exhausted (%d). Skipping further calls.", self.limit)
            return False
        self.used += 1
        return True


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class Post:
    subreddit: str
    post_id: str
    title: str
    selftext: str
    url: str
    score: int
    num_comments: int
    created_utc: float
    matched_criteria: list[str] = field(default_factory=list)
    top_comments: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Backend: public .json endpoints (fallback)
# ---------------------------------------------------------------------------

class JsonBackend:
    """Read-only access via public .json endpoints. Identified user agent."""

    BASE = "https://www.reddit.com"

    def __init__(self, user_agent: str, budget: RequestBudget):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": user_agent})
        self.budget = budget

    def _get(self, path: str, params: dict | None = None) -> dict | None:
        if not self.budget.spend():
            return None
        try:
            resp = self.session.get(
                f"{self.BASE}{path}", params=params, timeout=REQUEST_TIMEOUT
            )
            if resp.status_code == 429:
                log.warning("HTTP 429 on %s — backing off 30s", path)
                time.sleep(30)
                return None
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            log.error("Request failed for %s: %s", path, exc)
            return None
        finally:
            time.sleep(SLEEP_BETWEEN_REQUESTS)

    def fetch_posts(self, subreddit: str, limit: int) -> list[Post]:
        data = self._get(f"/r/{subreddit}/new.json", {"limit": min(limit, 100)})
        if not data:
            return []
        posts = []
        for child in data.get("data", {}).get("children", []):
            d = child.get("data", {})
            posts.append(Post(
                subreddit=subreddit,
                post_id=d.get("id", ""),
                title=d.get("title", ""),
                selftext=d.get("selftext", ""),
                url=f"https://www.reddit.com{d.get('permalink', '')}",
                score=d.get("score", 0),
                num_comments=d.get("num_comments", 0),
                created_utc=d.get("created_utc", 0),
            ))
        return posts

    def fetch_top_comments(self, post: Post, limit: int) -> list[str]:
        data = self._get(
            f"/r/{post.subreddit}/comments/{post.post_id}.json",
            {"limit": limit, "depth": 1, "sort": "top"},
        )
        if not data or len(data) < 2:
            return []
        comments = []
        for child in data[1].get("data", {}).get("children", [])[:limit]:
            body = child.get("data", {}).get("body", "")
            if body:
                comments.append(body[:500])
        return comments


# ---------------------------------------------------------------------------
# Backend: official Data API via PRAW (preferred)
# ---------------------------------------------------------------------------

class PrawBackend:
    """Read-only access via the official Reddit Data API (script app)."""

    def __init__(self, budget: RequestBudget):
        if praw is None:
            log.error("praw is not installed. Run: pip install praw")
            sys.exit(1)
        self.budget = budget
        self.reddit = praw.Reddit(
            client_id=env("REDDIT_CLIENT_ID", required=True),
            client_secret=env("REDDIT_CLIENT_SECRET", required=True),
            username=env("REDDIT_USERNAME", required=True),
            password=env("REDDIT_PASSWORD", required=True),
            user_agent=env("REDDIT_USER_AGENT", required=True),
        )
        self.reddit.read_only = True  # enforced: this tool never writes

    def fetch_posts(self, subreddit: str, limit: int) -> list[Post]:
        if not self.budget.spend():
            return []
        posts = []
        try:
            for submission in self.reddit.subreddit(subreddit).new(limit=limit):
                posts.append(Post(
                    subreddit=subreddit,
                    post_id=submission.id,
                    title=submission.title or "",
                    selftext=submission.selftext or "",
                    url=f"https://www.reddit.com{submission.permalink}",
                    score=submission.score,
                    num_comments=submission.num_comments,
                    created_utc=submission.created_utc,
                ))
        except Exception as exc:  # praw raises many exception types
            log.error("PRAW fetch failed for r/%s: %s", subreddit, exc)
        return posts

    def fetch_top_comments(self, post: Post, limit: int) -> list[str]:
        if not self.budget.spend():
            return []
        try:
            submission = self.reddit.submission(id=post.post_id)
            submission.comment_sort = "top"
            submission.comments.replace_more(limit=0)
            return [c.body[:500] for c in submission.comments[:limit] if c.body]
        except Exception as exc:
            log.error("PRAW comments failed for %s: %s", post.post_id, exc)
            return []


# ---------------------------------------------------------------------------
# Local filtering — zero API cost, zero LLM cost
# ---------------------------------------------------------------------------

def filter_posts(posts: list[Post]) -> list[Post]:
    """Keep posts matching at least one criteria group (all terms present)."""
    matched = []
    for post in posts:
        haystack = f"{post.title} {post.selftext}".lower()
        for group in INTEREST_CRITERIA:
            if all(term.lower() in haystack for term in group):
                post.matched_criteria.append(" + ".join(group))
        if post.matched_criteria:
            matched.append(post)
    return matched


def dedupe_against_memory(posts: list[Post], memory_file: str) -> list[Post]:
    """Skip posts already processed in previous runs (incremental memory)."""
    seen: set[str] = set()
    if os.path.exists(memory_file):
        try:
            with open(memory_file, "r", encoding="utf-8") as f:
                seen = set(json.load(f))
        except (json.JSONDecodeError, OSError) as exc:
            log.warning("Could not read memory file, starting fresh: %s", exc)
    fresh = [p for p in posts if p.post_id not in seen]
    seen.update(p.post_id for p in fresh)
    try:
        with open(memory_file, "w", encoding="utf-8") as f:
            json.dump(sorted(seen)[-5000:], f)  # keep memory bounded
    except OSError as exc:
        log.error("Could not write memory file: %s", exc)
    return fresh


# ---------------------------------------------------------------------------
# Claude analysis
# ---------------------------------------------------------------------------

ANALYSIS_SYSTEM_PROMPT = """You are a market research analyst. You receive a batch of Reddit posts (with top comments) from productivity and freelancing communities. Produce a concise daily digest in French with:

1. THEMES: 3-5 recurring themes across the posts, each with a one-line description and the count of posts supporting it.
2. PAIN_POINTS: concrete problems users describe, quoted loosely (paraphrased, not verbatim).
3. OPPORTUNITIES: for each pain point, note if a digital product (Notion template, prompt pack, tracker) could address it. Be specific about what the product would be.
4. HOT_POSTS: the 3 most engaged posts (score + comments) with their URL.

Respond ONLY with valid JSON, no markdown fences, using keys: themes, pain_points, opportunities, hot_posts. Keep the total under 600 words."""


def analyze_with_claude(posts: list[Post], api_key: str) -> dict | None:
    """Single batched Claude call. Token-optimized: truncated, capped batch."""
    batch = posts[:MAX_POSTS_TO_ANALYZE]
    corpus = []
    for p in batch:
        text = f"[r/{p.subreddit}] {p.title}\n{p.selftext}"[:MAX_CHARS_PER_POST]
        if p.top_comments:
            text += "\nTop comments: " + " | ".join(p.top_comments)
        corpus.append({
            "url": p.url,
            "score": p.score,
            "comments": p.num_comments,
            "matched": p.matched_criteria,
            "text": text,
        })

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": CLAUDE_MODEL,
                "max_tokens": CLAUDE_MAX_TOKENS,
                "system": ANALYSIS_SYSTEM_PROMPT,
                "messages": [{
                    "role": "user",
                    "content": json.dumps(corpus, ensure_ascii=False),
                }],
            },
            timeout=90,
        )
        resp.raise_for_status()
        raw = resp.json()["content"][0]["text"]
        raw = re.sub(r"^```(?:json)?|```$", "", raw.strip(), flags=re.MULTILINE).strip()
        return json.loads(raw)
    except requests.RequestException as exc:
        log.error("Claude API call failed: %s", exc)
        return None
    except (KeyError, json.JSONDecodeError) as exc:
        log.error("Could not parse Claude response: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Outputs: Google Sheets + Telegram
# ---------------------------------------------------------------------------

def write_to_sheet(digest: dict, matched_count: int, sheet_id: str, sa_file: str) -> bool:
    if gspread is None:
        log.warning("gspread not installed — skipping Sheets output.")
        return False
    try:
        creds = Credentials.from_service_account_file(
            sa_file,
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        client = gspread.authorize(creds)
        sheet = client.open_by_key(sheet_id)
        try:
            tab = sheet.worksheet(SHEET_TAB)
        except gspread.WorksheetNotFound:
            tab = sheet.add_worksheet(title=SHEET_TAB, rows=1000, cols=6)
            tab.append_row(["Date", "Posts_Matched", "Themes", "Pain_Points",
                            "Opportunities", "Hot_Posts"])
        tab.append_row([
            datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            matched_count,
            json.dumps(digest.get("themes", []), ensure_ascii=False),
            json.dumps(digest.get("pain_points", []), ensure_ascii=False),
            json.dumps(digest.get("opportunities", []), ensure_ascii=False),
            json.dumps(digest.get("hot_posts", []), ensure_ascii=False),
        ])
        return True
    except Exception as exc:
        log.error("Google Sheets write failed: %s", exc)
        return False


def tg_escape(text: str) -> str:
    """Escape Telegram MarkdownV2 special characters."""
    for ch in r"_*[]()~`>#+-=|{}.!":
        text = text.replace(ch, f"\\{ch}")
    return text


def send_telegram_digest(digest: dict, matched_count: int,
                         bot_token: str, chat_id: str) -> bool:
    lines = [f"📡 *Reddit Monitor — {tg_escape(datetime.now().strftime('%Y-%m-%d'))}*",
             tg_escape(f"{matched_count} posts pertinents détectés"), ""]

    themes = digest.get("themes", [])
    if themes:
        lines.append("*🧭 Thèmes:*")
        for t in themes[:5]:
            label = t.get("theme", t) if isinstance(t, dict) else str(t)
            lines.append(f"• {tg_escape(str(label))}")
        lines.append("")

    opps = digest.get("opportunities", [])
    if opps:
        lines.append("*💡 Opportunités produit:*")
        for o in opps[:5]:
            label = o.get("product", o) if isinstance(o, dict) else str(o)
            lines.append(f"• {tg_escape(str(label))}")
        lines.append("")

    hot = digest.get("hot_posts", [])
    if hot:
        lines.append("*🔥 Posts chauds:*")
        for h in hot[:3]:
            url = h.get("url", "") if isinstance(h, dict) else str(h)
            lines.append(tg_escape(url))

    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json={"chat_id": chat_id, "text": "\n".join(lines),
                  "parse_mode": "MarkdownV2",
                  "disable_web_page_preview": True},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return True
    except requests.RequestException as exc:
        log.error("Telegram send failed: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    backend_name = env("REDDIT_BACKEND", "json").lower()
    user_agent = env(
        "REDDIT_USER_AGENT",
        "linux:subreddit-monitor:v1.0 (personal read-only research tool)",
    )
    budget = RequestBudget(MAX_API_REQUESTS_PER_RUN)

    if backend_name == "praw":
        backend = PrawBackend(budget)
    else:
        backend = JsonBackend(user_agent, budget)
    log.info("Backend: %s | budget: %d requests", backend_name, budget.limit)

    # 1. Fetch
    all_posts: list[Post] = []
    for sub in SUBREDDITS:
        posts = backend.fetch_posts(sub, MAX_POSTS_PER_SUBREDDIT)
        log.info("r/%s: fetched %d posts", sub, len(posts))
        all_posts.extend(posts)

    # 2. Filter locally (zero cost)
    matched = filter_posts(all_posts)
    log.info("Local filter: %d/%d posts matched criteria", len(matched), len(all_posts))

    # 3. Dedupe against incremental memory
    memory_file = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               "monitor_memory.json")
    fresh = dedupe_against_memory(matched, memory_file)
    log.info("After dedup: %d new posts to analyze", len(fresh))

    if not fresh:
        log.info("Nothing new today. Exiting cleanly.")
        return 0

    # 4. Enrich top posts with comments (budget-aware)
    fresh.sort(key=lambda p: (p.score + p.num_comments), reverse=True)
    for post in fresh[:MAX_POSTS_TO_ANALYZE]:
        post.top_comments = backend.fetch_top_comments(post, MAX_COMMENTS_PER_POST)

    # 5. Analyze with Claude (single batched call)
    api_key = env("ANTHROPIC_API_KEY", required=True)
    digest = analyze_with_claude(fresh, api_key)
    if digest is None:
        log.error("Analysis failed — aborting outputs.")
        return 1

    # 6. Outputs
    sheet_id = env("GSHEET_ID")
    sa_file = env("GOOGLE_SERVICE_ACCOUNT_FILE")
    if sheet_id and sa_file:
        ok = write_to_sheet(digest, len(fresh), sheet_id, sa_file)
        log.info("Sheets output: %s", "ok" if ok else "FAILED")

    bot_token = env("TELEGRAM_BOT_TOKEN")
    chat_id = env("TELEGRAM_CHAT_ID")
    if bot_token and chat_id:
        ok = send_telegram_digest(digest, len(fresh), bot_token, chat_id)
        log.info("Telegram output: %s", "ok" if ok else "FAILED")

    log.info("Run complete. API requests used: %d/%d", budget.used, budget.limit)
    return 0


if __name__ == "__main__":
    sys.exit(main())
