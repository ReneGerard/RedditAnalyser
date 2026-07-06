# subreddit-monitor

A personal, **read-only** Reddit monitoring and analysis tool. Runs once per day
on a cron schedule, scans a fixed list of subreddits, filters posts locally
against personal interest criteria, summarizes the matches with Claude, and
delivers a private daily digest (Google Sheets + Telegram).

Built as a single-user research assistant — it never posts, comments, votes,
or messages. All participation on Reddit remains manual.

## How it works

```
cron (daily)
  └─ fetch new posts from a fixed subreddit list  (capped volume)
      └─ local keyword filtering                  (zero API / zero LLM cost)
          └─ dedupe against incremental memory    (monitor_memory.json)
              └─ single batched Claude analysis   (themes, pain points, opportunities)
                  ├─ append digest to Google Sheet
                  └─ send Telegram summary
```

## Hard limits (enforced in code)

| Limit | Value |
|---|---|
| Posts fetched per subreddit per run | 40 |
| Total API requests per run | 120 |
| Posts analyzed by LLM per run | 15 |
| Comments fetched per matched post | 5 |
| Runs per day | 1 (cron) |

The `RequestBudget` class hard-stops all network calls once the ceiling is
reached. The tool processes same-day public posts only, stores summaries
rather than verbatim content, and redistributes nothing.

## Backends

- **`praw`** — official Reddit Data API via a script-type app (preferred).
  The client is forced to `read_only = True`.
- **`json`** — public `.json` endpoints with an identified user agent
  (fallback while API credentials are pending approval).

Switch with the `REDDIT_BACKEND` environment variable.

## Setup

```bash
pip install requests praw gspread google-auth
cp .env.example .env   # fill in your values
python subreddit_monitor.py
```

Cron example (daily at 07:30):

```
30 7 * * * cd /path/to/subreddit-monitor && /usr/bin/env $(cat .env | xargs) python3 subreddit_monitor.py >> monitor.log 2>&1
```

## Configuration

Edit the top of `subreddit_monitor.py`:

- `SUBREDDITS` — fixed list of communities to scan
- `INTEREST_CRITERIA` — keyword groups; a post matches if all terms of any
  group appear in its title + body
- Volume caps — lower them freely; raising them is discouraged

## Privacy & compliance

- Read-only scopes only (`read`, `identity`)
- No user profiling, no bulk archiving, no redistribution
- Output is private (personal Google Sheet + personal Telegram chat)
- Not used to train AI models

## License

MIT
