# Clickstream Sessionization (PySpark)

Splits clickstream events `(user_id, ts, url)` into **sessions**: a new session
starts when a user is inactive for **more than 30 minutes**. Then it computes
**sessions per day** and the **average session length**.

The whole thing uses window functions, with no self-joins and no UDFs.

```
clickstream-sessionization/
├── data/clickstream.csv   # sample data: 16 events, 3 users, 2 days
├── sessionize.py          # the Spark job
└── requirements.txt
```

## How to run

Requirements: Python 3.9+, Java 17, and PySpark.

```bash
pip install -r requirements.txt
python sessionize.py                     # uses data/clickstream.csv
python sessionize.py path/to/other.csv   # or your own file
```

---

## Architecture

The job is a three-step pipeline. Each step is a small function that takes a
DataFrame and returns a DataFrame, so you can test or reuse each step on its own.

```
 clickstream.csv
       │  read with an explicit schema (ts → TimestampType)
       ▼
 ┌─────────────────────┐   window: PARTITION BY user_id ORDER BY ts
 │ 1. assign_sessions  │   lag() → gap → is_new_session flag → running sum
 └─────────────────────┘   output: one row per EVENT, plus session_id
       ▼
 ┌─────────────────────┐   groupBy(user_id, session_id)
 │ 2. summarize_sessions│  min(ts), max(ts), count → session_seconds
 └─────────────────────┘   output: one row per SESSION
       ▼
 ┌─────────────────────┐   groupBy(session_date)
 │ 3. daily_metrics    │   count(*), avg(session_seconds)
 └─────────────────────┘   output: one row per DAY
```

**Why window functions?** A window lets each row look at its neighbours
(the previous click) *without* collapsing rows the way `groupBy` does. Spark
shuffles the data once so all of a user's events sit in the same partition,
sorts them by time, and then walks through them in order. That scales well,
because every user can be processed in parallel.

---

## The logic, step by step

### Step 1: Assign session ids (the core trick)

Here is user `u1` going through each column:

| ts               | prev_ts          | gap (min) | is_new_session | session_num (running sum) |
|------------------|------------------|-----------|----------------|---------------------------|
| 11-01 09:00      | null             | –         | **1** (first click) | 1 |
| 11-01 09:05      | 09:00            | 5         | 0              | 1 |
| 11-01 09:20      | 09:05            | 15        | 0              | 1 |
| 11-01 09:50      | 09:20            | 30        | 0 (not > 30)   | 1 |
| 11-01 11:00      | 09:50            | 70        | **1**          | 2 |
| 11-01 11:10      | 11:00            | 10        | 0              | 2 |
| 11-02 08:00      | 11-01 11:10      | 1250      | **1**          | 3 |
| 11-02 08:15      | 08:00            | 15        | 0              | 3 |

1. **`lag("ts")`** gets the previous event's timestamp for the same user.
2. **gap** = `ts - prev_ts` in seconds (timestamps cast to epoch seconds).
3. **`is_new_session`** = 1 if this is the user's first event or the gap is
   over 30 minutes, otherwise 0. These 1s mark where a session starts.
4. **Running `sum(is_new_session)`** turns those boundary markers into session
   numbers 1, 1, 1, 1, 2, 2, 3, 3. This "flag, then cumulative sum" pattern is
   the standard way to split a sequence into groups.
5. **`session_id`** = `user_id-session_num` (for example `u1-2`), which makes it
   unique across users.

The running sum uses an explicit frame,
`rowsBetween(unboundedPreceding, currentRow)`. The default frame is
`RANGE`-based, and with it two events that share a timestamp would be added
together.

### Step 2: One row per session

Group the events by `session_id`:

- `session_start = min(ts)` and `session_end = max(ts)`
- `session_seconds = session_end - session_start`
- `session_date = to_date(session_start)`. A session belongs to the day it
  **started** on.

### Step 3: Daily metrics

Group the sessions by `session_date`:

- `sessions = count(*)`
- `avg_session_minutes = avg(session_seconds) / 60`

---

## Design decisions

| Decision | Choice | Why |
|---|---|---|
| Inactivity threshold | gap **> 30 min** starts a new session | A gap of exactly 30 min stays in the same session (`u1` 09:20 → 09:50) |
| Session length | last event − first event | A session with one event has length **0**, because the log has no end signal |
| Day of a session | the day the session **started** | `u2-3` (23:50 → 00:10) counts once, on 11-01, and is never split |
| Time zone | `spark.sql.session.timeZone = UTC` | Results don't change with the machine's time zone or DST |
| Schema | explicit, not inferred | Faster, and `ts` is guaranteed to be a timestamp |

---

## Expected output for the sample data

**Sessions**

| session_id | start | end | events | minutes |
|---|---|---|---|---|
| u1-1 | 11-01 09:00 | 11-01 09:50 | 4 | 50 |
| u1-2 | 11-01 11:00 | 11-01 11:10 | 2 | 10 |
| u1-3 | 11-02 08:00 | 11-02 08:15 | 2 | 15 |
| u2-1 | 11-01 10:00 | 11-01 10:00 | 1 | 0 |
| u2-2 | 11-01 10:45 | 11-01 10:50 | 2 | 5 |
| u2-3 | 11-01 23:50 | 11-02 00:10 | 2 | 20 (crosses midnight) |
| u3-1 | 11-02 14:00 | 11-02 14:00 | 1 | 0 |
| u3-2 | 11-02 14:31 | 11-02 14:40 | 2 | 9 (31-min gap → new session) |

**Daily metrics**

| session_date | sessions | avg_session_minutes |
|---|---|---|
| 2026-11-01 | 5 | 17.0 |
| 2026-11-02 | 3 | 8.0 |

Overall average session length: 109 min / 8 sessions = **13.63 min**.
