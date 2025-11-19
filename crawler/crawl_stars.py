#!/usr/bin/env python3
"""
crawl_stars.py

- Enumerates public repositories using GET /repositories?since=<id>
- For each repo, fetches GET /repos/{owner}/{repo} to get stargazers_count and metadata
- Uses a small ThreadPool for parallel repo detail fetches.
- Respects GitHub REST rate limits via headers:
   X-RateLimit-Limit, X-RateLimit-Remaining, X-RateLimit-Reset
- Uses exponential backoff + jitter for transient errors.
- Upserts repos and daily star samples into Postgres.
- Maintains a checkpoint (repos_since) in Postgres to resume across runs.
"""

import os
import sys
import time
import json
import random
from datetime import date, datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import psycopg2
from psycopg2.extras import execute_values
from tqdm import tqdm

# --- CONFIG ---
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")  # Provided by GitHub Actions automatically
PG_CONN = os.environ.get("PG_CONN")  # e.g. "host=localhost port=5432 dbname=crawl user=postgres password=postgres"
BATCH_PAGE_SIZE = 100        # REST /repositories per page (max 100)
DETAIL_WORKERS = 5          # concurrency for GET /repos/{owner}/{repo}
MAX_REPOS_TO_CRAWL = int(os.environ.get("MAX_REPOS", "100000"))  # allow overriding for testing
UPSERT_BATCH = 200          # rows to upsert per DB execute_values
SLEEP_ON_EMPTY = 5          # seconds if REST returns empty
RATE_LIMIT_THRESHOLD = 10   # remaining requests threshold to trigger sleep until reset

HEADERS = {
    "Accept": "application/vnd.github+json",
    "Authorization": f"Bearer {GITHUB_TOKEN}" if GITHUB_TOKEN else None
}
# remove None header if token missing
HEADERS = {k: v for k, v in HEADERS.items() if v is not None}

# --- HELPERS: backoff & rate handling ---

def backoff_sleep(attempt, base=1.0, cap=300.0):
    """
    Exponential backoff with jitter for retries.
    attempt: 0,1,2,...
    """
    sleep = min(cap, base * (2 ** attempt))
    # full jitter
    jitter = random.random() * sleep
    time.sleep(jitter)

def inspect_rate_limit_from_response(resp):
    """
    Reads REST rate limit headers; returns (remaining, reset_epoch)
    """
    try:
        rem = int(resp.headers.get("X-RateLimit-Remaining"))
        reset = int(resp.headers.get("X-RateLimit-Reset"))
        return rem, reset
    except Exception:
        return None, None

def sleep_until_reset(reset_epoch):
    now = int(time.time())
    delta = reset_epoch - now
    if delta <= 0:
        return
    # small buffer
    to_sleep = delta + 2
    print(f"[rate] sleeping for {to_sleep}s until reset (epoch {reset_epoch})")
    time.sleep(to_sleep)

# --- DB operations ---

def connect_pg():
    if not PG_CONN:
        raise RuntimeError("PG_CONN env var not set")
    return psycopg2.connect(PG_CONN)

def get_checkpoint(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT value FROM crawl_checkpoint WHERE name='repos_since'")
        r = cur.fetchone()
        return r[0] if r else "0"

def set_checkpoint(conn, value):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO crawl_checkpoint (name, value, updated_at) VALUES ('repos_since', %s, now()) "
            "ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at",
            (str(value),)
        )
    conn.commit()

def upsert_repos_and_stars(conn, repo_objs):
    """
    repo_objs: list of dicts containing fields from /repos endpoint including id, full_name, owner.login, stargazers_count, etc.
    Upsert into repos and repo_stars.
    """
    if not repo_objs:
        return
    today = date.today()
    repo_rows = []
    star_rows = []
    for r in repo_objs:
        repo_rows.append((
            int(r["id"]),
            r.get("node_id"),
            r.get("name"),
            r["owner"]["login"] if r.get("owner") else None,
            r.get("full_name"),
            r.get("html_url"),
            (r.get("language") or None),
            r.get("description"),
            r.get("created_at"),
            r.get("updated_at"),
            json.dumps(r)  # raw metadata
        ))
        star_rows.append((int(r["id"]), today, int(r.get("stargazers_count", 0)), "rest"))

    with conn.cursor() as cur:
        # upsert repos
        execute_values(cur,
            """
            INSERT INTO repos
              (id, node_id, name, owner_login, full_name, url, language, description, created_at, updated_at, metadata)
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET
              node_id = EXCLUDED.node_id,
              name = EXCLUDED.name,
              owner_login = EXCLUDED.owner_login,
              full_name = EXCLUDED.full_name,
              url = EXCLUDED.url,
              language = EXCLUDED.language,
              description = EXCLUDED.description,
              updated_at = EXCLUDED.updated_at,
              metadata = EXCLUDED.metadata,
              last_crawled_at = now()
            """,
            repo_rows,
            page_size=UPSERT_BATCH
        )

        # upsert daily star sample
        execute_values(cur,
            """
            INSERT INTO repo_stars (repo_id, sample_date, stargazers_count, source)
            VALUES %s
            ON CONFLICT (repo_id, sample_date) DO UPDATE SET
              stargazers_count = EXCLUDED.stargazers_count,
              source = EXCLUDED.source
            """,
            star_rows,
            page_size=UPSERT_BATCH
        )
    conn.commit()

# --- API helpers ---

def rest_get_with_retry(url, params=None, max_attempts=6):
    attempt = 0
    while True:
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=30)
            if r.status_code == 200:
                return r
            # handle rate-limiting (403 or specific headers)
            if r.status_code in (403, 429):
                rem, reset = inspect_rate_limit_from_response(r)
                if rem is not None and reset is not None:
                    if rem < RATE_LIMIT_THRESHOLD:
                        sleep_until_reset(reset)
                        continue
                # fallback backoff
            # treat 5xx as transient
            if 500 <= r.status_code < 600:
                if attempt >= max_attempts - 1:
                    r.raise_for_status()
                backoff_sleep(attempt)
                attempt += 1
                continue
            # other non-200 -> raise to surface errors (e.g. 404 for repo details)
            return r
        except requests.RequestException as e:
            if attempt >= max_attempts - 1:
                raise
            backoff_sleep(attempt)
            attempt += 1

def fetch_repo_details(owner, repo):
    """
    GET /repos/{owner}/{repo}
    Returns JSON dict or None if 404.
    """
    url = f"https://api.github.com/repos/{owner}/{repo}"
    r = rest_get_with_retry(url)
    if r.status_code == 200:
        # check rate limit header
        rem, reset = inspect_rate_limit_from_response(r)
        if rem is not None and rem < RATE_LIMIT_THRESHOLD:
            sleep_until_reset(reset)
        return r.json()
    elif r.status_code == 404:
        return None
    else:
        # raise for other errors
        r.raise_for_status()

# --- Main crawling loop ---

def enumerate_repos_since(since_id, max_repos):
    """
    Generator that yields page lists of repos from GET /repositories?since=<id>&per_page=100
    Ends when total yielded repos >= max_repos
    """
    url = "https://api.github.com/repositories"
    fetched = 0
    since = int(since_id)
    pbar = tqdm(total=max_repos, desc="Total repos")
    while fetched < max_repos:
        params = {"since": since, "per_page": BATCH_PAGE_SIZE}
        r = rest_get_with_retry(url, params=params)
        if r.status_code != 200:
            # if something odd happens, break
            print(f"[warn] expected 200 from /repositories, got {r.status_code}")
            break
        page = r.json()
        if not page:
            print("[info] empty page from /repositories; sleeping briefly")
            time.sleep(SLEEP_ON_EMPTY)
            continue
        yield page
        fetched += len(page)
        pbar.update(len(page))
        # last repo numeric id becomes new since
        since = page[-1]["id"]
    pbar.close()

def main():
    print("[start] GitHub star crawler")
    conn = connect_pg()
    try:
        since = get_checkpoint(conn)
        print(f"[checkpoint] starting from since={since}")
        total_processed = 0

        repo_pages = enumerate_repos_since(since, MAX_REPOS_TO_CRAWL)
        # We'll process each page, and fetch repo details in parallel per page
        for page in repo_pages:
            # Build tasks
            tasks = []
            with ThreadPoolExecutor(max_workers=DETAIL_WORKERS) as ex:
                futures = {ex.submit(fetch_repo_details, r["owner"]["login"], r["name"]): r for r in page}
                repo_objs = []
                for fut in as_completed(futures):
                    orig = futures[fut]
                    try:
                        detail = fut.result()
                    except Exception as e:
                        print(f"[error] failed fetching {orig['full_name']}: {e}")
                        continue
                    if detail:
                        repo_objs.append(detail)
                if repo_objs:
                    upsert_repos_and_stars(conn, repo_objs)
                    total_processed += len(repo_objs)
                    print(f"[progress] upserted {len(repo_objs)} repos; total {total_processed}")
            # update checkpoint using last id from page
            last_id = page[-1]["id"]
            set_checkpoint(conn, str(last_id))
            # stop if we've reached MAX_REPOS_TO_CRAWL
            if total_processed >= MAX_REPOS_TO_CRAWL:
                break

        print(f"[done] total processed {total_processed}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
