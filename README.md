## Scaling from 100,000 to 500 Million Repositories

If this crawler were scaled from 100k → 500M repositories, the following changes would be required:

### 1. Distributed Crawling
- The crawler would be split into multiple workers.
- Each worker handles a portion of the cursor pages.
- Use a distributed queue (Kafka / SQS / Redis Streams).

### 2. Sharded Storage
Postgres alone cannot store 500M repos efficiently. Options:
- Postgres with table partitioning (range/hash partitioning)
- ClickHouse for analytical queries (recommended)
- BigQuery or Snowflake for large-scale analytics

### 3. Incremental / Delta Crawling
Instead of re-downloading everything daily:
- Only update `star_count`, `fork_count`, etc.
- Use GitHub `updatedAt` to fetch only mutated repos.
- Store daily deltas in a `repo_stats_history` table.

### 4. Strong Caching & Checkpointing
- Store a checkpoint cursor in DB after each page.
- If crawler restarts, continue from last checkpoint.
- Avoid re-fetching pages already crawled.

### 5. Highly Parallel GraphQL Queries
- 10–50 parallel GraphQL queries while respecting rate limits.
- Backoff + retry when hitting secondary rate limits.

### 6. Compressed Storage
- Store repo metadata in columnar format (e.g. Parquet).
- Only store the fields needed for analytics.

### 7. API Cost Optimization
Fetching issues/PRs/comments for 500M repos is impossible directly:
- Fetch only repos above a selected popularity threshold.
- Use sampling or event-based refresh (webhooks).

## Schema Evolution for More Metadata

The DB schema is designed to separate immutable and mutable fields.
This allows efficient updates even when metadata changes every day.

### 1. Immutable Tables (never change)
These store data that will never change after a repo is created.

- `repos`
    - id (PK)
    - name_with_owner
    - created_at

- `pull_requests`
    - id (PK)
    - repo_id (FK)
    - author
    - title
    - created_at

### 2. Mutable Tables (updated daily)
These track stats that change frequently.

- `repo_stats`
    - repo_id (FK)
    - stars
    - forks
    - watchers
    - open_issues
    - fetched_at (PK)

- `pr_stats`
    - pr_id (FK)
    - comments_count
    - reviews_count
    - commits_count
    - fetched_at (PK)

### Why this works
- New comments on PRs → only `pr_stats` row is added.
- Stars change → only `repo_stats` is updated.
- Core repo rows never change → minimal row updates.

### Future Metadata Supported
- CI checks → `ci_run_stats`
- Issue comments → `issue_comment_stats`
- PR reviews → `review_stats`
- Commit metadata → `commit_stats`
function graphqlQuery(query) {
    return fetch("https://api.github.com/graphql", {
        method: "POST",
        headers: {
            "Authorization": `Bearer ${process.env.GITHUB_TOKEN}`,
            "Content-Type": "application/json"
        },
        body: JSON.stringify({ query })
    })
    .then(async res => {
        const remaining = res.headers.get("X-RateLimit-Remaining");
        const reset = res.headers.get("X-RateLimit-Reset");

        // If rate limit is hit
        if (remaining === "0") {
            const wait = (parseInt(reset) * 1000) - Date.now();
            console.log("Rate limit hit. Waiting", wait / 1000, "seconds...");
            await new Promise(resolve => setTimeout(resolve, wait));
        }

        return res.json();
    });
}
## Clean Architecture / Code Structure

The project follows clean architecture principles:

### Domain Layer (pure logic)
- Entities: Repository, RepoStats
- No dependencies

### Use Case Layer
- crawlRepos() – business logic for crawling and pagination
- saveRepoStats() – storing stats efficiently

### Infrastructure Layer
- GitHub GraphQL client (anti-corruption layer)
- Database adapter (Postgres client)
- Config/environment loader

### CLI Layer
- Entry point to run crawler
## GitHub Actions Pipeline Requirements

- Uses Postgres service container
- Initializes schema via SQL script
- Runs the crawler using GitHub’s default GITHUB_TOKEN
- Stores results in Postgres
- Exports results as artifact (CSV or JSON)
- Contains at least one successful run
