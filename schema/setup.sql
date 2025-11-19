-- schema/setup.sql

CREATE TABLE IF NOT EXISTS repos (
  id BIGINT PRIMARY KEY,                 -- GitHub numeric repo id
  node_id TEXT,
  name TEXT,
  owner_login TEXT,
  full_name TEXT,
  url TEXT,
  language TEXT,
  description TEXT,
  created_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ,
  last_crawled_at TIMESTAMPTZ,
  metadata JSONB,
  inserted_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS repo_stars (
  repo_id BIGINT REFERENCES repos(id) ON DELETE CASCADE,
  sample_date DATE NOT NULL,
  stargazers_count INTEGER NOT NULL,
  source TEXT,
  PRIMARY KEY (repo_id, sample_date)
);

CREATE INDEX IF NOT EXISTS idx_repo_stars_sample_date ON repo_stars (sample_date);

-- checkpoint table to resume enumeration using `since` id
CREATE TABLE IF NOT EXISTS crawl_checkpoint (
  name TEXT PRIMARY KEY,
  value TEXT,
  updated_at TIMESTAMPTZ DEFAULT now()
);

-- initialize checkpoint if not present
INSERT INTO crawl_checkpoint (name, value) 
  SELECT 'repos_since', '0' WHERE NOT EXISTS (SELECT 1 FROM crawl_checkpoint WHERE name='repos_since');
