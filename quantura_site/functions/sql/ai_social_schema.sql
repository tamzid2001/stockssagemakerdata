-- Quantura Social AI relational schema (PostgreSQL)
-- Purpose: internal lightweight follow/like tracking for AI Portfolio Agents.
-- Constraint: no external stream providers.

CREATE TABLE IF NOT EXISTS ai_agents (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  owner_user_id TEXT NOT NULL,
  name TEXT NOT NULL,
  description TEXT,
  strategy TEXT,
  holdings_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  returns_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  rationale TEXT,
  source_run_id TEXT,
  likes_count INTEGER NOT NULL DEFAULT 0,
  followers_count INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ai_agent_followers (
  agent_id TEXT NOT NULL REFERENCES ai_agents(id) ON DELETE CASCADE,
  follower_user_id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (agent_id, follower_user_id)
);

CREATE TABLE IF NOT EXISTS ai_agent_likes (
  agent_id TEXT NOT NULL REFERENCES ai_agents(id) ON DELETE CASCADE,
  liker_user_id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (agent_id, liker_user_id)
);

CREATE INDEX IF NOT EXISTS idx_ai_agents_workspace_updated ON ai_agents (workspace_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_ai_followers_workspace_user ON ai_agent_followers (workspace_id, follower_user_id);
CREATE INDEX IF NOT EXISTS idx_ai_likes_workspace_user ON ai_agent_likes (workspace_id, liker_user_id);
