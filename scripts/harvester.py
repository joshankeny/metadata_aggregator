import os, json, pathlib, yaml
cs.execute("""
CREATE TABLE IF NOT EXISTS LINEAGE_EDGES(
REPO STRING, SRC STRING, DST STRING, TOOL STRING, FREQ STRING, DESC STRING
);
""")




def upsert(projects):
ctx = snowflake_conn()
try:
cs = ctx.cursor()
ensure_tables(cs)
for repo, d in projects:
pr = d.get("project", {})
payload = {"repo": repo, **pr, "tags": pr.get("tags", [])}
cs.execute(
"""
MERGE INTO PROJECTS t USING (SELECT PARSE_JSON(%s) v) s
ON t.REPO = s.v:repo
WHEN MATCHED THEN UPDATE SET KEY=s.v:key, NAME=s.v:name, OWNER=s.v:owner, STATUS=s.v:status,
DOMAIN=s.v:domain, TAGS=s.v:tags, RAW_VARIANT=s.v
WHEN NOT MATCHED THEN INSERT (REPO, KEY, NAME, OWNER, STATUS, DOMAIN, TAGS, RAW_VARIANT)
VALUES (s.v:repo, s.v:key, s.v:name, s.v:owner, s.v:status, s.v:domain, s.v:tags, s.v);
""",
(json.dumps(payload),),
)
# wipe old connections/edges for this repo to avoid dupes
cs.execute("DELETE FROM CONNECTIONS WHERE REPO=%s", (repo,))
cs.execute("DELETE FROM LINEAGE_EDGES WHERE REPO=%s", (repo,))
# connections
stack = d.get("stack", {})
for tool, cfg in stack.items():
if not isinstance(cfg, dict):
continue
for k, v in cfg.items():
if k == "used":
continue
if isinstance(v, (str, int, float)):
cs.execute(
"INSERT INTO CONNECTIONS(REPO,TOOL,PROP,VALUE) VALUES(%s,%s,%s,%s)",
(repo, tool, k, str(v)),
)
# edges
for e in d.get("lineage", {}).get("edges", []):
cs.execute(
"""
INSERT INTO LINEAGE_EDGES(REPO,SRC,DST,TOOL,FREQ,DESC)
VALUES(%s,%s,%s,%s,%s,%s)
""",
(
repo,
e.get("from"),
e.get("to"),
e.get("tool", ""),
e.get("frequency", ""),
e.get("description", ""),
),
)
finally:
ctx.close()




if __name__ == "__main__":
projects = list(iter_projects())
upsert(projects)
print(f"Harvested {len(projects)} repos.")