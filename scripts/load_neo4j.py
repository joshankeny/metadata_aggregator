# scripts/load_neo4j.py
import csv, os, sys
from neo4j import GraphDatabase

# --- Config from env ---
NEO4J_URI  = os.getenv("NEO4J_URI",  "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASS = os.getenv("NEO4J_PASS", "neo4jPW")

DATASOURCES = os.getenv("DATASOURCES_PATH", "data/datasources.csv")
LINEAGE     = os.getenv("LINEAGE_PATH",     "data/lineage.csv")

# --- Neo4j driver ---
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))

def ensure_constraints(tx):
    tx.run("CREATE CONSTRAINT asset_name IF NOT EXISTS FOR (a:Asset) REQUIRE a.name IS UNIQUE")
    tx.run("CREATE CONSTRAINT project_key IF NOT EXISTS FOR (p:Project) REQUIRE p.key IS UNIQUE")

def load_datasources(tx):
    try:
        f = open(DATASOURCES, newline="", encoding="utf-8")
    except FileNotFoundError:
        print(f"[WARN] {DATASOURCES} not found; skipping datasources")
        return
    with f:
        r = csv.DictReader(f)
        for row in r:
            repo = row.get("repo")
            key  = row.get("project_key") or repo
            name = row.get("project_name") or repo
            src  = row.get("source_name")
            if not src:
                continue
            system = row.get("system") or None
            typ    = row.get("type") or None
            url    = row.get("url") or None

            # Project
            tx.run("""
                MERGE (p:Project {key:$key})
                SET p.name = COALESCE($name, p.name),
                    p.repo = COALESCE($repo, p.repo)
            """, key=key, name=name, repo=repo)

            # Asset
            tx.run("""
                MERGE (a:Asset {name:$src})
                SET a.system = COALESCE($system, a.system),
                    a.type   = COALESCE($typ, a.type),
                    a.url    = COALESCE($url, a.url)
            """, src=src, system=system, typ=typ, url=url)

            # Link
            tx.run("""
                MATCH (p:Project {key:$key}), (a:Asset {name:$src})
                MERGE (p)-[:USES]->(a)
            """, key=key, src=src)

def load_lineage(tx):
    try:
        f = open(LINEAGE, newline="", encoding="utf-8")
    except FileNotFoundError:
        print(f"[WARN] {LINEAGE} not found; skipping lineage")
        return
    with f:
        r = csv.DictReader(f)
        for row in r:
            src = (row.get("src") or "").strip()
            dst = (row.get("dst") or "").strip()
            if not src or not dst:
                continue
            tool = (row.get("tool") or None)
            freq = (row.get("frequency") or None)
            desc = (row.get("description") or None)
            key  = row.get("project_key") or row.get("repo")

            # Ensure nodes exist
            tx.run("MERGE (a:Asset {name:$n})", n=src)
            tx.run("MERGE (b:Asset {name:$n})", n=dst)

            # Edge with properties (idempotent)
            tx.run("""
                MATCH (a:Asset {name:$src}), (b:Asset {name:$dst})
                MERGE (a)-[r:FEEDS]->(b)
                SET r.tool=$tool, r.frequency=$freq, r.description=$desc
            """, src=src, dst=dst, tool=tool, freq=freq, desc=desc)

            # Optional: tie assets to project context
            if key:
                tx.run("""
                    MERGE (p:Project {key:$key})
                    WITH p
                    MATCH (a:Asset {name:$src}), (b:Asset {name:$dst})
                    MERGE (p)-[:CONTAINS]->(a)
                    MERGE (p)-[:CONTAINS]->(b)
                """, key=key, src=src, dst=dst)

def main():
    with driver.session() as s:
        s.execute_write(ensure_constraints)
        s.execute_write(load_datasources)
        s.execute_write(load_lineage)
    print("Loaded datasources & lineage into Neo4j.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
