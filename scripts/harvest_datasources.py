import os, json, csv, pathlib, yaml

ROOT = pathlib.Path("repos")
OUTDIR = pathlib.Path("data")
OUTDIR.mkdir(exist_ok=True, parents=True)

def normalize_source(src):
    # expect keys like: name, type, system, uri
    return {
        "name": str(src.get("name","")).strip() or None,
        "system": str(src.get("system","")).strip() or None,
        "type": str(src.get("type","")).strip() or None,
        "uri": str(src.get("uri","")).strip() or None,
    }

def iter_projects():
    for repo_dir in sorted([p for p in ROOT.iterdir() if p.is_dir()]):
        y = repo_dir / "project.yaml"
        if not y.exists():
            continue
        try:
            data = yaml.safe_load(y.read_text(encoding="utf-8")) or {}
        except Exception as e:
            print(f"[WARN] YAML parse error in {repo_dir.name}: {e}")
            continue
        proj = data.get("project", {}) or {}
        yield repo_dir.name, proj, data

def harvest():
    rows = []
    seen = set()  # de-duplicate by (repo, source_key)
    for repo, proj, data in iter_projects():
        key = (proj.get("key") or repo)
        name = proj.get("name") or repo

        # 1) from data_assets.sources
        for src in (data.get("data_assets", {}) or {}).get("sources", []) or []:
            s = normalize_source(src)
            skey = (repo, ("data_assets", s["name"], s["system"], s["type"], s["uri"]))
            if skey in seen:
                continue
            seen.add(skey)
            rows.append({
                "repo": repo,
                "project_key": key,
                "project_name": name,
                "discovered_via": "data_assets",
                "source_name": s["name"],
                "system": s["system"],
                "type": s["type"],
                "uri": s["uri"],
                "notes": ""
            })

        # 2) from lineage.edges 'from' (use as sources if not already present)
        edges = (data.get("lineage", {}) or {}).get("edges", []) or []
        for e in edges:
            src_name = str(e.get("from","")).strip()
            if not src_name:
                continue
            skey = (repo, ("lineage", src_name))
            if skey in seen:
                continue
            # try to avoid duping if the exact name already appeared in data_assets
            has_same_name = any(r["repo"]==repo and r["source_name"]==src_name for r in rows)
            if has_same_name:
                continue
            seen.add(skey)
            rows.append({
                "repo": repo,
                "project_key": key,
                "project_name": name,
                "discovered_via": "lineage",
                "source_name": src_name,
                "system": None,
                "type": None,
                "uri": None,
                "notes": ""
            })

    # sort for stable diffs
    rows.sort(key=lambda r: (r["repo"], r["source_name"] or "", r["discovered_via"]))
    return rows

def write_outputs(rows):
    # CSV
    csv_path = OUTDIR / "datasources.csv"
    cols = ["repo","project_key","project_name","discovered_via","source_name","system","type","uri","notes"]
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({k: ("" if r[k] is None else r[k]) for k in cols})

    # JSON
    json_path = OUTDIR / "datasources.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

    print(f"Wrote {csv_path} and {json_path}")

if __name__ == "__main__":
    rows = harvest()
    write_outputs(rows)
    print(f"Total sources: {len(rows)}")
