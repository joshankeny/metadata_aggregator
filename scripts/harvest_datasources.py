import os, json, csv, pathlib, yaml

ROOT = pathlib.Path("repos")
OUTDIR = pathlib.Path("data")
OUTDIR.mkdir(exist_ok=True, parents=True)

def normalize_source(src):
    # expected keys: name, system, type, url  (back-compat: uri)
    url = src.get("url", src.get("uri"))
    return {
        "name": (str(src.get("name","")).strip() or None),
        "system": (str(src.get("system","")).strip() or None),
        "type": (str(src.get("type","")).strip() or None),
        "url": (str(url).strip() if url is not None else None),
    }

def iter_projects():
    for repo_dir in sorted(p for p in ROOT.iterdir() if p.is_dir()):
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

def harvest_sources():
    rows, seen = [], set()
    for repo, proj, data in iter_projects():
        key = (proj.get("key") or repo)
        name = proj.get("name") or repo
        for src in (data.get("data_assets", {}) or {}).get("sources", []) or []:
            s = normalize_source(src)
            skey = (repo, s["name"], s["system"], s["type"], s["url"])
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
                "url": s["url"],
                "notes": ""
            })
    rows.sort(key=lambda r: (r["repo"], r["source_name"] or ""))
    return rows

def harvest_lineage():
    rows, seen = [], set()
    for repo, proj, data in iter_projects():
        key = (proj.get("key") or repo)
        name = proj.get("name") or repo
        edges = (data.get("lineage", {}) or {}).get("edges", []) or []
        for e in edges:
            src = (e.get("from") or "").strip()
            dst = (e.get("to") or "").strip()
            tool = (e.get("tool") or "").strip() or None
            freq = (e.get("frequency") or "").strip() or None
            desc = (e.get("description") or "").strip() or None
            if not src or not dst:
                continue
            ek = (repo, src, dst, tool, freq, desc)
            if ek in seen:
                continue
            seen.add(ek)
            rows.append({
                "repo": repo,
                "project_key": key,
                "project_name": name,
                "src": src,
                "dst": dst,
                "tool": tool,
                "frequency": freq,
                "description": desc,
                "notes": ""
            })
    rows.sort(key=lambda r: (r["repo"], r["src"], r["dst"]))
    return rows

def write_outputs(sources, edges):
    # datasources.csv/json
    csv_path = OUTDIR / "datasources.csv"
    src_cols = ["repo","project_key","project_name","discovered_via","source_name","system","type","url","notes"]
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=src_cols); w.writeheader()
        for r in sources: w.writerow({k: ("" if r.get(k) is None else r.get(k)) for k in src_cols})
    with open(OUTDIR / "datasources.json", "w", encoding="utf-8") as f:
        json.dump(sources, f, ensure_ascii=False, indent=2)

    # lineage.csv/json
    lin_path = OUTDIR / "lineage.csv"
    lin_cols = ["repo","project_key","project_name","src","dst","tool","frequency","description","notes"]
    with open(lin_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=lin_cols); w.writeheader()
        for r in edges: w.writerow({k: ("" if r.get(k) is None else r.get(k)) for k in lin_cols})
    with open(OUTDIR / "lineage.json", "w", encoding="utf-8") as f:
        json.dump(edges, f, ensure_ascii=False, indent=2)

    print(f"Wrote {csv_path} and {lin_path}")
    print(f"Totals → sources: {len(sources)}, edges: {len(edges)}")

if __name__ == "__main__":
    sources = harvest_sources()
    edges = harvest_lineage()
    write_outputs(sources, edges)
