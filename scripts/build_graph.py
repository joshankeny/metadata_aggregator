import csv, pathlib
from collections import defaultdict
import networkx as nx

# For interactive HTML
from pyvis.network import Network

# For static PNG
import matplotlib.pyplot as plt

ROOT = pathlib.Path(".")
DATASOURCES = ROOT / "data" / "datasources.csv"
LINEAGE     = ROOT / "data" / "lineage.csv"
DOCS_DIR    = ROOT / "docs"
ASSETS_DIR  = ROOT / "assets"
DOCS_DIR.mkdir(exist_ok=True, parents=True)
ASSETS_DIR.mkdir(exist_ok=True, parents=True)

G = nx.DiGraph()

# Load datasources -> create Asset nodes (and remember project names)
projects = {}           # key -> name
assets_by_project = defaultdict(set)

if DATASOURCES.exists():
    with DATASOURCES.open(newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            pkey = row.get("project_key") or row.get("repo")
            pname = row.get("project_name") or pkey
            projects[pkey] = pname
            src = row.get("source_name")
            if not src:
                continue
            G.add_node(src, label=src, kind="Asset", system=row.get("system"), type=row.get("type"))
            assets_by_project[pkey].add(src)

# Load lineage -> FEEDS edges between assets
if LINEAGE.exists():
    with LINEAGE.open(newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            src = (row.get("src") or "").strip()
            dst = (row.get("dst") or "").strip()
            if not src or not dst:
                continue
            G.add_node(src, label=src, kind="Asset")
            G.add_node(dst, label=dst, kind="Asset")
            G.add_edge(src, dst, tool=row.get("tool"), frequency=row.get("frequency"), description=row.get("description"))
            pkey = row.get("project_key") or row.get("repo")
            if pkey:
                assets_by_project[pkey].update([src, dst])

# Optionally create Project nodes and connect to Assets (helps navigation)
for pkey, pname in projects.items():
    pnode = f"Project::{pkey}"
    G.add_node(pnode, label=pname, kind="Project")
    for a in assets_by_project.get(pkey, []):
        G.add_edge(pnode, a, rel="USES")

# --------- Build interactive HTML (PyVis) ----------
net = Network(height="800px", width="100%", directed=True, bgcolor="#ffffff")
net.from_nx(G)

# Simple styling: Projects as squares, Assets as dots
for n, data in G.nodes(data=True):
    if data.get("kind") == "Project":
        node = net.get_node(n)
        node["shape"] = "box"
        node["color"] = {"border": "#333333", "background": "#E6F0FF"}
    else:
        node = net.get_node(n)
        node["shape"] = "dot"
        node["size"] = 12

net.write_html(str(DOCS_DIR / "graph.html"), notebook=False, open_browser=False)

# --------- Build static PNG (NetworkX) ----------
plt.figure(figsize=(10, 8))
pos = nx.spring_layout(G, k=0.4, seed=42)
proj_nodes = [n for n, d in G.nodes(data=True) if d.get("kind") == "Project"]
asset_nodes = [n for n in G if n not in proj_nodes]

nx.draw_networkx_nodes(G, pos, nodelist=asset_nodes)
nx.draw_networkx_nodes(G, pos, nodelist=proj_nodes, node_shape="s")
nx.draw_networkx_edges(G, pos, arrows=True, arrowstyle="->", arrowsize=10, width=1)
# label a subset to avoid clutter
labels = {n: G.nodes[n].get("label", n) for n in list(G.nodes)[:50]}
nx.draw_networkx_labels(G, pos, labels=labels, font_size=8)

plt.axis("off")
plt.tight_layout()
plt.savefig(str(ASSETS_DIR / "graph.png"), dpi=150)
plt.close()

print("Wrote docs/graph.html and assets/graph.png")
