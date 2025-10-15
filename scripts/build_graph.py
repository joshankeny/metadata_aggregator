# scripts/build_graph.py
import csv, pathlib
import networkx as nx
from pyvis.network import Network
import matplotlib.pyplot as plt

LINEAGE = pathlib.Path("data/lineage.csv")
DOCS    = pathlib.Path("docs");  DOCS.mkdir(parents=True, exist_ok=True)
ASSETS  = pathlib.Path("assets"); ASSETS.mkdir(parents=True, exist_ok=True)

# 1) Build a directed graph from lineage.csv (src,dst)
G = nx.DiGraph()
if LINEAGE.exists():
    with LINEAGE.open(newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            src = (row.get("src") or "").strip()
            dst = (row.get("dst") or "").strip()
            if not src or not dst:
                continue
            # De-dup automatically handled by NetworkX; just add
            G.add_node(src, kind="Asset", label=src)
            G.add_node(dst, kind="Asset", label=dst)
            G.add_edge(src, dst)  # ignore tool/frequency/description on purpose
else:
    # still write a tiny placeholder page so Pages deploys
    (DOCS / "index.html").write_text(
        "<h1>No lineage.csv found</h1><p>Place a data/lineage.csv with src,dst headers.</p>",
        encoding="utf-8"
    )
    raise SystemExit("No data/lineage.csv; wrote placeholder index.html")

# 2) Interactive HTML (PyVis)
net = Network(height="800px", width="100%", directed=True, bgcolor="#ffffff")
net.from_nx(G)

# Uniform styling for assets
for n, data in G.nodes(data=True):
    node = net.get_node(n)
    node["shape"] = "dot"
    node["size"]  = 12

out_html = DOCS / "graph.html"
net.write_html(str(out_html), notebook=False, open_browser=False)

# Root redirect so https://<user>.github.io/<repo>/ works
(DOCS / "index.html").write_text(
    '<meta http-equiv="refresh" content="0; url=graph.html">', encoding="utf-8"
)

# 3) Static PNG (NetworkX)
plt.figure(figsize=(10, 8))
pos = nx.spring_layout(G, k=0.4, seed=42)
nx.draw_networkx_nodes(G, pos)
nx.draw_networkx_edges(G, pos, arrows=True, arrowstyle="->", arrowsize=10, width=1)
# label up to 50 nodes to avoid clutter
labels = {n: n for i, n in enumerate(G.nodes()) if i < 50}
nx.draw_networkx_labels(G, pos, labels=labels, font_size=8)
plt.axis("off"); plt.tight_layout()
plt.savefig(str(ASSETS / "graph.png"), dpi=150); plt.close()

print(f"Wrote {out_html} and assets/graph.png (nodes={G.number_of_nodes()}, edges={G.number_of_edges()})")
