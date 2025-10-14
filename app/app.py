import streamlit as st, pandas as pd, networkx as nx
from pyvis.network import Network
import tempfile, snowflake.connector, os


st.title("NR Data Lineage")
ctx = snowflake.connector.connect(
account=os.environ["SNOWFLAKE_ACCOUNT"],
user=os.environ["SNOWFLAKE_USER"],
password=os.environ["SNOWFLAKE_PASSWORD"],
role=os.environ["SNOWFLAKE_ROLE"],
warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
database=os.environ.get("SNOWFLAKE_DATABASE","METADATA"),
schema=os.environ.get("SNOWFLAKE_SCHEMA","LINEAGE"),
)


edges = pd.read_sql("SELECT SRC, DST FROM LINEAGE_EDGES", ctx)
G = nx.from_pandas_edgelist(edges, source="SRC", target="DST", create_using=nx.DiGraph())
net = Network(height="700px", directed=True)
net.from_nx(G)
html = net.generate_html()
st.components.v1.html(html, height=740, scrolling=True)