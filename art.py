import streamlit as st
import requests
import pandas as pd
import time
from sqlalchemy import create_engine, text

# -------------------- HARVARD API SETTINGS -------------------- 
API_KEY = "56bf439b-dbbc-4535-bfdf-39617a16d185"

engine = create_engine(
    "mysql+pymysql://root:Shalu0.34@localhost:3306/harvard_db"
)

# -------------------- STREAMLIT SETUP --------------------
st.set_page_config(page_title="Harvard Artifacts", layout="wide")
st.title("🏛️ Harvard’s Artifacts Collection")
st.write("Collect, store, and analyze Harvard Art Museums data")

# -------------------- DATA FETCH FUNCTION --------------------
def fetch_artifacts(classification, limit=2500, page_size=100):
    records = []
    page = 1

    while len(records) < limit:
        url = "https://api.harvardartmuseums.org/object"
        params = {
            "apikey": API_KEY,
            "classification": classification,
            "page": page,
            "size": page_size
        }

        response = requests.get(url, params=params).json()
        data = response.get("records", [])

        if not data:
            break

        records.extend(data)
        page += 1
        time.sleep(0.01)

    return records[:limit]

# -------------------- TRANSFORM FUNCTION --------------------
def transform_data(records):
    metadata, media, colors = [], [], []

    for r in records:
        metadata.append({
            "id": r.get("id"),
            "title": r.get("title"),
            "culture": r.get("culture"),
            "period": r.get("period"),
            "century": r.get("century"),
            "medium": r.get("medium"),
            "dimensions": r.get("dimensions"),
            "description": r.get("description"),
            "department": r.get("department"),
            "classification": r.get("classification"),
            "accessionyear": r.get("accessionyear"),
            "accessionmethod": r.get("accessionmethod")
        })

        media.append({
            "objectid": r.get("id"),
            "imagecount": r.get("imagecount"),
            "mediacount": r.get("mediacount"),
            "colorcount": r.get("colorcount"),
            "media_rank": r.get("rank"),
            "datebegin": r.get("datebegin"),
            "dateend": r.get("dateend")
        })

        for c in r.get("colors", []) or []:
            colors.append({
                "objectid": r.get("id"),
                "color": c.get("color"),
                "spectrum": c.get("spectrum"),
                "hue": c.get("hue"),
                "percent": c.get("percent"),
                "css3": c.get("css3")
            })

    return metadata, media, colors

 # -------------------- LOAD EXISTING IDS FROM DB --------------------
    
def insert_to_sql(meta, media, colors):
   
    existing_ids = pd.read_sql(
        "SELECT id FROM artifact_metadata",
        engine
    )["id"].astype(int).tolist()

    # -------------------- METADATA --------------------
    df_meta = pd.DataFrame(meta)
    df_meta["id"] = df_meta["id"].astype(int)

    # keep only NEW records
    df_meta_new = df_meta[~df_meta["id"].isin(existing_ids)]

    if df_meta_new.empty:
        return

    df_meta_new.to_sql(
        "artifact_metadata",
        engine,
        if_exists="append",
        index=False
    )

    # -------------------- MEDIA --------------------
    df_media = pd.DataFrame(media)
    df_media["objectid"] = df_media["objectid"].astype(int)

    df_media_new = df_media[df_media["objectid"].isin(df_meta_new["id"])]

    if not df_media_new.empty:
        df_media_new.to_sql(
            "artifact_media",
            engine,
            if_exists="append",
            index=False
        )

    # -------------------- COLORS --------------------
    df_colors = pd.DataFrame(colors)
    df_colors["objectid"] = df_colors["objectid"].astype(int)

    df_colors_new = df_colors[df_colors["objectid"].isin(df_meta_new["id"])]

    if not df_colors_new.empty:
        df_colors_new.to_sql(
            "artifact_colors",
            engine,
            if_exists="append",
            index=False
        )

   
# -------------------- DATA COLLECTION UI --------------------
st.subheader("📥 Data Collection")

classification = st.selectbox(
    "Select Classification",
    ["Paintings", "Sculpture", "Coins", "Drawings", "Jewelry"]
)

if "fetched_data" not in st.session_state:
    st.session_state.fetched_data = None

record_limit = st.selectbox(
    "Select number of records to fetch",
    [100,2500],
    index=0
)

if st.button("Collect Data"):
    with st.spinner("Fetching data from API..."):
        st.session_state.fetched_data = fetch_artifacts(
            classification,
            limit=record_limit
        )
    st.success(f"Collected {len(st.session_state.fetched_data)} records")


if st.button("Show Data"):
    if st.session_state.fetched_data:

        meta, media, colors = transform_data(st.session_state.fetched_data)

        st.subheader("📘 Artifact Metadata")
        st.dataframe(pd.DataFrame(meta), use_container_width=True)

        st.subheader("🖼 Artifact Media")
        st.dataframe(pd.DataFrame(media), use_container_width=True)

        st.subheader("🎨 Artifact Colors")
        st.dataframe(pd.DataFrame(colors), use_container_width=True)

    else:
        st.warning("No data collected yet")


if st.button("Insert into SQL"):
    if st.session_state.fetched_data:
        meta, media, colors = transform_data(st.session_state.fetched_data)
        insert_to_sql(meta, media, colors)
        st.success("Data inserted into SQL successfully")
    else:
       st.warning("Collect data before inserting")

# -------------------- SQL QUERY SECTION --------------------
st.subheader("🔍 SQL Analysis")
queries = {
      # -------- artifact_metadata --------
    "1. 11th Century Byzantine Artifacts": """
        SELECT *
        FROM artifact_metadata
        WHERE century = '11th century'
          AND culture = 'Byzantine'
    """,

    "2. Unique Cultures": """
        SELECT DISTINCT culture
        FROM artifact_metadata
        WHERE culture IS NOT NULL
    """,

    "3. Artifacts from Archaic Period": """
        SELECT *
        FROM artifact_metadata
        WHERE period = 'Archaic Period'
    """,

    "4. Titles Ordered by Accession Year": """
        SELECT title, accessionyear
        FROM artifact_metadata
        ORDER BY accessionyear DESC
    """,

    "5. Artifacts per Department": """
        SELECT department, COUNT(*) AS total
        FROM artifact_metadata
        GROUP BY department
    """,

    # -------- artifact_media --------
    "6. Artifacts with More Than 1 Image": """
        SELECT *
        FROM artifact_media
        WHERE imagecount > 1
    """,

    "7. Average Media Rank": """
        SELECT AVG(media_rank) AS avg_rank
        FROM artifact_media
    """,

    "8. Colorcount > Mediacount": """
        SELECT *
        FROM artifact_media
        WHERE colorcount > mediacount
    """,

    "9. Artifacts Created Between 1500 and 1600": """
        SELECT *
        FROM artifact_media
        WHERE datebegin <= 1600
        AND dateend >= 1500

    """,

    "10. Artifacts with No Media Files": """
        SELECT *
        FROM artifact_media
        WHERE mediacount = 0
    """,

    # -------- artifact_colors --------
    "11. Distinct Hues": """
        SELECT DISTINCT hue
        FROM artifact_colors
        WHERE hue IS NOT NULL
    """,

    "12. Top 5 Most Used Colors": """
        SELECT color, COUNT(*) AS frequency
        FROM artifact_colors
        GROUP BY color
        ORDER BY frequency DESC
        LIMIT 5
    """,

    "13. Average Coverage Percentage per Hue": """
        SELECT hue, AVG(percent) AS avg_percent
        FROM artifact_colors
        GROUP BY hue
    """,

    "14. Colors for a Given Artifact ID (Example)": """
        SELECT *
        FROM artifact_colors
        WHERE objectid = 299843
    """,

    "15. Total Color Records": """
        SELECT COUNT(*) AS total_colors
        FROM artifact_colors
    """,

    # -------- JOIN QUERIES --------
    "16. Byzantine Artifacts with Hues": """
        SELECT m.title, c.hue
        FROM artifact_metadata m
        JOIN artifact_colors c
          ON m.id = c.objectid
        WHERE m.culture = 'Byzantine'
    """,

    "17. Artifact Titles with Their Hues": """
        SELECT m.title, c.hue
        FROM artifact_metadata m
        JOIN artifact_colors c
          ON m.id = c.objectid
    """,

    "18. Artifacts with Period Not NULL": """
        SELECT m.title, m.culture, a.media_rank
        FROM artifact_metadata m
        JOIN artifact_media a
          ON m.id = a.objectid
        WHERE m.period IS NOT NULL
    """,

    "19. Top 10 Grey-Hued Artifacts": """
        SELECT DISTINCT m.title
        FROM artifact_metadata m
        JOIN artifact_colors c
          ON m.id = c.objectid
        WHERE c.hue = 'Grey'
        LIMIT 10
    """,

    "20. Artifacts per Classification & Avg Media Count": """
        SELECT
            m.classification,
            COUNT(*) AS total_artifacts,
            AVG(a.mediacount) AS avg_mediacount
        FROM artifact_metadata m
        JOIN artifact_media a
          ON m.id = a.objectid
        GROUP BY m.classification
    """,
      # --- Own SQL Queries for Deeper Insights ---
    "21.Most Common Mediums Used Across Artifacts":"""
            SELECT medium, COUNT(*) AS count
            FROM artifact_metadata
            GROUP BY medium
            ORDER BY count DESC
            LIMIT 10
     """,
    
    "22.Artifacts with Missing Descriptions": """
            SELECT id, title, culture, classification
            FROM artifact_metadata
            WHERE description IS NULL OR description = ''
    """,
    "23.Average Accession Year by Classification": """
            SELECT classification, AVG(accessionyear) AS avg_year
            FROM artifact_metadata
            GROUP BY classification
            ORDER BY avg_year DESC
    """,
    "24.Artifacts with Longest Time Span Between Creation Dates": """
            SELECT objectid, datebegin, dateend, (dateend - datebegin) AS duration
            FROM artifact_media
            ORDER BY duration DESC
            LIMIT 10
    """,
    "25.Top 5 Departments by Artifact Count": """
            SELECT department, COUNT(*) AS total
            FROM artifact_metadata
            GROUP BY department
            ORDER BY total DESC
            LIMIT 5
    """,
    "26.Most Frequently Used Hue Across All Artifacts": """
            SELECT hue, COUNT(*) AS frequency
            FROM artifact_colors
            GROUP BY hue
            ORDER BY frequency DESC
            LIMIT 1
    """  
}
# Select query
selected_query = st.selectbox("Choose Query", list(queries.keys()), key="choose_query_main")

# Show query
st.subheader("📄 SQL Query")
st.code(queries[selected_query], language="sql")

# Run query
if st.button("Run Query"):
    try:
        df = pd.read_sql(sql=text(queries[selected_query]), con=engine)
        st.success("Query Executed Successfully!")
        st.dataframe(df)
    except Exception as e:
        st.error(f"Error executing query: {e}")


# -------------------- FOOTER --------------------
st.markdown("---")
st.caption("Harvard Art Museums API | Streamlit + MySQL")

