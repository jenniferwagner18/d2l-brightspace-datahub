from pathlib import Path
import duckdb
from datetime import datetime

BASE_DIR = Path(__file__).parent
db_path = BASE_DIR / "brightspace.duckdb"
export_folder = BASE_DIR / "DataHub_Joins"
export_folder.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(str(db_path))

ts = datetime.now().strftime("%Y%m%d_%H%M%S")

# Separate multiple queries with commas
queries = {
    "all_posts": """
        WITH dp AS (
            SELECT 
                orgunitid,
                topicid,
                thread, 
                postid,
                dateposted, 
                isreply, 
                score,
                userid
            FROM discussion_posts
            WHERE orgunitid = 1234567
        )
        SELECT
            o.orgunitid,
            o.code AS coursecode,
            dt.name AS topicname,
            dp.thread,
            dp.postid,
            dp.dateposted,
            dp.isreply,
            dp.score,
            dp.userid
        FROM dp
        JOIN discussion_topics AS dt
            ON dp.orgunitid = dt.orgunitid
            AND dp.topicid = dt.topicid
        JOIN organizational_units AS o
            ON dp.orgunitid = o.orgunitid
    """
}

# Loop through queries, add timestamp to filename, and export to CSV
for name, sql in queries.items():
    filename = f"{name}_{ts}.csv"
    export_path = export_folder / filename
    export_sql = f"""
        COPY ({sql})
        TO '{export_path}' WITH (HEADER, DELIMITER ',')
    """
    con.execute(export_sql)
    print(f"Exported {export_path}")
