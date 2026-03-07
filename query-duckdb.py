from pathlib import Path
import duckdb
from datetime import datetime

BASE_DIR = Path(__file__).parent
db_path = BASE_DIR / "brightspace.duckdb"
export_folder = BASE_DIR / "DataHub_Queries"
export_folder.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(str(db_path))

ts = datetime.now().strftime("%Y%m%d_%H%M%S")

# Separate multiple queries with commas
queries = {
    "assignments": """
        WITH a AS (
            SELECT
                dropboxid,
                orgunitid,
                submitterid,
                score,
                feedbackisread,
                feedbackreaddate,
                lastsubmissiondate,
                completiondate
            FROM assignment_submissions
            WHERE orgunitid IN (1234567, 1234568, 1234569)
        )
        SELECT
            o.orgunitid,
            o.code AS coursecode,
            o.name AS coursename,
            a.dropboxid,
            asum.name AS assignname,
            a.submitterid AS userid,
            a.score AS userpoints,
            asum.possiblescore AS maxpoints,
            a.feedbackisread,
            strftime('%m/%d/%Y %H:%M:%S', a.feedbackreaddate::TIMESTAMP) AS feedbackreaddate,
            strftime('%m/%d/%Y %H:%M:%S', a.lastsubmissiondate::TIMESTAMP) AS lastsubmissiondate,
            strftime('%m/%d/%Y %H:%M:%S', a.completiondate::TIMESTAMP) AS completiondate
        FROM a
        JOIN assignment_summary AS asum
            ON a.orgunitid = asum.orgunitid
            AND a.dropboxid = asum.dropboxid
        JOIN organizational_units AS o
            ON a.orgunitid = o.orgunitid
    """,
    "posts": """
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
            WHERE orgunitid = 1285363
        )
        SELECT
            o.orgunitid,
            o.code AS coursecode,
            o.name AS coursename,
            dt.name AS topicname,
            dp.thread,
            dp.postid,
            strftime('%m/%d/%Y %H:%M:%S', dp.dateposted::TIMESTAMP) AS dateposted,
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
