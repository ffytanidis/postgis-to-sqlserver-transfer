from sqlalchemy import create_engine, text
import pyodbc, os
from shapely import wkb
from urllib.parse import quote_plus
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# --- Config ---
pg_config = {
    'username': 'analyst_ddl',
    'password':  quote_plus(os.getenv('PG_PASSWORD')),
    'host': 'maritime-assets-db1-dev-geospatial.cluster-cinsmmsxwkgg.eu-west-1.rds.amazonaws.com',
    'port': 5432,
    'database': 'maritime_assets'
}

sql_server_conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=192.168.100.130,1437;"
    "DATABASE=your_sql_db;"
    f"UID=kp_daan;PWD={os.getenv("SQL_SERVER_PASSWORD")}"
)

target_table = "sandboxs.target_table"
pg_query = """
SELECT id, name, ST_AsBinary(geom) AS geom
FROM your_schema.your_table
"""

# --- Connect to PostgreSQL with SQLAlchemy ---
pg_url = (
    f"postgresql+psycopg2://{pg_config['username']}:{pg_config['password']}@"
    f"{pg_config['host']}:{pg_config['port']}/{pg_config['database']}"
)

pg_engine = create_engine(pg_url)
with pg_engine.connect() as pg_conn:
    result = pg_conn.execute(text(pg_query))
    rows = result.fetchall()
    colnames = result.keys()

# --- Connect to SQL Server ---
sql_conn = pyodbc.connect(sql_server_conn_str)
sql_cur = sql_conn.cursor()

# --- Insert into SQL Server ---
insert_sql = f"""
INSERT INTO {target_table} (id, name, geom)
VALUES (?, ?, geometry::STGeomFromText(?, 4326))
"""

for row in rows:
    id_, name, geom_wkb = row
    geom_wkt = wkb.loads(geom_wkb).wkt
    sql_cur.execute(insert_sql, id_, name, geom_wkt)

sql_conn.commit()

# --- Cleanup ---
sql_cur.close()
sql_conn.close()

print("Transfer complete.")