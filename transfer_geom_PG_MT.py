from sqlalchemy import create_engine, text
import pyodbc, os
from shapely import wkb
from urllib.parse import quote_plus
from dotenv import load_dotenv
import geopandas as gpd

# Load environment variables from .env
load_dotenv()

# --- Config ---
pg_config = {
    'username': 'analyst_ddl',
    'password': quote_plus(os.getenv('PG_PASSWORD')),
    'host': 'maritime-assets-db1-dev-geospatial.cluster-cinsmmsxwkgg.eu-west-1.rds.amazonaws.com',
    'port': 5432,
    'database': 'maritime_assets'
}

sql_server_conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=192.168.100.130,1437;"
    "DATABASE=ais;"
    f"UID=kp_daan;PWD={os.getenv('SQL_SERVER_PASSWORD')}"
)

# --- Create PostgreSQL engine ---
pg_url = (
    f"postgresql+psycopg2://{pg_config['username']}:{pg_config['password']}@"
    f"{pg_config['host']}:{pg_config['port']}/{pg_config['database']}"
)
pg_engine = create_engine(pg_url)

# --- Connect to SQL Server ---
sql_conn = pyodbc.connect(sql_server_conn_str)
sql_cur = sql_conn.cursor()

# --- Helper Function to Upload a GeoDataFrame ---
def upload_gdf_to_sqlserver(gdf, mapping_fields, target_table):
    """
    Upload a GeoDataFrame into SQL Server, mapping fields correctly.
    """
    insert_sql = f"""
    INSERT INTO {target_table} ({', '.join(mapping_fields.values())})
    VALUES ({', '.join(['?'] * len(mapping_fields))})
    """
    try:
        for idx, row in gdf.iterrows():
            values = []
            for source_field, target_field in mapping_fields.items():
                value = row[source_field]
                # Convert geometry to WKT
                if isinstance(value, (wkb.BaseGeometry, object)) and source_field.lower().endswith('geom'):
                    value = value.wkt if value else None
                values.append(value)
            sql_cur.execute(insert_sql, *values)

        sql_conn.commit()
        print(f"Uploaded {len(gdf)} records to {target_table}")

    except Exception as e:
        print(f"Error uploading to {target_table}: {str(e)}")
        sql_conn.rollback()

# --- Function to Update Ports ---
def update_ports():
    print("Starting to update PORTS...")
    #sql query to get Ports
    pg_Port_query = """
    SELECT 
        zone_id,
        mt_id,
        stndrd_zone_name AS name,
        zone_sub_type AS zone_type,
        unlocode,
        country_code,
        timezone_id,
        dst_id,
        enable_calls,
        confirmed,
        "CENTERX",
        "CENTERY",
        alternative_names,
        alternative_unlocodes,
        related_zone_anch_id,
        related_zone_port_id,
        polygon_geom
    FROM sandbox.mview_master_ports
    """

    try:
        gdf = gpd.read_postgis(
            sql=pg_Port_query,
            con=pg_engine,
            geom_col='polygon_geom'
        )
        print(f"Ports GeoDataFrame loaded, {len(gdf)} records.")

        # Optional: set CRS if missing
        if gdf.crs is None:
            gdf.set_crs(epsg=4326, inplace=True)

        # Define mapping: source field -> target SQL Server field
        port_mapping_fields = {
            'zone_id': 'ZONE_ID',
            'mt_id': 'PORT_ID',
            'name': 'PORT_NAME',
            'zone_type': 'PORT_TYPE',
            'unlocode': 'UNLOCODE',
            'country_code': 'COUNTRY_CODE',
            'timezone_id': 'TIMEZONE',
            'dst_id': 'DST',
            'enable_calls': 'ENABLE_CALLS',
            'confirmed': 'CONFIRMED',
            "CENTERX": 'CENTERX',
            "CENTERY": 'CENTERY',
            'alternative_names': 'ALTERNATIVE_NAMES',
            'alternative_unlocodes': 'ALTERNATIVE_UNLOCODES',
            'related_zone_anch_id': 'RELATED_ANCH_ID',
            'related_zone_port_id': 'RELATED_PORT_ID',
            'polygon_geom': 'POLYGON'
        }

        upload_gdf_to_sqlserver(gdf, port_mapping_fields, target_table="dbo.PORTS")

    except Exception as e:
        print(f"Error in update_ports(): {str(e)}")


# --- Function to Update Berths ---
def update_berths():
    print("Starting to update BERTHS...")

    pg_Berth_query = """
    SELECT 
        zone_id,
        stndrd_zone_name AS name,
        zone_sub_type AS berth_type,
        related_zone_port_id,
        related_zone_term_id,
        "CENTERX",
        "CENTERY",
        polygon_geom
    FROM sandbox.mview_master_berths
    """

    try:
        gdf = gpd.read_postgis(
            sql=pg_Berth_query,
            con=pg_engine,
            geom_col='polygon_geom'
        )
        print(f"Berths GeoDataFrame loaded, {len(gdf)} records.")

        if gdf.crs is None:
            gdf.set_crs(epsg=4326, inplace=True)

        berth_mapping_fields = {
            'zone_id': 'BERTH_ID',
            'name': 'BERTH_NAME',
            'berth_type': 'BERTH_TYPE',
            'related_zone_port_id': 'PORT_ID',
            'related_zone_term_id': 'TERMINAL_ID',
            "CENTERX": 'CENTERX',
            "CENTERY": 'CENTERY',
            'polygon_geom': 'POLYGON'
        }

        upload_gdf_to_sqlserver(gdf, berth_mapping_fields, target_table="dbo.BERTHS")

    except Exception as e:
        print(f"Error in update_berths(): {str(e)}")


# --- Function to Update Terminals ---
def update_terminals():
    print("Starting to update TERMINALS...")

    pg_Terminal_query = """
    SELECT 
        zone_id,
        stndrd_zone_name AS name,
        related_zone_port_id,
        "CENTERX",
        "CENTERY",
        polygon_geom
    FROM sandbox.mview_master_terminals
    """

    try:
        gdf = gpd.read_postgis(
            sql=pg_Terminal_query,
            con=pg_engine,
            geom_col='polygon_geom'
        )
        print(f"Terminals GeoDataFrame loaded, {len(gdf)} records.")

        if gdf.crs is None:
            gdf.set_crs(epsg=4326, inplace=True)

        terminal_mapping_fields = {
            'zone_id': 'TERMINAL_ID',
            'name': 'TERMINAL_NAME',
            'related_zone_port_id': 'PORT_ID',
            "CENTERX": 'CENTERX',
            "CENTERY": 'CENTERY',
            'polygon_geom': 'POLYGON'
        }

        upload_gdf_to_sqlserver(gdf, terminal_mapping_fields, target_table="dbo.TERMINALS")

    except Exception as e:
        print(f"Error in update_terminals(): {str(e)}")

# --- MAIN ---
if __name__ == "__main__":
    try:
        update_ports()
        #update_berths()
        #update_terminals()
    finally:
        sql_cur.close()
        sql_conn.close()
        print("SQL Server connection closed.")