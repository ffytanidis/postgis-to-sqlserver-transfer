#pip install psycopg2-binary SQLAlchemy python-dotenv --index-url https://pypi.org/simple
from numpy.core.defchararray import startswith
from sqlalchemy import create_engine, text
import pyodbc, os
from shapely.geometry.base import BaseGeometry
from urllib.parse import quote_plus
from dotenv import load_dotenv
import geopandas as gpd
import traceback, logging
from shapely.geometry import Polygon, MultiPolygon
from shapely.geometry.polygon import orient

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

sql_server_conn_str = ("DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=192.168.100.130,1437;"
    "DATABASE=ais;"
    f"UID=kp_daan;PWD={os.getenv('SQL_SERVER_PASSWORD')}")

# --- Create PostgreSQL engine ---
pg_url = (
    f"postgresql://{pg_config['username']}:{pg_config['password']}@"
    f"{pg_config['host']}:{pg_config['port']}/{pg_config['database']}"
)
pg_engine = create_engine(pg_url)

# --- Connect to SQL Server ---
sql_conn = pyodbc.connect(sql_server_conn_str)
sql_cur = sql_conn.cursor()

#Fix target value (character limit, mappings)
def fix_target_value(target_field, value):
    # Ports mapping
    PORT_TYPE_MAPPING = {
        'Port': 'P',
        'Marina': 'M',
        'Anchorage': 'A',
        'Offshore Terminal': 'T'
    }
    if target_field == 'PORT_TYPE':
        value = PORT_TYPE_MAPPING.get(value, None)

    ## The solution is temporary
    # set max field lenths
    MAX_FIELD_LENGTHS = {
        'PORT_NAME': 20
    }
    # check if value exceeds limit - Temporary solution
    if target_field in MAX_FIELD_LENGTHS and isinstance(value, str):
        max_len = MAX_FIELD_LENGTHS[target_field]
        value = value[:max_len] if value else value

    # Special handling for fields that may be arrays - Temporary solution
    if isinstance(value, list):
        value = value[0] if value else None

    # Handle cases Arithmetic overflow error for data type smallint
    SMALLINT_MAX = 32767
    if target_field in {'RELATED_ANCH_ID', 'RELATED_PORT_ID'}:
        if isinstance(value, int) and (value > SMALLINT_MAX ):
            logging.warning(
                f"SMALLINT overflow: {target_field} value {value} exceeds range. Replacing with NULL."
            )
            return None





    return value

# Ensure correct ring orientation for polygons and multipolygons
def correct_orientation(geom):
    if isinstance(geom, Polygon):
        return orient(geom)
    elif isinstance(geom, MultiPolygon):
        return MultiPolygon([orient(p) for p in geom.geoms])
    else:
        return geom

# --- Helper Function to Upload a GeoDataFrame ---
def upload_gdf_to_sqlserver(gdf, mapping_fields, target_table, use_identity_insert=False):
    """
    Upload a GeoDataFrame into SQL Server, mapping fields correctly.
    Arithmetic overflow error for data type smallint,
    """
    # Setup error logger
    logging.basicConfig(
        filename='failed_inserts.log',
        filemode='a',
        level=logging.ERROR,
        format='%(message)s'
    )
    print(f"Updating {target_table}...")
    insert_sql = f"""
    INSERT INTO {target_table} ({', '.join(mapping_fields.values())})
    VALUES ({', '.join(['?'] * len(mapping_fields))})
    """
    #Set counter
    failed_rows = 0

    try:
        # Check if mt_id is not null
        if use_identity_insert:
            print(f"SET IDENTITY_INSERT {target_table} ON")
            sql_cur.execute(f"SET IDENTITY_INSERT {target_table} ON;")

        for idx, row in gdf.iterrows():
            values = []
            # keep current fields and values for error logging
            current_row_data = {}

            for source_field, target_field in mapping_fields.items():
                value = fix_target_value(target_field,row[source_field])
                # Convert geometry to WKT
                if isinstance(value, (BaseGeometry, object)) and source_field.lower().endswith('geom'):
                    value = value.wkt if value else None
                values.append(value)
                current_row_data[target_field] = value
            #print(f"Preparing to insert into {target_table}: {dict(zip(mapping_fields.values(), values))}")
            try:
                sql_cur.execute(insert_sql, *values)
            except Exception as row_error:
                failed_rows += 1
                # Get zone_id
                zone_id_value = row['zone_id']
                # Capture full traceback
                full_error_message = str(row_error).replace('\n', ' ').replace(',', ';')
                # Format log: zone_id, target_table, error_message
                error_log_line = f"{zone_id_value},{target_table},{full_error_message}"
                print(error_log_line)
                logging.error(error_log_line)
                continue  # Skip this bad row and continue

        if use_identity_insert:
            print(f"\U0001F512 SET IDENTITY_INSERT {target_table} OFF")
            sql_cur.execute(f"SET IDENTITY_INSERT {target_table} OFF;")
        # Commit changes
        sql_conn.commit()
        print(f"Uploaded {len(gdf) - failed_rows} successful records to {target_table}")
        if failed_rows > 0:
            print(f"{failed_rows} records failed and were logged.")

    except Exception as e:
        print(f"Error uploading to {target_table}: {str(e)}")
        traceback.print_exc()
        sql_conn.rollback()

# --- Function to Update Ports ---
def update_ports():
    print("Starting to update PORTS...")
    #sql query to get Ports
    pg_Port_query = """
    SELECT 
        zone_id,
        mt_id,
        name,
        zone_type,
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
        print("read_postgis...")
        gdf = gpd.read_postgis(
            sql=pg_Port_query,
            con=pg_engine,
            geom_col='polygon_geom'
        )
        # Check for ring orientation issues SQL server requires : outer ring - anticclockwise & inner ring-clockwise
        gdf['polygon_geom'] = gdf['polygon_geom'].apply(lambda geom: correct_orientation(geom) if geom and geom.is_valid else geom)
        print(f"Ports GeoDataFrame loaded and checked: {len(gdf)} records.")

        # Optional: set CRS if missing
        if gdf.crs is None:
            gdf.set_crs(epsg=4326, inplace=True)
        print ("gdf.crs: ",gdf.crs)

        # Define mapping: source field -> target SQL Server field
        port_mapping_fields = {
            'zone_id': 'shelter',#to keep track of zone_id, help field
            'mt_id': 'PORT_ID',
            'name': 'PORT_NAME',
            'zone_type': 'PORT_TYPE',
            'unlocode': 'UNLOCODE',
            'country_code': 'COUNTRY_CODE',
            'timezone_id': 'TIMEZONE',
            'dst_id': 'DST',
            'enable_calls': 'ENABLE_CALLS',
            'confirmed': 'CONFIRMED',
            #"CENTERX": 'CENTERX',
            #"CENTERY": 'CENTERY',
            #'alternative_names': 'ALTERNATIVE_NAMES',
            #'alternative_unlocodes': 'ALTERNATIVE_UNLOCODES',
            'related_zone_anch_id': 'RELATED_ANCH_ID',
            'related_zone_port_id': 'RELATED_PORT_ID',
            'polygon_geom': 'POLYGON'
        }

        # Split dataset
        gdf_with_id = gdf[gdf['mt_id'].notnull()].copy() #when mt_id is not null, match
        gdf_without_id = gdf[gdf['mt_id'].isnull()].copy() #when mt_id is null, no match

        # Insert with explicit ID (IDENTITY_INSERT ON) mt_id not Null
        if not gdf_with_id.empty:
            upload_gdf_to_sqlserver(
                gdf=gdf_with_id,
                mapping_fields=port_mapping_fields,
                target_table="dbo.PORTS",
                use_identity_insert=True
            )

        # Remove 'mt_id' from mapping for auto-increment insert to handle cases with mt_id is Null
        no_id_mapping = port_mapping_fields.copy()
        del no_id_mapping['mt_id']

        if not gdf_without_id.empty:
            print (len(gdf_without_id), "records without mt_id are about to insert.")
            upload_gdf_to_sqlserver(
                gdf=gdf_without_id,
                mapping_fields=no_id_mapping,
                target_table="dbo.PORTS",
                use_identity_insert=False
            )

    except Exception as e:
        print(f"Error in update_ports(): {str(e)}")
        traceback.print_exc()

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