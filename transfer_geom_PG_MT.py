#pip install psycopg2-binary SQLAlchemy python-dotenv --index-url https://pypi.org/simple
from numpy.core.defchararray import startswith
from sqlalchemy import create_engine, text
import pyodbc, os
from shapely.geometry.base import BaseGeometry
from urllib.parse import quote_plus
from dotenv import load_dotenv
import geopandas as gpd, pandas as pd
import traceback, logging
from shapely.geometry import Polygon, MultiPolygon
from shapely.geometry.polygon import orient
import math
from typing import Union, List
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
print("Autocommit:", sql_conn.autocommit)
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
        'PORT_NAME': 20,
        'BERTH_NAME': 30,
        'TERMINAL_NAME' : 50
    }
    # check if value exceeds limit - Temporary solution
    if target_field in MAX_FIELD_LENGTHS and isinstance(value, str):
        max_len = MAX_FIELD_LENGTHS[target_field]
        value = value[:max_len] if value else value

    # Special handling for fields that may be arrays - Temporary solution
    if isinstance(value, list):
        value = value[0] if value else None

    # Convert NaN to None
    if isinstance(value, float) and math.isnan(value):
        return None

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

# Global dictionary to hold mapping of source zone_id to new SQL Server-assigned PORT_ID,TERMINAL_ID
zoneid_to_new_portid = {}
zoneid_to_new_terminalid = {}
# Setup error logger
logging.basicConfig(
    filename='failed_inserts.log',
    filemode='a',
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)

# Create subset of the gdf based on field mapping for different tables
def create_df_subset(df, field_mapping):
    subset_columns = list(field_mapping.keys())
    #Add also zone_id to be used for the  mapping csv file generation
    if 'zone_id' not in subset_columns:
        subset_columns.append('zone_id')
    # Detect which columns contain any list-type values
    list_columns = [col for col in df.columns if df[col].apply(lambda x: isinstance(x, list)).any()]
    #Covert list to comma delimeted strings
    for col in list_columns:
        df[col] = df[col].apply(lambda x: ','.join(map(str, x)) if isinstance(x, list) else x)
    #Drop Duplicates
    gdf_unique = df.drop_duplicates()
    #Covert back to list type
    for col in list_columns:
        gdf_unique[col] = gdf_unique[col].apply(
                lambda x: x.split(',') if isinstance(x, str) and ',' in x else [x] if x else [])
        gdf_unique[col] = gdf_unique[col].apply(
            lambda x: [i for i in x.split(',') if i not in ('', 'None', 'null')] if isinstance(x, str) else []
        )#Remove null values

    return gdf_unique

# --- Helper Function to Upload a GeoDataFrame ---
def upload_gdf_to_sqlserver(gdf, mapping_fields, target_table, use_identity_insert=False, return_identity_mapping=False):
    print(f"Updating {target_table}...")
    insert_sql = f"""
    INSERT INTO {target_table} ({', '.join(mapping_fields.values())})
    --OUTPUT INSERTED.PORT_ID
    VALUES ({', '.join(['?'] * len(mapping_fields))});
    SELECT SCOPE_IDENTITY();
    """
    #Set counter
    failed_rows = 0

    identity_mapping = {}  # to collect zone_id -> new PORT_ID
    try:
        # Check if mt_id is not null
        if use_identity_insert:
            print(f"SET IDENTITY_INSERT {target_table} ON")
            sql_cur.execute(f"SET IDENTITY_INSERT {target_table} ON;")

        for idx, row in gdf.iterrows():
            values = []
            # keep current fields and values for error logging
            current_row_data = {}
            #loop for every source and target field pair
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
                if return_identity_mapping  and not use_identity_insert:
                    if target_table == 'dbo.PORTS':
                        #PORTS-try to get the ID of the inserted record by matching the polygons
                        sql_cur.execute(f"""
                            SELECT port_id
                            FROM {target_table}
                            WHERE POLYGON.STEquals(geography::STGeomFromText(?, 4326)) = 1
                            ORDER BY port_id DESC
                        """, (values[-1],))  # assuming geometry is the last field
                    elif target_table == 'dbo.PORT_TERMINALS':
                        #TERMINALS-TRY TO GET THE id OF THE INSERTED RECORD BY MATCHING NAME & port_id
                        sql = f"""SELECT terminal_id FROM {target_table} WHERE TERMINAL_NAME = '{values[0]}' AND PORT_ID = {int(values[1])}
                        """
                        #print (sql)
                        sql_cur.execute(sql)
                    new_id = sql_cur.fetchone()[0]
                    zone_id = row['zone_id']
                    identity_mapping[zone_id] = new_id ##{zone_id:<new_port_id>} / {zone_id:<new_terminal_id>}

                sql_conn.commit()
            except Exception as row_error:
                print ("Error occurred. Values to insert: ",values)
                logging.error(values)
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
            print(f" SET IDENTITY_INSERT {target_table} OFF")
            sql_cur.execute(f"SET IDENTITY_INSERT {target_table} OFF;")
        # Commit changes
        sql_conn.commit()
        print(f"Uploaded {len(gdf) - failed_rows} successful records to {target_table}")
        if failed_rows > 0:
            print(f"{failed_rows} records failed and were logged.")
        #Return identity mapping if created

        if identity_mapping:
            return identity_mapping

    except Exception as e:
        print(f"Error uploading to {target_table}: {str(e)}")
        traceback.print_exc()
        sql_conn.rollback()

# --- Function to Update Ports ---
def update_ports(port_list = None):
    print("Starting to update PORTS...")
    logging.info("Starting to update PORTS...")
    if isinstance(port_list, list):
        id_str = ', '.join(str(i) for i in port_list)
        sql_where = f'zone_id in ({id_str})'
    else:
        sql_where = '1=1'
    #sql query to get Ports
    pg_Port_query = f"""
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
    FROM sandbox.mview_master_ports where {sql_where}
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
        logging.info(f"Ports GeoDataFrame loaded and checked: {len(gdf)} records.")

        # Optional: set CRS if missing
        if gdf.crs is None:
            gdf.set_crs(epsg=4326, inplace=True)
        print ("gdf.crs: ",gdf.crs)


        # Define mapping: source field -> target SQL Server field
        #PORTS
        port_mapping_fields = {
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
            'related_zone_anch_id': 'RELATED_ANCH_ID',
            'related_zone_port_id': 'RELATED_PORT_ID',
            'polygon_geom': 'POLYGON'
        }
        #R_PORT_ALTNAMES
        alt_port_name_mapping_fields = {
            'mt_id': 'PORT_ID',
            'alternative_names': 'ALTERNATIVE_NAMES',
            'alternative_unlocodes': 'ALTERNATIVE_UNLOCODES',
        }
        def upload_port_data(target_table, field_mapping, gdf):
            print ("Target table:",target_table)

            # Split dataset
            gdf_with_id = gdf[gdf['mt_id'].notnull()].copy() #when mt_id is not null, match
            gdf_without_id = gdf[gdf['mt_id'].isnull()].copy() #when mt_id is null, no match

            #A. Insert with explicit ID (IDENTITY_INSERT ON) 'mt_id is not Null'
            if not gdf_with_id.empty:
                upload_gdf_to_sqlserver(
                    gdf=gdf_with_id,
                    mapping_fields=field_mapping,
                    target_table=f"dbo.{target_table}",
                    use_identity_insert=True
                )
            #double check
            sql_conn.commit()

            # B. Insert and let system assign IDs (IDENTITY_INSERT OFF) 'mt_id is Null'
            # Remove 'mt_id' from mapping for auto-increment insert to handle cases with mt_id is Null
            no_id_mapping = field_mapping.copy()
            del no_id_mapping['mt_id']

            #print ("gdf_without_id.empty: ",gdf_without_id.empty)
            if not gdf_without_id.empty:
                print (len(gdf_without_id), "records without mt_id are about to insert.")
                logging.info(f"{len(gdf_without_id)} records without mt_id are about to insert.")
                mapping = upload_gdf_to_sqlserver(
                    gdf=gdf_without_id,
                    mapping_fields=no_id_mapping,
                    target_table=f"dbo.{target_table}",
                    use_identity_insert=False,
                    return_identity_mapping=True
                )
            else:
                return

            # Store globally the zone_ids : new assigned MT ids by the system
            # Save the mapping only if target table is PORT_TERMINALS
            if target_table == "PORTS":
                zoneid_to_new_portid.update(mapping)
                #print ("zoneid_to_new_portid:",zoneid_to_new_portid)
                #save it locally in CSV file
                pd.DataFrame.from_dict(zoneid_to_new_portid, orient='index', columns=['new_port_id']) \
                    .rename_axis('zone_id') \
                    .reset_index() \
                    .to_csv(f"zoneid_to_new_portid_mapping.csv", index=False)

        # Create subset for Port_Terminals & SMDG tables
        Ports_df = create_df_subset(gdf, port_mapping_fields)
        Alt_names_df = create_df_subset(gdf, alt_port_name_mapping_fields)
        #print ("Alt_names_df:",Alt_names_df.columns)

        ###Upload PORTS
        upload_port_data("PORTS", port_mapping_fields, Ports_df)

        #### Read the mapping csv file to get the new mt_ids
        ##Bypassing the dynamic creation of the global dict and read from local file
        import csv

        if os.path.exists('zoneid_to_new_portid_mapping.csv'):
            with open('zoneid_to_new_portid_mapping.csv', mode='r', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    zone_id = int(row['zone_id'])
                    new_port_id = int(row['new_port_id']) if row['new_port_id'] else None
                    zoneid_to_new_portid[zone_id] = new_port_id
            #Update missing values in new mt port_id to use it in the R_PORT_ALTNAMES table
            for idx, row in Alt_names_df.iterrows():
                mt_port_id = row['mt_id']
                if pd.isnull(mt_port_id) and row['port_id'] in zoneid_to_new_portid:
                    Alt_names_df.at[idx, 'mt_port_id'] = zoneid_to_new_portid[row['port_id']]


        ###Upload R_PORT_ALTNAMES
        #upload_port_data("R_PORT_ALTNAMES", alt_port_name_mapping_fields, Alt_names_df)

    except Exception as e:
        print(f"Error in update_ports(): {str(e)}")
        logging.error(f"Error in update_ports(): {str(e)}")
        traceback.print_exc()

# --- Function to Update Berths ---
def update_berths(port_list = None):
    print("Starting to update BERTHS...")
    logging.info("Starting to update BERTHS...")
    if isinstance(port_list, list):
        id_str = ', '.join(str(i) for i in port_list)
        sql_where = f'port_id in ({id_str})'
    else:
        sql_where = '1=1'
    #sql query to get Ports
    pg_Berth_query = f"""
    SELECT 
        zone_id,
        mt_id,
        terminal_id,
        mt_terminal_id,
        port_id,
        mt_port_id,
        name,
        zone_type,
        "MAX_LENGTH",
        "MAX_DRAUGHT",
        "MAX_BREADTH",
        "LIFTING_GEAR",
        "BULK_CAPACITY",
        "DESCRIPTION",
        "MAX_TIDAL_DRAUGHT",
        polygon_geom
    FROM sandbox.mview_master_berths where {sql_where}
    """
    try:
        print("read_postgis...")
        gdf = gpd.read_postgis(
            sql=pg_Berth_query,
            con=pg_engine,
            geom_col='polygon_geom'
        )
        # Check for ring orientation issues SQL server requires : outer ring - anti clockwise & inner ring-clockwise
        gdf['polygon_geom'] = gdf['polygon_geom'].apply(lambda geom: correct_orientation(geom) if geom and geom.is_valid else geom)
        print(f"Berths GeoDataFrame loaded and checked: {len(gdf)} records.")
        logging.info(f"Berths GeoDataFrame loaded and checked: {len(gdf)} records.")

        # Optional: set CRS if missing
        if gdf.crs is None:
            gdf.set_crs(epsg=4326, inplace=True)
        print ("gdf.crs: ",gdf.crs)

        # Assign the new PORT_ID given by the system for cases when Ports in PG and MT are not matched

        #### START Bypassing mapping dict START ###
        # Read New Port ids mapping given in MT system
        ##Bypassing the dynamic creation of the global dict and read from local file
        import csv
        if os.path.exists('zoneid_to_new_portid_mapping.csv'):
            with open('zoneid_to_new_portid_mapping.csv', mode='r', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    zone_id = int(row['zone_id'])
                    new_port_id = int(row['new_port_id']) if row['new_port_id'] else None
                    zoneid_to_new_portid[zone_id] = new_port_id
            #Update null values in mt port_id
            for idx, row in gdf.iterrows():
                mt_port_id = row['mt_port_id']
                if pd.isnull(mt_port_id) and row['port_id'] in zoneid_to_new_portid:
                    gdf.at[idx, 'mt_port_id'] = zoneid_to_new_portid[row['port_id']]

        #Read New terminal ids mapping given in MT system
        if os.path.exists('zoneid_to_new_terminalid_mapping.csv'):
            with open('zoneid_to_new_terminalid_mapping.csv', mode='r', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    zone_id = int(row['zone_id'])
                    new_terminal_id = int(row['new_terminal_id']) if row['new_terminal_id'] else None
                    zoneid_to_new_terminalid[zone_id] = new_terminal_id
            #update null values in me terminal id
            for idx, row in gdf.iterrows():
                mt_terminal_id = row['mt_terminal_id']
                if pd.isnull(mt_terminal_id) and row['terminal_id'] in zoneid_to_new_terminalid:
                    gdf.at[idx, 'mt_terminal_id'] = zoneid_to_new_terminalid[row['terminal_id']]
        #### END Bypassing mapping dict END ###


        # Define mapping: source field -> target SQL Server field
        berth_mapping_fields = {
            #'zone_id':'',
            'mt_id':'BERTH_ID',
            'name':'BERTH_NAME',#30 char limit
            #'zone_type':'',
            'mt_terminal_id':'TERMINAL_ID',
            'mt_port_id':'PORT_ID',
            'MAX_LENGTH':'MAX_LENGTH',
            'MAX_DRAUGHT':'MAX_DRAUGHT',
            'MAX_BREADTH':'MAX_BREADTH',
            'LIFTING_GEAR':'LIFTING_GEAR',
            'BULK_CAPACITY':'BULK_CAPACITY',
            'DESCRIPTION':'DESCRIPTION',
            'MAX_TIDAL_DRAUGHT':'MAX_TIDAL_DRAUGHT',
            'polygon_geom': 'POLYGON'
        }

        # Split dataset
        gdf_with_id = gdf[gdf['mt_id'].notnull()].copy() #when mt_id is not null, match
        gdf_without_id = gdf[gdf['mt_id'].isnull()].copy() #when mt_id is null, no match

        # A. Insert with explicit ID (IDENTITY_INSERT ON) mt_id not Null
        if not gdf_with_id.empty:
            print(len(gdf_without_id), "records with mt_id are about to insert.")
            logging.info(f"{len(gdf_without_id)} records with mt_id are about to insert.")
            upload_gdf_to_sqlserver(
                gdf=gdf_with_id,
                mapping_fields=berth_mapping_fields,
                target_table="dbo.PORT_BERTHS",
                use_identity_insert=True
            )

        # B. Remove 'mt_id' from mapping for auto-increment insert to handle cases with mt_id is Null
        no_id_mapping = berth_mapping_fields.copy()
        del no_id_mapping['mt_id']
        if not gdf_without_id.empty:
            print (len(gdf_without_id), "records without mt_id are about to insert.")
            logging.info(f"{len(gdf_without_id)} records without mt_id are about to insert.")
            upload_gdf_to_sqlserver(
                gdf=gdf_without_id,
                mapping_fields=no_id_mapping,
                target_table="dbo.PORT_BERTHS",
                use_identity_insert=False,
                return_identity_mapping=False
            )
        else:
            return

    except Exception as e:
        print(f"Error in update_berths(): {str(e)}")
        logging.error(f"Error in update_berths(): {str(e)}")
        traceback.print_exc()

# --- Function to Update Terminals ---
def update_terminals(port_list = None):
    print("Starting to update Terminals...")
    logging.info("Starting to update Terminals...")
    if isinstance(port_list, list):
        id_str = ', '.join(str(i) for i in port_list)
        sql_where = f'port_id in ({id_str})'
    else:
        sql_where = '1=1'
    # sql query to get Ports
    pg_Terminal_query = f"""
        SELECT 
            zone_id,
            port_id,
            terminal_id,
            mt_port_id,
            unlocode,
            terminal_code,
            terminal_facility_name,
            terminal_company_name,
            lat,
            lon,
            smdg_listing_date,
            smdg_unlisting_date,
            smdg_updated_date,
            terminal_website,
            terminal_address,
            remarks,
            name,
            zone_type, 
            polygon_geom
        FROM sandbox.mview_master_terminals_mt where {sql_where}
        """
    try:
        print("read_postgis...")
        gdf = gpd.read_postgis(
            sql=pg_Terminal_query,
            con=pg_engine,
            geom_col='polygon_geom'
        )
        # Check for ring orientation issues SQL server requires : outer ring - anti clockwise & inner ring-clockwise
        gdf['polygon_geom'] = gdf['polygon_geom'].apply(
            lambda geom: correct_orientation(geom) if geom and geom.is_valid else geom)
        print(f"Terminal GeoDataFrame loaded and checked: {len(gdf)} records.")
        logging.info(f"Terminal GeoDataFrame loaded and checked: {len(gdf)} records.")

        # Optional: set CRS if missing
        if gdf.crs is None:
            gdf.set_crs(epsg=4326, inplace=True)
        print("gdf.crs: ", gdf.crs)

        #### Bypassing mapping dict START ###
        ##Bypassing the dynamic creation of the global dict and read from local file
        import csv

        if os.path.exists('zoneid_to_new_portid_mapping.csv'):
            with open('zoneid_to_new_portid_mapping.csv', mode='r', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    zone_id = int(row['zone_id'])
                    new_port_id = int(row['new_port_id']) if row['new_port_id'] else None
                    zoneid_to_new_portid[zone_id] = new_port_id
            #### Bypassing mapping dict END ###

            # Update the missing mt_port_id from the csv zone_id<->mt_new_port_id
            before_nulls = gdf['mt_port_id'].isna().sum()
            print(f"Null mt_port_id before mapping: {before_nulls}")
            logging.info(f"Null mt_port_id before mapping: {before_nulls}")
            #Assign the new Port_id set by auto-increment to the related objects
            for idx, row in gdf.iterrows():
                mt_port_id = row['mt_port_id']
                if pd.isnull(mt_port_id) and row['port_id'] in zoneid_to_new_portid:
                    gdf.at[idx, 'mt_port_id'] = zoneid_to_new_portid[row['port_id']]
            # Count nulls after update
            after_nulls = gdf['mt_port_id'].isna().sum()
            print(f"Null mt_port_id after mapping: {after_nulls}")
            logging.info(f"Null mt_port_id after mapping: {after_nulls}")

        # Define mapping: source field -> target SQL Server field
        # source table: sandbox.mview_master_terminals
        # target table: PORT_TERMINALS
        terminal_mapping_fields = {
            # 'zone_id':'',
            'terminal_id': 'TERMINAL_ID',
            'name': 'TERMINAL_NAME',  # 50 char limit
            'mt_port_id': 'PORT_ID',
        }

        #source table: sandbox.mview_master_terminals_mt
        # target table: ais.dbo.smdg_terminal_codes
        smdg_mapping_fields = {
            # 'zone_id':'',
            'terminal_id': 'terminal_id',
            'unlocode': 'unlocode',
            'terminal_code': 'terminal_code',
            'terminal_facility_name': 'terminal_facility_name',
            'terminal_company_name': 'terminal_company_name',
            'lat': 'lat',
            'lon': 'lon',
            'smdg_listing_date': 'smdg_listing_date',
            'smdg_unlisting_date': 'smdg_unlisting_date',
            'smdg_updated_date': 'smdg_updated_date',
            'terminal_website': 'terminal_website',
            'terminal_address': 'terminal_address',
            'remarks': 'remarks'
        }

        def upload_terminal_data(target_table, field_mapping, df):
            print ("Target table:",target_table)

            # Split dataset
            gdf_with_id = df[df['terminal_id'].notnull()].copy()  # when terminal_id is not null, match
            gdf_without_id = df[df['terminal_id'].isnull()].copy()  # when terminal_id is null, no match
            #A. Insert with explicit ID (IDENTITY_INSERT ON) mt_id not Null
            if not gdf_with_id.empty:
                print(len(gdf_with_id), "records with mt_id are about to insert.")
                logging.info(f"{len(gdf_with_id)} records with mt_id are about to insert.")
                upload_gdf_to_sqlserver(
                    gdf=gdf_with_id,
                    mapping_fields=field_mapping,
                    target_table=f"dbo.{target_table}",
                    use_identity_insert=True
                )
            #B. Insert with IDENTITY_INSERT OFF mt_id is Null
            # Remove 'mt_id' from mapping for auto-increment insert to handle cases with mt_id is Null
            no_id_mapping = field_mapping.copy()
            del no_id_mapping['terminal_id']
            if not gdf_without_id.empty:
                print(len(gdf_without_id), "records without mt_id are about to insert.")
                logging.info(f"{len(gdf_without_id)} records without mt_id are about to insert.")
                mapping = upload_gdf_to_sqlserver(
                    gdf=gdf_without_id,
                    mapping_fields=no_id_mapping,
                    target_table=f"dbo.{target_table}",
                    use_identity_insert=False,
                    return_identity_mapping=True
                )
            else: return
            #Save the mapping only if target table is PORT_TERMINALS
            if target_table == "PORT_TERMINALS":
                # Store globally the {zone_ids:new assigned MT ids} dict by the system
                zoneid_to_new_terminalid.update(mapping)
                print ("zoneid_to_new_terminalid:",zoneid_to_new_terminalid)
                #save it locally in CSV file
                pd.DataFrame.from_dict(zoneid_to_new_terminalid, orient='index', columns=['new_terminal_id']) \
                    .rename_axis('zone_id') \
                    .reset_index() \
                    .to_csv(f"zoneid_to_new_terminalid_mapping.csv", index=False)

        #Create subset for Port_Terminals & SMDG tables
        terminals_df = create_df_subset(gdf,terminal_mapping_fields)
        SMDG_df = create_df_subset(gdf, smdg_mapping_fields)

        #Clean SMDG df
        filtered_SMDG = SMDG_df[SMDG_df['terminal_code'].notnull() & (SMDG_df['terminal_code'] != '')]

        ###Upload PORT_TERMINALS
        upload_terminal_data("PORT_TERMINALS", terminal_mapping_fields, terminals_df)
        ###Upload smdg
        #upload_terminal_data("smdg_terminal_codes", smdg_mapping_fields, filtered_SMDG)


    except Exception as e:
        print(f"Error in update_terminals(): {str(e)}")
        logging.error(f"Error in update_terminals(): {str(e)}")
        traceback.print_exc()

# --- MAIN ---
Port_testing_list = [12474]
#mt_port_list = [117,122,134,137,170,262,373,377,794,883,919,970,1253,1459,1505,2715,2745,18411,22221,22264]
if __name__ == "__main__":
    try:
        update_ports(Port_testing_list)
        update_terminals(Port_testing_list)
        update_berths(Port_testing_list)

    finally:
        sql_cur.close()
        sql_conn.close()
        print("SQL Server connection closed.")
