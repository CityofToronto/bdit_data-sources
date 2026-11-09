import xml.etree.ElementTree as ET
import pandas as pd
import logging
import configparser
import psycopg2
from psycopg2.extras import execute_batch
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_detectors(xml_file):
    """Parse Detectors XML into list of dicts"""
    tree = ET.parse(xml_file)
    root = tree.getroot()
    
    detectors = []
    for detector in root.findall('Detectors'):
        record = {
            'detector_id': detector.findtext('DetectorId'),
            'detector_name': detector.findtext('DetectorName'),
            'short_name': detector.findtext('ShortName'),
            'equipment_type': detector.findtext('EquipmentType'),
            'latitude': detector.findtext('Latitude_deg'),
            'longitude': detector.findtext('Longitude_deg'),
            'altitude': detector.findtext('Altitude_m'),
            'utc_offset': detector.findtext('UTCOffset_hr'),
            'allowed_silence_s': detector.findtext('AllowedSilence_s'),
            'gapout_time_s': detector.findtext('GapoutTime_s'),
            'real_time_stats': detector.findtext('RealTimeStats'),
            'battery_alarm_threshold_v': detector.findtext('BatteryAlarmThreshold_v'),
            'additional_info': detector.findtext('AdditionalInfo'),
            'comments': detector.findtext('Comments'),
        }
        detectors.append(record)
    
    return detectors

def parse_links(xml_file):
    """Parse Links XML into list of dicts"""
    tree = ET.parse(xml_file)
    root = tree.getroot()
    
    links = []
    for link in root.findall('Links'):
        record = {
            'link_id': link.findtext('LinkId'),
            'link_name': link.findtext('LinkName'),
            'short_name': link.findtext('ShortName'),
            'additional_info': link.findtext('AdditionalInfo'),
            'src_detector_id': link.findtext('SrcDetectorId'),
            'dest_detector_id': link.findtext('DestDetectorId'),
            'line_distance_m': link.findtext('LineDistance_m'),
            'path_distance_m': link.findtext('PathDistance_m'),
            'route_direction_name': link.findtext('RouteDirectionName'),
            'speed_limit_kmh': link.findtext('SpeedLimit_kmh'),
            'max_travel_time_s': link.findtext('MaxTravelTime_s'),
            'detection_rules': link.findtext('DetectionRules'),
            'real_time_stats': link.findtext('RealTimeStats'),
            'comments': link.findtext('Comments'),
            'extra_info': link.findtext('ExtraInfo'),
        }
        links.append(record)
    
    return links

def parse_routes(xml_file):
    """Parse Routes XML into list of dicts with member links as JSON array"""
    tree = ET.parse(xml_file)
    root = tree.getroot()
    
    routes = []
    for route in root.findall('Routes'):
        # Extract all Member* fields dynamically
        members = []
        for elem in route:
            if elem.tag.startswith('Member'):
                link_id = elem.text
                if link_id and link_id.strip():  # Only add non-empty members
                    members.append(link_id)
        
        record = {
            'route_id': route.findtext('RouteId'),
            'route_name': route.findtext('RouteName'),
            'short_name': route.findtext('ShortName'),
            'start_offset_m': route.findtext('StartOffset_m'),
            'end_offset_m': route.findtext('EndOffset_m'),
            'comments': route.findtext('Comments'),
            'extra_info': route.findtext('ExtraInfo'),
            'members': members,  # List of link IDs
            'additional_info': route.findtext('AdditionalInfo'),
        }
        routes.append(record)
    
    return routes

def clean_detectors_df(records):
    """Clean and type-cast detector data"""
    df = pd.DataFrame(records)
    
    # Handle empty/whitespace strings
    df = df.replace(r'^\s*$', None, regex=True)
    
    # Type conversions
    numeric_cols = ['latitude', 'longitude', 'altitude', 'utc_offset', 
                    'allowed_silence_s', 'gapout_time_s', 'battery_alarm_threshold_v']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    df['real_time_stats'] = df['real_time_stats'].astype('Int64', errors='ignore')
    
    # Validation
    df = df.dropna(subset=['detector_id'])  # Must have an ID
    
    logger.info(f"Cleaned {len(df)} detector records")
    return df

def clean_links_df(records):
    """Clean and type-cast link data"""
    df = pd.DataFrame(records)
    
    # Handle empty/whitespace strings
    df = df.replace(r'^\s*$', None, regex=True)
    
    # Type conversions
    numeric_cols = ['line_distance_m', 'path_distance_m', 'speed_limit_kmh', 'max_travel_time_s']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Validation
    df = df.dropna(subset=['link_id'])
    
    logger.info(f"Cleaned {len(df)} link records")
    return df

def clean_routes_df(records):
    """Clean and type-cast route data, convert members to JSON"""
    df = pd.DataFrame(records)
    
    # Handle empty/whitespace strings
    df = df.replace(r'^\s*$', None, regex=True)
    
    # Type conversions
    df['start_offset_m'] = pd.to_numeric(df['start_offset_m'], errors='coerce')
    df['end_offset_m'] = pd.to_numeric(df['end_offset_m'], errors='coerce')
    
    # Convert members list to PostgreSQL array format {elem1,elem2,elem3}
    def list_to_pg_array(lst):
        if not lst:
            return None
        # Convert list to PostgreSQL array syntax
        return '{' + ','.join(lst) + '}'
    
    df['members'] = df['members'].apply(list_to_pg_array)
    df.rename(columns={"members": "links"}, inplace=True)
    
    # Validation
    df = df.dropna(subset=['route_id']) # Must have an ID
    
    logger.info(f"Cleaned {len(df)} route records")
    return df

def load_to_postgres(df, table_name):
    """Load DataFrame to PostgreSQL with upsert capability"""
    
    CONFIG = configparser.ConfigParser()
    CONFIG.read(str(Path.home().joinpath('db.cfg')))

    dbset = CONFIG['DBSETTINGS']
    conn = psycopg2.connect(**dbset)
    conn.autocommit = True
    cursor = conn.cursor()
    
    try:
        # Prepare data
        columns = df.columns.tolist()
        data = [tuple(row) for row in df.values]
        sql = f"""
            INSERT INTO bluetooth.{table_name} ({', '.join(columns)})
            VALUES ({', '.join(['%s'] * len(columns))})
        """
        
        # Use batch insert for performance
        execute_batch(cursor, sql, data, page_size=1000)
        conn.commit()
        
        logger.info(f"Successfully loaded {len(data)} records to {table_name}")
        return True
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading {table_name}: {e}")
        raise
        
    finally:
        cursor.close()
        conn.close()

# Usage
fpath = str(Path.home().joinpath('TPANA_Equipment.XML'))

detectors = parse_detectors(fpath)
links = parse_links(fpath)
routes = parse_routes(fpath)
logger.info(f"Parsed {len(detectors)} detectors, {len(links)} links, {len(routes)} routes")

#load detectors
df_detectors = clean_detectors_df(detectors)
print(df_detectors.head())
load_to_postgres(df_detectors, 'tpana_detectors')

#load links
df_links = clean_links_df(links)
print(df_links.head())
load_to_postgres(df_links, 'tpana_links')

#load routes
df_routes = clean_routes_df(routes)
print(df_routes.head())
load_to_postgres(df_routes, 'tpana_routes')