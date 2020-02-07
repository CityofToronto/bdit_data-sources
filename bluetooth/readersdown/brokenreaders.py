import configparser
from psycopg2 import connect
from email_notifications import send_mail

class BlipScriptFailed(Exception):
    pass

def update_empty_date_reports(con):
    '''Update dates_without_data table with the inactive routes from the previous day'''
    with con:
        with con.cursor() as cur:
            cur.execute('SELECT bluetooth.insert_dates_without_data()')
    
def find_brokenreaders(con):
    '''Identify the sensors which stopped reporting yesterday. 
    
    If this number is equal to the number of routes, then the Blip script likely failed and this exception is raise'''

    sql = '''WITH broken_routes AS (SELECT DISTINCT (json_array_elements(route_points)->'name')::TEXT as sensor
             FROM bluetooth.all_analyses
             INNER JOIN (SELECT analysis_id 
                         FROM bluetooth.dates_without_data WHERE day_without_data >= current_date - 2 --Ensure recency
                         GROUP BY analysis_id 
                         HAVING MAX(day_without_data) = current_date - 1 AND COUNT(1) < 2  --Ensure there was no data from the day before but 
                                                                                           --sensor didn't get emailed the previous day
                         ) missing_dates USING (analysis_id)
             )
             , working_routes AS (SELECT DISTINCT (json_array_elements(route_points)->'name')::TEXT as sensor
             FROM bluetooth.all_analyses
             INNER JOIN (SELECT DISTINCT analysis_id
                         FROM bluetooth.observations
                         WHERE measured_timestamp > current_date - 1) routes_with_recent_data USING (analysis_id)            
             )
             SELECT sensor
             FROM broken_routes
             EXCEPT 
             SELECT sensor
             FROM working_routes
             ORDER BY sensor

    '''

    final = []
    with con.cursor() as cur:
        cur.execute(sql)
        final = cur.fetchall()
        all_routes_count = 'SELECT COUNT(1) FROM bluetooth.all_analyses WHERE pull_data'
        noreport_routes_count = 'SELECT COUNT(1) FROM bluetooth.routes_not_reporting_yesterday'
        cur.execute(all_routes_count)
        routes_count = cur.fetchone()[0]
        cur.execute(noreport_routes_count)
        bad_routes_count = cur.fetchone()[0]
    
    if routes_count == bad_routes_count:
        raise BlipScriptFailed()

    return final 


def email_updates(subject: str, to: str, updates: list):
    '''Send email with a list of sensors stopped reporting yesterday'''
    message = ''
    for i, broken_reader in enumerate(updates):
        if i > 0:
            message += ",\n" 
        message += 'Reader: {reader_name} '.format(reader_name=broken_reader[0])
    sender = "Broken Readers Detection Script"

    send_mail(to, sender, subject, message)

def load_config(config_path='config.cfg'):
    
    config = configparser.ConfigParser()
    config.read(config_path)
    dbset = config['DBSETTINGS']
    email_settings = config['EMAIL']
    return dbset, email_settings

def main (): 

    dbset, email_settings = load_config()
    con = connect(**dbset)

    update_empty_date_reports(con)
    try:
        broken_readers = find_brokenreaders(con)
    except BlipScriptFailed:
        script = '''This email was generated because no data was received by the database this morning. 
                    This may be because the script failed for an unknown reason, the blip server was offline 
                    or there were connectivity issues between the Terminal Server and either the Blip server 
                    or our RDS. Someone will have to log in to the Terminal Server to debug.'''
        send_mail(email_settings['to'], "Broken Readers Detection Script", "Blip Script Failed this morning", script)
    else:
        if broken_readers != []:
            email_updates(email_settings['subject'], email_settings['to'], broken_readers)

if __name__ == '__main__': 
    main()
