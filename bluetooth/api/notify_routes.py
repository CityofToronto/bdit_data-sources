'''notify_routes.py 
Compares Blip route configurations in the database with the previous day's tables and sends an email of the difference.

'''
import configparser

from psycopg2 import connect

from email_notifications import send_email

def email_updates(subject: str, to: str, updates: list):
    """
    Email updated or new analysis configurations
        :param subject:
            Email subject line
        :param to:
            String of addresses to send to
        :param upserted_analyses:
            A list containg tuples of (report.id, report.reportName, report.routeName)
    """

    message = ''
    for analysis in updates:
        message += 'Route id: {route_id}, '\
                   'Report Name: {report_name}, '\
                   'Route Name: {route_name}'.format(route_id=analysis[0],
                                                     report_name=analysis[1],
                                                     route_name=analysis[2])
        message += "\n"
    sender = "Blip API Script"

    send_email(to, sender, subject, message)

def load_diff(dbsettings):
    con = connect(**dbsettings)

    with con.cursor() as cur:
        cur.execute('''SELECT route_id, report_name, route_name
        FROM bluetooth.all_analyses
        EXCEPT

        SELECT route_id, report_name, route_name
        FROM bluetooth.all_analyses_day_old''')
        data = cur.fetchall()
    con.close()
    return data

def load_config(config_path='config.cfg'):
    config = configparser.ConfigParser()
    config.read(config_path)
    dbset = config['DBSETTINGS']
    email_settings = config['EMAIL']
    return dbset, email_settings

def main():

    dbset, email_settings = load_config()

    updated_rows = load_diff(dbset)

    email_updates(email_settings['subject'], email_settings['to'], updated_rows)

if __name__ == '__main__':
    main()
