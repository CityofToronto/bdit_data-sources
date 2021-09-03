import requests
from requests import Session
from requests.auth import HTTPBasicAuth
from requests.exceptions import RequestException
import configparser
import logging
from logging.handlers import TimedRotatingFileHandler
import sys


requests.packages.urllib3.disable_warnings()

logger = logging.getLogger('blip_space_logger')
logger.setLevel(logging.INFO)

handler = TimedRotatingFileHandler(r'C:\Users\Public\Documents\blip_space\blip_space_log.log',
                             when='D', interval=1, backupCount=7)
handler.suffix = "%Y%m%d"
formatter = logging.Formatter('%(asctime)s, %(levelname)s, %(message)s',
                              "%Y-%m-%d %H:%M:%S")
handler.setFormatter(formatter)
#Since TimedRotatingFileHandler won't rotate logs unless the program is running through midnight, force a manual rollover 
handler.doRollover()
logger.addHandler(handler)


def main():
    session = Session()
    session.verify = False
    config = configparser.ConfigParser()
    config.read('config.cfg')
    space_api_settings = config['SPACE']

    try:
        result = session.get(space_api_settings['url'], 
                         auth=HTTPBasicAuth(space_api_settings['username'], space_api_settings['password']))
        result.raise_for_status()
        result_dict = result.json()
    except RequestException as exc:
        logger.critical('Error querying API, %s', exc)
        sys.exit(2)

    try:
        space_value = float(result_dict['data'][0][2])
    except ValueError as exc:
        logger.critical('ValueError trying to convert, %s',
                         result['data'][0][2])
        sys.exit(1)
    if space_value < 15:
        logger.warning('Disk Space Low, %s', space_value)
    else:
        logger.info('Disk Space OK, %s', space_value)

if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        logger.critical('Unknown error, %s', exc)
