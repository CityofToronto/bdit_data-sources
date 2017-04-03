#!/usr/bin/python3
'''Aggregrates Inrix data to 15 minute bins'''
def agg_table(logger, cursor, *, tmcschema = None, alldata = False, yyyymm, **kwargs):
    '''Aggregate data from the inrix.raw_data partitioned table with yyyymm
    and insert it in inrix.agg_extract_hour.'''
    if tmcschema:
        logger.info('Aggregating table %s with subset from schema %s table %s',
                    'inrix.raw_data'+yyyymm, tmcschema, tmctable)
        cursor.execute('SELECT inrix.agg_extract_hour_alldata(%(yyyymm)s, '
                    '%(tmcschema)s, %(tmctable)s)',
                    {'yyyymm':yyyymm,
                     'tmcschema':tmcschema,
                     'tmctable':tmctable})
    elif alldata:
        logger.info('Aggregating table %s using all data', 'inrix.raw_data'+yyyymm)
        cursor.execute('SELECT inrix.agg_extract_hour_alldata(%(yyyymm)s)', {'yyyymm':yyyymm})
    else:
        logger.info('Aggregating table %s', 'inrix.raw_data'+yyyymm)
        cursor.execute('SELECT inrix.agg_extract_hour(%(yyyymm)s)', {'yyyymm':yyyymm})

#if __name__ == "__main__":
#    #For initial run, creating years and months of available data as a python dictionary
#    YEARS = {"2012":range(7, 13),
#             "2013":range(1, 13),
#             "2011":range(8, 13),
#             "2016":range(1, 7),
#             "2014":range(1, 13),
#             "2015":range(1, 13)}
#    #Configure logging
#    FORMAT = '%(asctime)-15s %(message)s'
#    logging.basicConfig(level=logging.INFO, format=FORMAT)
#    LOGGER = logging.getLogger(__name__)
#    from dbsettings import dbsetting
#    agg_tables(YEARS, dbsetting, LOGGER)
