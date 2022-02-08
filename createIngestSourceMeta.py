#!/usr/bin/env python
# coding: utf-8

# Import python modules
import argparse, psycopg2, sys, os
import pandas as pd
from psycopg2.extensions import AsIs
from loguru import logger

# This function takes a list of station names as input, and uses them to query the apsviz_gauges database, and return a list
# of station id(s).
def getStationID(locationType):
    try:
        # Create connection to database and get cursor
        conn = psycopg2.connect("dbname='apsviz_gauges' user='apsviz_gauges' host='localhost' port='5432' password='apsviz_gauges'")
        cur = conn.cursor()

        # Set enviromnent 
        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")

        # Run query 
        cur.execute("""SELECT station_id, station_name FROM drf_gauge_station
                       WHERE location_type = %(location_type)s
                       ORDER BY station_name""", 
                    {'location_type': locationType})
       
        # convert query output to Pandas dataframe 
        df = pd.DataFrame(cur.fetchall(), columns=['station_id', 'station_name'])

        # Close cursor and database connection
        cur.close()
        conn.close()

        # Return Pandas dataframe
        return(df)

    # If exception print error
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

# This function takes a input a directory path and outputFile, and used them to read the input file
# and add station_id(s) that are extracted from the apsviz_gauges database.
def addMeta(outputDir, outputFile):
    # Extract list of stations from dataframe for query database using the getStationID function,
    locationType = outputFile.split('_')[2]
    df = getStationID(locationType)

    # Get source name from outputFilee
    source = outputFile.split('_')[0]

    # Check if source is ADCIRC
    if source == 'adcirc':
        # Get source_name and data_source from outputFile, and add them to the dataframe along
        # with the source_archive value
        df['data_source'] = outputFile.split('_')[3].lower()+'_'+outputFile.split('_')[4].lower()
        df['source_name'] = source
        df['source_archive'] = 'renci'
    elif source == 'contrails':
        # Add data_source, source_name, and source_archive to dataframe
        gtype = outputFile.split('_')[2].lower()
        df['data_source'] = gtype+'_gauge'
        df['source_name'] = 'ncem'
        df['source_archive'] = source
    elif source == 'noaa':
        # Add data_source, source_name, and source_archive to dataframe
        df['data_source'] = 'tidal_gauge'
        df['source_name'] = source
        df['source_archive'] = source
    else:
        # If source in incorrect print message and exit
        sys.exit('Incorrect source')
 
    # Drop station_name from DataFrame 
    df.drop(columns=['station_name'], inplace=True)

    # Reorder column name and update indeces 
    newColsOrder = ['station_id','data_source','source_name','source_archive']
    df=df.reindex(columns=newColsOrder)

    # Write dataframe to csv file 
    df.to_csv(outputDir+'source_'+outputFile, index=False)

# Main program function takes args as input, which contains the outputDir, and outputFile values.
@logger.catch
def main(args):
    # Add logger
    logger.remove()
    log_path = os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs'))
    logger.add(log_path+'/createIngestSourceMeta.log', level='DEBUG')

    # Extract args variables
    outputDir = args.outputDir
    outputFile = args.outputFile

    logger.info('Start processing source data for file '+outputFile+'.')
    addMeta(outputDir, outputFile)
    logger.info('Finished processing source data for file '+outputFile+'.')

# Run main function takes outputDir, and outputFile as input.
if __name__ == "__main__":
    """ This is executed when run from the command line """
    parser = argparse.ArgumentParser()

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--outputDir", action="store", dest="outputDir")
    parser.add_argument("--outputFile", action="store", dest="outputFile")

    args = parser.parse_args()
    main(args)

