#!/usr/bin/env python
# coding: utf-8

# Import Python modules
import argparse, glob, os, psycopg2
import pandas as pd
from loguru import logger

# This function takes an dataset name as input and uses it to query the drf_harvest_data_file_meta table,
# creating a DataFrame that contains a list of data files to ingest. The ingest directory is the directory
# path in the apsviz-timeseriesdb database container.
def getHarvestDataFileMeta(inputDataset):
    try:
        # Create connection to database and get cursor
        conn = psycopg2.connect("dbname='apsviz_gauges' user='apsviz_gauges' host='localhost' port='5432' password='apsviz_gauges'")
        cur = conn.cursor()

        # Set enviromnent
        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")

        # Run query
        cur.execute("""SELECT file_name
                       FROM drf_harvest_data_file_meta
                       WHERE source = %(source)s AND ingested = False
                       ORDER BY data_date_time""",
                    {'source': inputDataset})

        # convert query output to Pandas dataframe
        df = pd.DataFrame(cur.fetchall(), columns=['file_name'])
 
        # Close cursor and database connection
        cur.close()
        conn.close()

        # Return Pandas dataframe
        if inputDataset == 'adcirc':
            # Limit to 40 files at a time
            return(df.head(40))
        else:
            # Limit to 20 files at a time
            return(df.head(20))

    # If exception print error
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

# This function takes an input directory and ingest directory as input. It uses the input directory to seach for
# harvest_files that need to be ingested. It uses the ingest directory to define the path of the harvest_file
# to ingesting. The ingest directory is the directory path in the apsviz-timeseriesdb database container.
def ingestHarvestDataFileMeta(inputDir, ingestDir):
    inputFiles = glob.glob(inputDir+"harvest_files_*.csv")

    for infoFile in inputFiles:
        # Create list of data info files, to be ingested by searching the input directory for data info files.
        ingestPathFile = ingestDir+infoFile.split('/')[-1]

        try:
            # Create connection to database and get cursor
            conn = psycopg2.connect("dbname='apsviz_gauges' user='apsviz_gauges' host='localhost' port='5432' password='apsviz_gauges'")
            cur = conn.cursor()

            # Set enviromnent
            cur.execute("""SET CLIENT_ENCODING TO UTF8""")
            cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
            cur.execute("""BEGIN""")

            # Run query
            cur.execute("""COPY drf_harvest_data_file_meta(dir_path,file_name,data_date_time,data_begin_time,data_end_time,file_date_time,source,content_info,ingested,version,overlap_past_file_date_time)
                           FROM %(ingest_path_file)s
                           DELIMITER ','
                           CSV HEADER""",
                        {'ingest_path_file': ingestPathFile})

            # Commit ingest
            conn.commit()

            # Close cursor and database connection
            cur.close()
            conn.close()

        # If exception print error
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

# This function takes an input directory and an ingest directory as input. The input directory is used to search for geom 
# station files that are to be ingested. The ingest directory is used to define the path of the file to be ingested. The 
# ingest directory is the directory path in the apsviz-timeseriesdb database container.
def ingestStation(inputDir, ingestDir):
    # Create list of geom files, to be ingested by searching the input directory for geom files.
    inputFiles = glob.glob(inputDir+"geom_*.csv")

    # Loop thru geom file list, ingesting each one 
    for geomFile in inputFiles:
        # Define the ingest path and file using the ingest directory and the geom file name
        ingestPathFile = ingestDir+geomFile.split('/')[-1]
 
        try:
            # Create connection to database and get cursor
            conn = psycopg2.connect("dbname='apsviz_gauges' user='apsviz_gauges' host='localhost' port='5432' password='apsviz_gauges'")
            cur = conn.cursor()

            # Set enviromnent
            cur.execute("""SET CLIENT_ENCODING TO UTF8""")
            cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
            cur.execute("""BEGIN""")

            # Run query
            cur.execute("""COPY drf_gauge_station(station_name,lat,lon,tz,gauge_owner,location_name,location_type,country,state,county,geom)
                           FROM %(ingest_path_file)s
                           DELIMITER ','
                           CSV HEADER""",
                        {'ingest_path_file': ingestPathFile})

            # Commit ingest
            conn.commit()
 
            # Close cursor and database connection
            cur.close()
            conn.close()

        # If exception print error
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

# This function takes an input directory and ingest directory as input. It uses the input directory to search for source  
# csv files, that were created by the createIngestSourceMeta.py program. It uses the ingest directory to define the path
# of the file that is to be ingested. The ingest directory is the directory path in the apsviz-timeseriesdb database container.
def ingestSource(inputDir, ingestDir):
    # Create list of source files, to be ingested by searching the input directory for source files.
    inputFiles = glob.glob(inputDir+"source_*.csv")

    # Loop thru source file list, ingesting each one
    for sourceFile in inputFiles:
        # Define the ingest path and file using the ingest directory and the source file name
        ingestPathFile = ingestDir+sourceFile.split('/')[-1]

        try:
            # Create connection to database and get cursor
            conn = psycopg2.connect("dbname='apsviz_gauges' user='apsviz_gauges' host='localhost' port='5432' password='apsviz_gauges'")
            cur = conn.cursor()

            # Set enviromnent
            cur.execute("""SET CLIENT_ENCODING TO UTF8""")
            cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
            cur.execute("""BEGIN""")

            # Run query
            cur.execute("""COPY drf_gauge_source(station_id,data_source,source_name,source_archive)
                           FROM %(ingest_path_file)s
                           DELIMITER ','
                           CSV HEADER""",
                        {'ingest_path_file': ingestPathFile})

            # Commit ingest
            conn.commit()

            # Close cursor and database connection
            cur.close()
            conn.close()

        # If exception print error
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

# This function takes an ingest directory and input dataset as input, and uses them to run the getHarvestDataFileMeta
# function and define the ingestPathFile variable. The getHarvestDataFileMeta function produces a DataFrame (dfDirFiles) 
# that contains a list of data files, that are queried from the drf_harvest_data_file_meta table. These files are then 
# ingested into the drf_gauge_data table. After the data has been ingested, from a file, the column "ingested", in the 
# drf_harvest_data_file_meta table, is updated from False to True. The ingest directory is the directory path in the 
# apsviz-timeseriesdb database container.
def ingestData(ingestDir, inputDataset):
    # Get DataFrame the contains list of data files that need to be ingested
    dfDirFiles = getHarvestDataFileMeta(inputDataset)

    # Loop thru DataFrame ingesting each data file 
    for index, row in dfDirFiles.iterrows():
        # Get name of file, that needs to be ingested, from DataFrame, and create data_copy file name and output path
        # (ingestPathFile) outsaved to the DataIngesting directory area where is the be ingested using the copy command.
        ingestFile = row[0]
        ingestPathFile = ingestDir+'data_copy_'+ingestFile

        try:
            # Create connection to database and get cursor
            conn = psycopg2.connect("dbname='apsviz_gauges' user='apsviz_gauges' host='localhost' port='5432' password='apsviz_gauges'")
            cur = conn.cursor()

            # Set enviromnent
            cur.execute("""SET CLIENT_ENCODING TO UTF8""")
            cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
            cur.execute("""BEGIN""")

            # Run query
            cur.execute("""COPY drf_gauge_data(source_id,timemark,time,water_level)
                           FROM %(ingest_path_file)s
                           DELIMITER ','
                           CSV HEADER""",
                        {'ingest_path_file': ingestPathFile})

            # Commit ingest
            conn.commit()

            # Run update 
            cur.execute("""UPDATE drf_harvest_data_file_meta
                           SET ingested = True
                           WHERE file_name = %(update_file)s
                           """,
                        {'update_file': ingestFile})

            # Commit update 
            conn.commit()

            # Close cursor and database connection
            cur.close()
            conn.close()

        # If exception print error
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

# This function takes not input, and creates the drf_gauge_station_source_data view.
def createView():
    try:
        # Create connection to database and get cursor
        conn = psycopg2.connect("dbname='apsviz_gauges' user='apsviz_gauges' host='localhost' port='5432' password='apsviz_gauges'")
        cur = conn.cursor()

        # Set enviromnent
        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")

        # Run query
        cur.execute("""CREATE VIEW drf_gauge_station_source_data AS
                       SELECT d.obs_id AS obs_id,
                              s.source_id AS source_id,
                              g.station_id AS station_id,
                              g.station_name AS station_name,
                              d.timemark AS timemark,
                              d.time AS time,
                              d.water_level AS water_level,
                              g.tz AS tz,
                              g.gauge_owner AS gauge_owner,
                              s.data_source AS data_source,
                              s.source_name AS source_name,
                              s.source_archive AS source_archive,
                              g.location_name AS location_name,
                              g.location_type AS location_type,
                              g.country AS country,
                              g.state AS state,
                              g.county AS county,
                              g.geom AS geom
                       FROM drf_gauge_data d
                       INNER JOIN drf_gauge_source s ON s.source_id=d.source_id
                       INNER JOIN drf_gauge_station g ON s.station_id=g.station_id""")

        # Commit ingest
        conn.commit()

        # Close cursor and database connection
        cur.close()
        conn.close()

    # If exception print error
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

# Main program function takes args as input, which contains the inputDir, ingestDir, inputTask, and inputDataset values.
@logger.catch
def main(args):
    # Add logger
    logger.remove()
    log_path = os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs'))
    logger.add(log_path+'/ingestData.log', level='DEBUG')

    # Extract args variables
    inputDir = args.inputDir
    ingestDir = args.ingestDir
    inputTask = args.inputTask
    inputDataset = args.inputDataset

    # Check if inputTask if file, station, source, data or view, and run appropriate function
    if inputTask.lower() == 'file':
        logger.info('Ingesting input file information.')
        ingestHarvestDataFileMeta(inputDir, ingestDir)
        logger.info('Ingested input file information.')
    elif inputTask.lower() == 'station':
        logger.info('Ingesting station data.')
        ingestStation(inputDir, ingestDir)
        logger.info('Ingested station data.')
    elif inputTask.lower() == 'source':
        logger.info('Ingesting source data.')
        ingestSource(inputDir, ingestDir)
        logger.info('ingested source data.')
    elif inputTask.lower() == 'data':
        logger.info('Ingesting data for dataset '+inputDataset+'.')
        ingestData(ingestDir, inputDataset)
        logger.info('Ingested data for dataset '+inputDataset+'.')
    elif inputTask.lower() == 'view':
        logger.info('Creating view.')
        createView()
        logger.info('Created view.')

# Run main function takes inputDir, ingestDir, inputTask, and inputDataset as input.
if __name__ == "__main__":
    """ This is executed when run from the command line """
    parser = argparse.ArgumentParser()

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--inputDir", action="store", dest="inputDir")
    parser.add_argument("--ingestDir", action="store", dest="ingestDir")
    parser.add_argument("--inputTask", action="store", dest="inputTask")
    parser.add_argument("--inputDataset", action="store", dest="inputDataset")

    # Parse arguments
    args = parser.parse_args()

    # Run main
    main(args)

