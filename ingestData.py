#!/usr/bin/env python
# coding: utf-8

# Import Python modules
import argparse, glob, os, psycopg2
import pandas as pd
from loguru import logger

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
            return(df.head(40))
        else:
            return(df.head(20))

    # If exception print error
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def ingestHarvestDataFileMeta(inputDir, outputDir):
    inputFiles = glob.glob(inputDir+"harvest_files_*.csv")

    for infoFile in inputFiles:
        outPathFile = outputDir+infoFile.split('/')[-1]

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
                           FROM %(out_path_file)s
                           DELIMITER ','
                           CSV HEADER""",
                        {'out_path_file': outPathFile})

            # Commit ingest
            conn.commit()

            # Close cursor and database connection
            cur.close()
            conn.close()

        # If exception print error
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

def ingestStation(inputDir, outputDir):
    inputFiles = glob.glob(inputDir+"geom_*.csv")
 
    for geomFile in inputFiles:
        outPathFile = outputDir+geomFile.split('/')[-1]
 
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
                           FROM %(out_path_file)s
                           DELIMITER ','
                           CSV HEADER""",
                        {'out_path_file': outPathFile})

            # Commit ingest
            conn.commit()
 
            # Close cursor and database connection
            cur.close()
            conn.close()

        # If exception print error
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

def ingestSource(inputDir, outputDir):
    inputFiles = glob.glob(inputDir+"source_*.csv")

    for sourceFile in inputFiles:
        outPathFile = outputDir+sourceFile.split('/')[-1]

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
                           FROM %(out_path_file)s
                           DELIMITER ','
                           CSV HEADER""",
                        {'out_path_file': outPathFile})

            # Commit ingest
            conn.commit()

            # Close cursor and database connection
            cur.close()
            conn.close()

        # If exception print error
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

def ingestData(inputDir, outputDir, inputDataset):
    dfDirFiles = getHarvestDataFileMeta(inputDataset)

    for index, row in dfDirFiles.iterrows():
        updateFile = row[0]
        outPathFile = outputDir+'data_copy_'+updateFile

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
                           FROM %(out_path_file)s
                           DELIMITER ','
                           CSV HEADER""",
                        {'out_path_file': outPathFile})

            # Commit ingest
            conn.commit()

            # Run update 
            cur.execute("""UPDATE drf_harvest_data_file_meta
                           SET ingested = True
                           WHERE file_name = %(update_file)s
                           """,
                        {'update_file': updateFile})

            # Commit update 
            conn.commit()

            # Close cursor and database connection
            cur.close()
            conn.close()

        # If exception print error
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

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

# Main program function takes args as input, which contains the inputDir, inputTask, and inputDataset values.
@logger.catch
def main(args):
    # Add logger
    logger.remove()
    log_path = os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs'))
    logger.add(log_path+'/ingestData.log', level='DEBUG')

    # Extract args variables
    inputDir = args.inputDir
    outputDir = args.outputDir
    inputTask = args.inputTask
    inputDataset = args.inputDataset

    if inputTask.lower() == 'file':
        logger.info('Ingesting input file information.')
        ingestHarvestDataFileMeta(inputDir, outputDir)
        logger.info('Ingested input file information.')
    elif inputTask.lower() == 'station':
        logger.info('Ingesting station data.')
        ingestStation(inputDir, outputDir)
        logger.info('Ingested station data.')
    elif inputTask.lower() == 'source':
        logger.info('Ingesting source data.')
        ingestSource(inputDir, outputDir)
        logger.info('ingested source data.')
    elif inputTask.lower() == 'data':
        logger.info('Ingesting data for dataset '+inputDataset+'.')
        ingestData(inputDir, outputDir, inputDataset)
        logger.info('Ingested data for dataset '+inputDataset+'.')
    elif inputTask.lower() == 'view':
        logger.info('Creating view.')
        createView()
        logger.info('Created view.')

# Run main function takes inputDir, inputTask, and inputDataset as input.
if __name__ == "__main__":
    """ This is executed when run from the command line """
    parser = argparse.ArgumentParser()

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--inputDir", action="store", dest="inputDir")
    parser.add_argument("--outputDir", action="store", dest="outputDir")
    parser.add_argument("--inputTask", action="store", dest="inputTask")
    parser.add_argument("--inputDataset", action="store", dest="inputDataset")

    args = parser.parse_args()
    main(args)

