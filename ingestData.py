#!/usr/bin/env python
# coding: utf-8

# Import Python modules
import argparse, glob, os, psycopg2
from loguru import logger

def createStationIngest(inputDir, outputDir):
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
            cur.execute("""COPY drf_gauge_station(station_name,lat,lon,tz,gauge_owner,location_name,country,state,county,geom)
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

def createSourceIngest(inputDir, outputDir):
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
            cur.execute("""COPY drf_gauge_source(station_id,data_source,units,source_name,source_archive)
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

def createDataIngest(inputDir, outputDir, inputDataset):
    inputFiles = glob.glob(inputDir+"data_"+inputDataset+"_*.csv")

    for dataFile in inputFiles:
        outPathFile = outputDir+dataFile.split('/')[-1]

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
                              s.units AS units,
                              g.tz AS tz,
                              g.gauge_owner AS gauge_owner,
                              s.data_source AS data_source,
                              s.source_name AS source_name,
                              s.source_archive AS source_archive,
                              g.location_name AS location_name,
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

    if inputTask.lower() == 'station':
        logger.info('Creating station ingest script.')
        createStationIngest(inputDir, outputDir)
        logger.info('Created station ingest script.')
    elif inputTask.lower() == 'source':
        logger.info('Creating source ingest script.')
        createSourceIngest(inputDir, outputDir)
        logger.info('Created source ingest script..')
    elif inputTask.lower() == 'data':
        logger.info('Creating data ingest script for dataset '+inputDataset+'.')
        createDataIngest(inputDir, outputDir, inputDataset)
        logger.info('Created data ingest script for dataset '+inputDataset+'.')
    elif inputTask.lower() == 'view':
        logger.info('Creating script to create database view.')
        createView()
        logger.info('Created script to create database view.')

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

