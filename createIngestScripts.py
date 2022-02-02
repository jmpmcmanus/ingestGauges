#!/usr/bin/env python
# coding: utf-8

# Import Python modules
import argparse, glob, os
from loguru import logger

def createStationIngest(inputDir):
    inputFiles = glob.glob(inputDir+"geom_*.csv")

    if os.path.exists(inputDir+"docker_cp_apsviz_gauge_stations.sh"):
        os.remove(inputDir+"docker_cp_apsviz_gauge_stations.sh")
    else:
        print("Can not delete the file as it doesn't exists")

    f = open(inputDir+"docker_cp_apsviz_gauge_stations.sh","a")

    for geomfile in inputFiles:
       f.write("docker cp "+geomfile.strip()+" apsviz-timeseriesdb-refine_db_1:/home/\n")
 
    f.close()

    os.chmod(inputDir+"docker_cp_apsviz_gauge_stations.sh", 0o755)

    if os.path.exists(inputDir+"gauge_station_copy.sql"):
        os.remove(inputDir+"gauge_station_copy.sql")
    else:
        print("Can not delete the file as it doesn't exists")

    f = open(inputDir+"gauge_station_copy.sql","a")

    for geomfile in inputFiles:
        inputFile = geomfile.split('/')[-1]
        f.write("COPY drf_gauge_station(station_name,lat,lon,tz,gauge_owner,location_name,country,state,county,geom)\n")
        f.write("FROM '/home/"+inputFile+"'\n")
        f.write("DELIMITER ','\n")
        f.write("CSV HEADER;\n")
        f.write("\n")

    f.close()

    os.chmod(inputDir+"gauge_station_copy.sql", 0o755)

    if os.path.exists(inputDir+"psql_apsviz_gauge_station_copy.sh"):
        os.remove(inputDir+"psql_apsviz_gauge_station_copy.sh")
    else:
        print("Can not delete the file as it doesn't exists")

    f = open(inputDir+"psql_apsviz_gauge_station_copy.sh","w")
    f.write("PGPASSWORD=apsviz_gauges psql -U apsviz_gauges -d apsviz_gauges -p 5432 -h localhost -f gauge_station_copy.sql\n")
    f.close()

    os.chmod(inputDir+"psql_apsviz_gauge_station_copy.sh", 0o755)

def createSourceIngest(inputDir):
    inputFiles = glob.glob(inputDir+"source_*.csv")

    if os.path.exists(inputDir+"docker_cp_apsviz_gauge_source.sh"):
        os.remove(inputDir+"docker_cp_apsviz_gauge_source.sh")
    else:
        print("Can not delete the file as it doesn't exists")

    f = open(inputDir+"docker_cp_apsviz_gauge_source.sh","a")

    for sourcefile in inputFiles:
       f.write("docker cp "+sourcefile.strip()+" apsviz-timeseriesdb-refine_db_1:/home/\n")
 
    f.close()

    os.chmod(inputDir+"docker_cp_apsviz_gauge_source.sh", 0o755)

    if os.path.exists(inputDir+"gauge_source_copy.sql"):
        os.remove(inputDir+"gauge_source_copy.sql")
    else:
        print("Can not delete the file as it doesn't exists")

    f = open(inputDir+"gauge_source_copy.sql","a")

    for sourcefile in inputFiles:
        inputFile = sourcefile.split('/')[-1]
        f.write("COPY drf_gauge_source(station_id,data_source,units,source_name,source_archive)\n")
        f.write("FROM '/home/"+inputFile+"'\n")
        f.write("DELIMITER ','\n")
        f.write("CSV HEADER;\n")
        f.write("\n")

    f.close()

    os.chmod(inputDir+"gauge_source_copy.sql", 0o755)

    if os.path.exists(inputDir+"psql_apsviz_gauge_source_copy.sh"):
        os.remove(inputDir+"psql_apsviz_gauge_source_copy.sh")
    else:
        print("Can not delete the file as it doesn't exists")

    f = open(inputDir+"psql_apsviz_gauge_source_copy.sh","w")
    f.write("PGPASSWORD=apsviz_gauges psql -U apsviz_gauges -d apsviz_gauges -p 5432 -h localhost -f gauge_source_copy.sql\n")
    f.close()

    os.chmod(inputDir+"psql_apsviz_gauge_source_copy.sh", 0o755)
 
def createDataIngest(inputDir, inputDataset):
    inputFiles = glob.glob(inputDir+inputDataset+"_*.csv")

    dock_cp_file = "docker_cp_apsviz_gauge_"+inputDataset+".sh"

    if os.path.exists(inputDir+dock_cp_file):
        os.remove(inputDir+dock_cp_file)
    else:
        print("Can not delete the file as it doesn't exists")

    f = open(inputDir+dock_cp_file,"w")
    
    for datafile in inputFiles:
        #inputFile = datafile.split('/')[-1]
        f.write("docker cp "+datafile.strip()+" apsviz-timeseriesdb-refine_db_1:/home/\n")

    f.close()

    os.chmod(inputDir+dock_cp_file, 0o755)

    gauge_cp_file = "gauge_copy_"+inputDataset+".sql"

    if os.path.exists(inputDir+gauge_cp_file):
        os.remove(inputDir+gauge_cp_file)
    else:
        print("Can not delete the file as it doesn't exists")
 
    f = open(inputDir+gauge_cp_file,"w")
    
    for datafile in inputFiles:
        inputFile = datafile.split('/')[-1]
        f.write("COPY drf_gauge_data(source_id,timemark,time,water_level)\n")
        f.write("FROM '/home/"+inputFile+"'\n")
        f.write("DELIMITER ','\n")
        f.write("CSV HEADER;\n")
        f.write("\n")

    f.close()

    os.chmod(inputDir+gauge_cp_file, 0o755)

    psql_cp_file = "psql_apsviz_gauge_copy_"+inputDataset+".sh"

    if os.path.exists(inputDir+psql_cp_file):
        os.remove(inputDir+psql_cp_file)
    else:
        print("Can not delete the file as it doesn't exists")

    f = open(inputDir+psql_cp_file,"w")
    f.write("PGPASSWORD=apsviz_gauges psql -U apsviz_gauges -d apsviz_gauges -p 5432 -h localhost -f "+gauge_cp_file+"\n")
    f.close()

    os.chmod(inputDir+psql_cp_file, 0o755)

def createView(inputDir):
    view_cp_file = "create_gauges_station_source_data_view.sql"

    if os.path.exists(inputDir+view_cp_file):
        print("The SQL file exists, so no reason to copy it")
    else:
        os.system("cp "+view_cp_file+" "+inputDir+view_cp_file)

    psql_cp_file = "psql_apsviz_gauges_view.sh"

    if os.path.exists(inputDir+psql_cp_file):
        os.remove(inputDir+psql_cp_file)
    else:
        print("Can not delete the file as it doesn't exists")

    f = open(inputDir+psql_cp_file,"w")
    f.write("PGPASSWORD=apsviz_gauges psql -U apsviz_gauges -d apsviz_gauges -p 5432 -h localhost -f "+view_cp_file+"\n")
    f.close()

    os.chmod(inputDir+psql_cp_file, 0o755)

# Main program function takes args as input, which contains the inputDir, inputTask, and inputDataset values.
@logger.catch
def main(args):
    # Add logger
    logger.remove()
    log_path = os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs'))
    logger.add(log_path+'/createIngestScripts.log', level='DEBUG')

    # Extract args variables
    inputDir = args.inputDir
    inputTask = args.inputTask
    inputDataset = args.inputDataset

    if inputTask.lower() == 'station':
        logger.info('Creating station ingest script.')
        createStationIngest(inputDir)
        logger.info('Created station ingest script.')
    elif inputTask.lower() == 'source':
        logger.info('Creating source ingest script.')
        createSourceIngest(inputDir)
        logger.info('Created source ingest script..')
    elif inputTask.lower() == 'data':
        logger.info('Creating data ingest script for dataset '+inputDataset+'.')
        createDataIngest(inputDir, inputDataset)
        logger.info('Created data ingest script for dataset '+inputDataset+'.')
    elif inputTask.lower() == 'view':
        logger.info('Creating script to create database view.')
        createView(inputDir)
        logger.info('Created script to create database view.')

# Run main function takes inputDir, inputTask, and inputDataset as input.
if __name__ == "__main__":
    """ This is executed when run from the command line """
    parser = argparse.ArgumentParser()

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--inputDir", action="store", dest="inputDir")
    parser.add_argument("--inputTask", action="store", dest="inputTask")
    parser.add_argument("--inputDataset", action="store", dest="inputDataset")

    args = parser.parse_args()
    main(args)

