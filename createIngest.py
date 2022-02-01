#!/usr/bin/env python
# coding: utf-8

# Import Python modules
import glob, os, pdb

def createStationIngest(dirPath):
    filenames = glob.glob(dirPath+"geom_*.csv")

    if os.path.exists(dirPath+"docker_cp_apsviz_gauge_stations.sh"):
        os.remove(dirPath+"docker_cp_apsviz_gauge_stations.sh")
    else:
        print("Can not delete the file as it doesn't exists")

    f = open(dirPath+"docker_cp_apsviz_gauge_stations.sh","a")

    for geomfile in filenames:
       f.write("docker cp "+geomfile.strip()+" apsviz-timeseriesdb-db-1:/home/\n")
 
    f.close()

    os.chmod(dirPath+"docker_cp_apsviz_gauge_stations.sh", 0o755)

    if os.path.exists(dirPath+"gauge_station_copy.sql"):
        os.remove(dirPath+"gauge_station_copy.sql")
    else:
        print("Can not delete the file as it doesn't exists")

    f = open(dirPath+"gauge_station_copy.sql","a")

    for geomfile in filenames:
        filename = geomfile.split('/')[-1]
        f.write("COPY drf_gauge_station(station_name,lat,lon,tz,gauge_owner,location_name,country,state,county,geom)\n")
        f.write("FROM '/home/"+filename+"'\n")
        f.write("DELIMITER ','\n")
        f.write("CSV HEADER;\n")
        f.write("\n")

    f.close()

    os.chmod(dirPath+"gauge_station_copy.sql", 0o755)

    if os.path.exists(dirPath+"psql_apsviz_gauge_station_copy.sh"):
        os.remove(dirPath+"psql_apsviz_gauge_station_copy.sh")
    else:
        print("Can not delete the file as it doesn't exists")

    f = open(dirPath+"psql_apsviz_gauge_station_copy.sh","w")
    f.write("PGPASSWORD=apsviz_gauges psql -U apsviz_gauges -d apsviz_gauges -p 5432 -h localhost -f gauge_station_copy.sql\n")
    f.close()

    os.chmod(dirPath+"psql_apsviz_gauge_station_copy.sh", 0o755)

def createSourceIngest(dirPath):
    filenames = glob.glob(dirPath+"source_*.csv")

    if os.path.exists(dirPath+"docker_cp_apsviz_gauge_source.sh"):
        os.remove(dirPath+"docker_cp_apsviz_gauge_source.sh")
    else:
        print("Can not delete the file as it doesn't exists")

    f = open(dirPath+"docker_cp_apsviz_gauge_source.sh","a")

    for sourcefile in filenames:
       f.write("docker cp "+sourcefile.strip()+" apsviz-timeseriesdb-db-1:/home/\n")
 
    f.close()

    os.chmod(dirPath+"docker_cp_apsviz_gauge_source.sh", 0o755)

    if os.path.exists(dirPath+"gauge_source_copy.sql"):
        os.remove(dirPath+"gauge_source_copy.sql")
    else:
        print("Can not delete the file as it doesn't exists")

    f = open(dirPath+"gauge_source_copy.sql","a")

    for sourcefile in filenames:
        filename = sourcefile.split('/')[-1]
        f.write("COPY drf_gauge_source(station_id,data_source,units,source_name,source_archive)\n")
        f.write("FROM '/home/"+filename+"'\n")
        f.write("DELIMITER ','\n")
        f.write("CSV HEADER;\n")
        f.write("\n")

    f.close()

    os.chmod(dirPath+"gauge_source_copy.sql", 0o755)

    if os.path.exists(dirPath+"psql_apsviz_gauge_source_copy.sh"):
        os.remove(dirPath+"psql_apsviz_gauge_source_copy.sh")
    else:
        print("Can not delete the file as it doesn't exists")

    f = open(dirPath+"psql_apsviz_gauge_source_copy.sh","w")
    f.write("PGPASSWORD=apsviz_gauges psql -U apsviz_gauges -d apsviz_gauges -p 5432 -h localhost -f gauge_source_copy.sql\n")
    f.close()

    os.chmod(dirPath+"psql_apsviz_gauge_source_copy.sh", 0o755)
 
def createDataIngest(dirPath, dataset):
    filenames = glob.glob(dirPath+dataset+"*.csv")

    dock_cp_file = "docker_cp_apsviz_gauge_"+dataset+".sh"

    if os.path.exists(dirPath+dock_cp_file):
        os.remove(dirPath+dock_cp_file)
    else:
        print("Can not delete the file as it doesn't exists")

    f = open(dirPath+dock_cp_file,"w")
    
    for datafile in filenames:
        #filename = datafile.split('/')[-1]
        f.write("docker cp "+datafile.strip()+" apsviz-timeseriesdb-db-1:/home/\n")

    f.close()

    os.chmod(dirPath+dock_cp_file, 0o755)

    gauge_cp_file = "gauge_copy_"+dataset+".sql"

    if os.path.exists(dirPath+gauge_cp_file):
        os.remove(dirPath+gauge_cp_file)
    else:
        print("Can not delete the file as it doesn't exists")
 
    f = open(dirPath+gauge_cp_file,"w")
    
    for datafile in filenames:
        filename = datafile.split('/')[-1]
        f.write("COPY drf_gauge_data(source_id,timemark,time,water_level)\n")
        f.write("FROM '/home/"+filename+"'\n")
        f.write("DELIMITER ','\n")
        f.write("CSV HEADER;\n")
        f.write("\n")

    f.close()

    os.chmod(dirPath+gauge_cp_file, 0o755)

    psql_cp_file = "psql_apsviz_gauge_copy_"+dataset+".sh"

    if os.path.exists(dirPath+psql_cp_file):
        os.remove(dirPath+psql_cp_file)
    else:
        print("Can not delete the file as it doesn't exists")

    f = open(dirPath+psql_cp_file,"w")
    f.write("PGPASSWORD=apsviz_gauges psql -U apsviz_gauges -d apsviz_gauges -p 5432 -h localhost -f "+gauge_cp_file+"\n")
    f.close()

    os.chmod(dirPath+psql_cp_file, 0o755)

def createViewIngest(dirPath):
    view_cp_file = "create_gauges_station_source_data_view.sql"

    if os.path.exists(dirPath+view_cp_file):
        print("The SQL file exists, so no reason to copy it")
    else:
        os.system("cp "+view_cp_file+" "+dirPath+view_cp_file)

    psql_cp_file = "psql_apsviz_gauges_view.sh"

    if os.path.exists(dirPath+psql_cp_file):
        os.remove(dirPath+psql_cp_file)
    else:
        print("Can not delete the file as it doesn't exists")

    f = open(dirPath+psql_cp_file,"w")
    f.write("PGPASSWORD=apsviz_gauges psql -U apsviz_gauges -d apsviz_gauges -p 5432 -h localhost -f "+view_cp_file+"\n")
    f.close()

    os.chmod(dirPath+psql_cp_file, 0o755)

dirPath = '/projects/ees/TDS/DataIngesting/SIMULATED_DAILY_INGEST/'
#createStationIngest(dirPath)
#createSourceIngest(dirPath)
#createDataIngest(dirPath, 'data_contrails_stationdata')
#createDataIngest(dirPath, 'data_adcirc_stationdata')
#createDataIngest(dirPath, 'data_noaa_stationdata')
createViewIngest(dirPath)
