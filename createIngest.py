#!/usr/bin/env python
# coding: utf-8

# Import Python modules
import glob, os


def createDataIngest(datapath, ingestpath, dataset):
    filenames = glob.glob(datapath+dataset+"*.csv")
            
    f = open(ingestpath+"docker_cp_apsviz_gauge_data.sh","a")
    
    for datafile in filenames:
        #filename = datafile.split('/')[-1]
        f.write("docker cp "+datafile.strip()+" apsviz-timeseriesdb-refine_db_1:/home/\n")

    f.close()
    
    f = open(ingestpath+"gauge_data_copy.sql","a")
    
    for datafile in filenames:
        filename = datafile.split('/')[-1]
        f.write("COPY drf_gauge_data(source_id,timemark,time,water_level)\n")
        f.write("FROM '/home/"+filename+"'\n")
        f.write("DELIMITER ','\n")
        f.write("CSV HEADER;\n")
        f.write("\n")

    f.close()

datapath = '/Users/jmpmcman/Work/Surge/data/DataHarvesting/SIMULATED_DAILY_INGEST/'
ingestpath = '/Users/jmpmcman/Work/Surge/development/apsviz-timescale/ingest-refine/'
createDataIngest(datapath, ingestpath, 'data_contrails_stationdata_')
createDataIngest(datapath, ingestpath, 'data_adcirc_stationdata_')
createDataIngest(datapath, ingestpath, 'data_noaa_stationdata_')

