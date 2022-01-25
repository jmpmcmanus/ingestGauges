#!/usr/bin/env python
# coding: utf-8

# Import python modules
import glob, psycopg2
import pandas as pd
import numpy as np
from psycopg2.extensions import AsIs

# This function takes as input the source_archive (noaa, contrails), and a list of station_id(s), and returnst source_id(s) for 
# observation data from the gauge_source table in the apsviz_gauges database. This funciton specifically gets source_id(s) for
# observation data, such as from NOAA and NCEM.
def getObsSourceID(source_archive,station_tuples):
    try:
        # Create connection to database and get cursor
        conn = psycopg2.connect("dbname='apsviz_gauges' user='apsviz_gauges' host='localhost' port='5432' password='apsviz_gauges'")
        cur = conn.cursor()
       
        # Set enviromnent 
        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")
       
        # Run query 
        cur.execute("""SELECT s.source_id AS source_id, g.station_id AS station_id, g.station_name AS station_name,
                       s.data_source AS data_source, s.source_name AS source_name
                       FROM drf_gauge_station g INNER JOIN drf_gauge_source s ON s.station_id=g.station_id
                       WHERE source_archive = %(sourcearchive)s AND station_name IN %(stationtuples)s""", 
                    {'sourcearchive': source_archive,'stationtuples': AsIs(station_tuples)})

        # convert query output to Pandas dataframe
        dfstations = pd.DataFrame(cur.fetchall(), columns=['source_id','station_id', 'station_name',
                                                           'data_source','source_name'])

        # Close cursor and database connection
        cur.close()
        conn.close()

        # Return Pandas dataframe
        return(dfstations)

    # If exception print error
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

# This function takes as input the data_source (hsofs...), and a list of station_id(s), and returns source_id(s) for    
# model data from the gauge_source table in the apsviz_gauges database. This funciton specifically gets source_id(s) for
# model data, such as from ADCIRC. The data_source, such is hsofs, is the grid that is used in the ADCIRC run.
def getModelSourceID(source_name,data_source,station_tuples):
    try:
        # Create connection to database and get cursor
        conn = psycopg2.connect("dbname='apsviz_gauges' user='apsviz_gauges' host='localhost' port='5432' password='apsviz_gauges'")
        cur = conn.cursor()

        # Set enviromnent 
        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")

        # Run query
        cur.execute("""SELECT s.source_id AS source_id, g.station_id AS station_id, g.station_name AS station_name,
                       s.data_source AS data_source, s.source_name AS source_name
                       FROM drf_gauge_station g INNER JOIN drf_gauge_source s ON s.station_id=g.station_id
                       WHERE data_source = %(datasource)s AND source_name = %(sourcename)s 
                       AND station_name IN %(stationtuples)s""", 
                    {'datasource': data_source,'sourcename': source_name, 'stationtuples': AsIs(station_tuples)})

        # convert query output to Pandas dataframe
        dfstations = pd.DataFrame(cur.fetchall(), columns=['source_id','station_id','station_name','data_source',
                                                           'source_name'])
   
        # Close cursor and database connection 
        cur.close()
        conn.close()

        # Return Pandas dataframe 
        return(dfstations)

    # If exception print error
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

# This function takes as input a directory input path, directory output path and a filename, and returns a csv file
# file that containes gauge data. the function uses the getObsSourceID and getModelSourceID functions above to get
# a list of existing source ids that it includes in the gauge data to enable joining the gauge data table with 
# gauge source table. The function adds a timemark, that it gets from the input file name. The timemark values can
# be used to uniquely query an ADCIRC  model run.
def addMeta(dirinpath, diroutpath, filename):
    # Read input file, convert column name to lower case, rename station column to station_name, convert its data 
    # type to string, and add timemark and source_id columns
    df = pd.read_csv(dirinpath+filename)
    df.columns= df.columns.str.lower()
    df = df.rename(columns={'station': 'station_name'})
    df = df.astype({"station_name": str})
    df.insert(0,'timemark', '')
    df.insert(0,'source_id', '')
   
    # Extract list of stations from dataframe for query database, and get source_archive name from filename.
    station_tuples = tuple([str(x) for x in df['station_name'].unique().tolist()])
    source_archive = filename.split('_')[0].strip()

    # check if source archive name is ADCIRC
    if source_archive == 'adcirc':
        # Get soure_name and data_source from filename, and use it along with the list of stations to run
        # the getModelSourceID function to get the sourc_id(s)
        source_name = source_archive+'_'+filename.split('_')[2].lower().strip()
        data_source = filename.split('_')[3].lower().strip()
        dfstations = getModelSourceID(source_name,data_source,station_tuples)
       
        # Check source name, and use it to get the appropiate timemark for the forecast and nowecast data 
        if source_name == 'adcirc_forecast':
            df['timemark']  = filename.split('_')[5].split('.')[0].lower().strip()
        elif source_name == 'adcirc_nowcast':
            df['timemark']  = filename.split('_')[4].lower().strip()
        else:
            sys.exit('Incorrect source name.')
            
    else:
        # Use source_archive and list of stations to get source_id(s) for the observation gauge data
        dfstations = getObsSourceID(source_archive,station_tuples)
        df['timemark'] = filename.split('_')[2].strip()

    # Add source id(s) to dataframe 
    for index, row in dfstations.iterrows():
        df.loc[df['station_name'] == row['station_name'], 'source_id'] = row['source_id']

    # Drom station_name column from dataframe
    df.drop(columns=['station_name'], inplace=True)

    # Write dataframe to csv file
    df.to_csv(diroutpath+'data_'+filename, index=False)

# This function takes as input a directory input path, a directory output path and a dataset variable. It 
# generates and list of input filenames, and uses them to run the addMeta function above.
def processData(dirinpath, diroutpath, dataset):
    filenames = glob.glob(dirinpath+dataset+"*.csv")
    
    for dirinfile in filenames:
        filename = dirinfile.split('/')[-1]
        if filename.split('_')[2] != 'meta' and filename.split('_')[2] != 'FakeVeerRight':
            #print(filename)
            addMeta(dirinpath, diroutpath, filename)
        else:
            continue

# Define directory input and output paths, and use them, along with dataset, to run the 
# processData function
dirinpath = '/Users/jmpmcman/Work/Surge/data/DataHarvesting/SIMULATED_DAILY_HARVESTING/'
diroutpath = '/Users/jmpmcman/Work/Surge/data/DataHarvesting/SIMULATED_DAILY_INGEST/'
processData(dirinpath, diroutpath, 'contrails_stationdata_')
processData(dirinpath, diroutpath, 'adcirc_stationdata_')
processData(dirinpath, diroutpath, 'noaa_stationdata_')

