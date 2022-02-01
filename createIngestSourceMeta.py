#!/usr/bin/env python
# coding: utf-8

# Import python modules
import psycopg2, sys
import pandas as pd
from psycopg2.extensions import AsIs

# This function takes a list of station names as input, and uses them to query the apsviz_gauges database, and return a list
# of station id(s).
def getStationID(station_tuples):
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
                       WHERE station_name IN %(stationtuples)s
                       ORDER BY station_name""", 
                    {'stationtuples': AsIs(station_tuples)})
       
        # convert query output to Pandas dataframe 
        dfstations = pd.DataFrame(cur.fetchall(), columns=['station_id', 'station_name'])

        # Close cursor and database connection
        cur.close()
        conn.close()

        # Return Pandas dataframe
        return(dfstations)

    # If exception print error
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

# This function takes a input a directory path and filename, and used them to read the input file
# and add station_id(s) that are extracted from the apsviz_gauges database.
def addMeta(dirinpath, filename):
    # Read inputfile to a Pandas dataframe, convert column names to lower case, rename the station 
    # column to station_name, and drop columns that are not required
    df = pd.read_csv(dirinpath+filename)
    df.columns= df.columns.str.lower()
    df = df.rename(columns={'station': 'station_name'})
    # DID NOT INCLUDE COUNTY BECAUSE IT DOES NOT EXIST IN THE CONTRAIL FILES, BUT THIS MAY CHANGE
    df.drop(columns=['lat','lon','tz','owner','state','county'], inplace=True)


    # Extract list of stations from dataframe for query database using the getStationID function,
    # drop station_name column, and then add station_id(s) extracted from database to dataframe 
    station_tuples = tuple(sorted([str(x) for x in df['station_name'].unique().tolist()]))
    dfstations = getStationID(station_tuples)
    df['station_name'] = df['station_name'].astype(str)
    for index, row in dfstations.iterrows():
        df.loc[df['station_name'] == row['station_name'], 'station_id'] = row['station_id']

    df['station_id'] = df['station_id'].astype(int) 
    df.drop(columns=['station_name'], inplace=True)

    # Get source name from filename
    source = filename.split('_')[0]

    # Check if source is ADCIRC
    if source == 'adcirc':
        # DROP COUNTY MAY CHANGE IN THE FUTURE, WHERE IS WOULD BE DONE ABOVE
        #df.drop(columns=['county'], inplace=True)
        # Get source_name and data_source from filename, and add them to the dataframe along
        # with the source_archive value
        df['source_name'] = source
        df = df.rename(columns={'name': 'data_source'})
        #df['data_source'] = filename.split('_')[4].lower()
        df['source_archive'] = 'renci'
    elif source == 'contrails':
        # Drop name column
        df.drop(columns=['name'], inplace=True)
        # Add data_source, source_name, and source_archive to dataframe
        gtype = filename.split('_')[3].lower()
        df['data_source'] = gtype+'_gauge'
        df['source_name'] = 'ncem'
        df['source_archive'] = source
    elif source == 'noaa':
        # DROP COUNTY MAY CHANGE IN THE FUTURE, WHERE IS WOULD BE DONE ABOVE
        #df.drop(columns=['county'], inplace=True)
        # Drop name column
        df.drop(columns=['name'], inplace=True)
        # Add data_source, source_name, and source_archive to dataframe
        df['data_source'] = 'tidal_gauge'
        df['source_name'] = source
        df['source_archive'] = source
    else:
        # If source in incorrect print message and exit
        sys.exit('Incorrect source')
   
    # Reorder column name and update indeces 
    newColsOrder = ['station_id','data_source','units','source_name','source_archive']
    df=df.reindex(columns=newColsOrder)

    # Return dataframe 
    return(df)

# Define directory path
dirinpath = '/projects/ees/TDS/DataHarvesting/SIMULATED_DAILY_HARVESTING/'
diroutpath = '/projects/ees/TDS/DataIngesting/SIMULATED_DAILY_INGEST/'

# Define filename for the adcirc forecast data, and use it to run the addMeta function above, and
# write dataframe that is returned to a csv file
filename = 'adcirc_stationdata_meta_forecast_HSOFS_2022-01-07T00:00:00.csv'
df = addMeta(dirinpath,filename)
df.to_csv(diroutpath+'source_'+filename, index=False)

# Define filename for the adcirc nowcast data, and use it to run the addMeta function above, and
# write dataframe that is returned to a csv file
filename = 'adcirc_stationdata_meta_nowcast_HSOFS_2022-01-09T00:00:00.csv'
df = addMeta(dirinpath,filename)
df.to_csv(diroutpath+'source_'+filename, index=False)

# Define filename for the contrails data, and use it to run the addMeta function above, and
# write dataframe that is returned to a csv file
filename = 'contrails_stationdata_meta_COASTAL_2022-01-09T00:00:00.csv'
df = addMeta(dirinpath,filename)
df.to_csv(diroutpath+'source_'+filename, index=False)

filename = 'contrails_stationdata_meta_RIVERS_2022-01-09T00:00:00.csv'
df = addMeta(dirinpath,filename)
df.to_csv(diroutpath+'source_'+filename, index=False)

# Define filename for the noaa data, and use it to run the addMeta function above, and
# write dataframe that is returned to a csv file
filename = 'noaa_stationdata_meta_2022-01-09T00:00:00.csv'
df = addMeta(dirinpath,filename)
df.to_csv(diroutpath+'source_'+filename, index=False)

