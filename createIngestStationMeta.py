#!/usr/bin/env python
# coding: utf-8

# Import Python modules
import argparse, glob, sys, os, psycopg2, us
import pandas as pd
from psycopg2.extensions import AsIs
from geopy.geocoders import Nominatim
from pyzipcode import ZipCodeDatabase
from loguru import logger

# This function takes a longitude and latitude value as input, and returns a geometry in OGC Well-Known Text (WKT)
def getGeometry(lon, lat):
    try:
        # Create connection to database and get cursor 
        conn = psycopg2.connect("dbname='apsviz_gauges' user='apsviz_gauges' host='localhost' port='5432' password='apsviz_gauges'")
        cur = conn.cursor()

        # Set enviromnent 
        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")

        # Run query 
        cur.execute("""SELECT ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), 4326)""",
                    {'longitude': AsIs(lon),  'latitude': AsIs(lat)})

        # fetch rows 
        rows = cur.fetchall()

        # Close cursor and database connection          
        cur.close()
        conn.close()

        # return first row
        return(rows[0][0])      

    # If exception print error 
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
            

# This function take a directory path, and filename as input and returns a dataframe. It uses Nominatim
# and ZipCodeDatabase to generate and address from latitude and longitude values. 
def addNOAAMeta(dirpath, filename):
    # Create instance of Nominatim, and ZipCodeDatabase
    geolocator = Nominatim(user_agent="geoapiExercises")
    zcdb = ZipCodeDatabase()

    # Read input file and drop unnecessary columns
    df = pd.read_csv(dirpath+filename)
    df.drop(columns=['UNITS','STATE', 'COUNTY'], inplace=True)
   
    # Define list for input into new columns 
    owner = []
    country = []
    state = []
    county = []
    geom = []

    # Loop through dataframe
    for index, row in df.iterrows():
        # Get owner value
        owner = row['OWNER']
       
        # Get longitude and latitude values 
        lon = row['LON']
        lat = row['LAT']
       
        # Get location address using latitude and longitude values
        location = geolocator.reverse(str(lat)+","+str(lon))
        address = location.raw['address']

        # Extract county_code and counrty from address
        country_code = address.get('country_code', '').strip()
        country.append(country_code)
        

        # Check if address is in the US
        if country_code == 'us':
            try:
                # Extract zipcode address using the ZipCodeDatabase instance, by inputing the zipcode from 
                # address extracted from Nominatim instance. The state and country values from the new zipcode 
                # address are appended to the state and country list
                zipcode = zcdb[address.get('postcode', '').strip().split('-')[0]]
                state.append(zipcode.state.lower())
                county.append(address.get('county', '').replace('County', '').strip())
            except:
                stateinfo = us.states.lookup(address.get('state', '').strip())
                try:
                    state.append(stateinfo.abbr.lower())
                    county.append(address.get('county', '').replace('County', '').strip())
                except:
                    countyname = address.get('county', '').replace('County', '').strip()
                    if countyname == 'Lajas':
                        state.append('pr')
                        county.append(countyname)
                    else:
                        city = address.get('city', '').strip()
                        if city == 'Mayagüez':
                            state.append('pr')
                            county.append(city)
                        else:
                            state.append('')
                            county.append(countyname)
                            print(stateinfo)
                            print(address)
        else:
            statename = address.get('state', '').strip()
            if len(statename) > 2:
                state.append('')
            else:
                state.append(address.get('state', '').strip())
                
            county.append(address.get('county', '').replace('County', '').strip())
        
        geom.append(getGeometry(lon, lat))
    
    #sourcePrefix = filename.split('_')[0]
    
    df = df.rename(columns={'OWNER': 'gauge_owner'})

    df['country'] = country
    df['state'] = state
    df['county'] = county
    df['geom'] = geom
    df.columns= df.columns.str.lower()
    df = df.rename(columns={'station': 'station_name'})
    df = df.rename(columns={'name': 'location_name'})
    newColsOrder = ["station_name","lat","lon","tz","gauge_owner","location_name","country","state","county","geom"]
    df=df.reindex(columns=newColsOrder)
    
    return(df)

def addNCEMMeta(dirpath, contrailfile, adcircfile):
    # NEED TO GET THIS INFORMATION FROM CONTRAILS INSTEAD OF RELYING ON AN EXCEL FILE
    df = pd.read_excel('/projects/ees/TDS/ingest/FIMAN_NCEM/FIMAN_NCEM_Coastal.xlsx')
    fdf = df.loc[df['IS_COASTAL'] == 1]
    fdf = fdf.loc[fdf['IN_SERVICE'] == 1]
    fdf.drop(fdf[fdf['OWNER'] == 'NOAA'].index, inplace = True)

    cdf = pd.read_csv(dirpath+contrailfile)
    for index, row in cdf.iterrows():
        check1 = fdf.loc[fdf['SITE_ID'] == row['STATION']]
        if len(check1.index) == 0:
            check2 = df.loc[df['SITE_ID'] == row['STATION']]
            if len(check2.index) == 1:
                fdf = fdf.append(check2, ignore_index=True)

    cdf = pd.read_csv(dirpath+adcircfile)
    for index, row in cdf.iterrows():
        if len(row['STATION']) < 7:
            check1 = fdf.loc[fdf['SITE_ID'] == row['STATION']]
            if len(check1.index) == 0:
                check2 = df.loc[df['SITE_ID'] == row['STATION']]
                if len(check2.index) == 1:
                    fdf = fdf.append(check2, ignore_index=True)

    fdf.drop(columns=['RAIN_ONLY_GAGE','IN_SERVICE','IS_COASTAL'], inplace=True)
    fdf = fdf.rename(columns={'LATITUDE':'lat','LONGITUDE':'lon','SITE_ID':'station_name',
                              'NAME':'location_name','OWNER': 'gauge_owner'})
    fdf.columns= fdf.columns.str.lower()
    fdf['tz'] = 'gmt'
    fdf['country'] = 'us'
    fdf['state'] = 'nc'
    newColsOrder = ["station_name","lat","lon","tz","gauge_owner","location_name","country","state","county"]
    fdf=fdf.reindex(columns=newColsOrder)
    fdf.reset_index(drop=True, inplace=True)
    
    geom = []
    
    for index, row in fdf.iterrows():
        geom.append(getGeometry(row['lon'], row['lat']))
        
    fdf['geom'] = geom
    
    return(fdf)

# Main program function takes args as input, which contains the inputDir, outputDir, and inputFile values.  
@logger.catch
def main(args):
    # Add logger
    logger.remove()
    log_path = os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs'))
    logger.add(log_path+'/ingestIngestStationMeta.log', level='DEBUG')

    # Extract args variables
    inputDir = args.inputDir
    outputDir = args.outputDir
    inputFile = args.inputFile

    # Get dataset
    dataset = inputFile.split('_')[0]

    if dataset == 'noaa':
        # If dataset is noaa run the addNOAAMeta function and write output to csv file 
        logger.info('Start processing NOAA stations.')
        df = addNOAAMeta(inputDir,inputFile)
        df.to_csv(outputDir+'geom_'+inputFile, index=False) 
        logger.info('Finnished processing NOAA stations.')
    elif dataset == 'contrails':
         # If dataset is contrails run the addNCEMMeta function and write output to csv file
        logger.info('Start processing Contrails stations.')
        # NEED TO GET THIS INFORMATION FROM CONTRAILS DIRECTLY. SHOULD KNOW WHAT STATIONS ARE GOING TO BE USED 
        # INDEPENDENTLY OF JEFF 
        adcircFile = glob.glob(inputDir+'adcirc_stationdata_meta_*_*_'+inputFile.split('_')[-1])[0].split('/')[-1]
        df = addNCEMMeta(inputDir, inputFile, adcircFile)
        df.to_csv(outputDir+'geom_'+inputFile, index=False)
        logger.info('Finished processing Contrails stations.')
    else:
        # If dataset is not noaa or contrails exit program with error message.
        logger.add(lambda _: sys.exit(1), level="ERROR")

# Run main function takes inputDir, outputDir, and inputFile as input. 
if __name__ == "__main__":
    """ This is executed when run from the command line """
    parser = argparse.ArgumentParser()

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--inputDir", action="store", dest="inputDir")
    parser.add_argument("--outputDir", action="store", dest="outputDir")
    parser.add_argument("--inputFile", action="store", dest="inputFile")

    args = parser.parse_args()
    main(args)

# Define directory path
#dirpath = '/projects/ees/TDS/DataHarvesting/SIMULATED_DAILY_HARVESTING/'

# Define noaa input file, run the addNOAAMeta function and write dataframed that is returned to a csv file
#noaafile = 'noaa_stationdata_meta_2022-01-07T00:00:00_2022-01-09T00:00:00.csv'

# Define the contrails and adcirc input files, run the addNOAAMeta function and write dataframed that is 
# returned to a csv file
#contrailfile = 'contrails_stationdata_meta_2022-01-07T00:00:00_2022-01-09T00:00:00.csv'
#adcircfile = 'adcirc_stationdata_meta_forecast_HSOFS_2022-01-07T00:00:00_2022-01-09T00:00:00.csv'

