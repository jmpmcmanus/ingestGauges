#!/usr/bin/env python
# coding: utf-8

# Import Python modules
import argparse, glob, sys, os, psycopg2, us, pdb
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

def getNOAAStations():
    try:
        # Create connection to database and get cursor
        conn = psycopg2.connect("dbname='apsviz_gauges' user='apsviz_gauges' host='localhost' port='5432' password='apsviz_gauges'")
        cur = conn.cursor()

        # Set enviromnent
        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")

        # Run query
        cur.execute("""SELECT station_name, lat, lon, name FROM noaa_stations 
                       ORDER BY station_name""")

        # convert query output to Pandas dataframe
        df = pd.DataFrame(cur.fetchall(), columns=['station_name', 'lat', 'lon', 'name'])

        # Close cursor and database connection
        cur.close()
        conn.close()

        # return first row
        return(df)

    # If exception print error
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def getNCEMStations(locationType):
    try:
        # Create connection to database and get cursor
        conn = psycopg2.connect("dbname='apsviz_gauges' user='apsviz_gauges' host='localhost' port='5432' password='apsviz_gauges'")
        cur = conn.cursor()

        # Set enviromnent
        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")

        # Run query
        if locationType == 'coastal':
            cur.execute("""SELECT site_id, latitude, longitude, owner, name, county FROM dbo_gages_all
                           WHERE is_coastal = 1 AND owner != 'NOAA' AND latitude IS NOT NULL
                           ORDER BY site_id""")
        elif locationType == 'rivers':
            cur.execute("""SELECT site_id, latitude, longitude, owner, name, county FROM dbo_gages_all
                           WHERE is_coastal = 0 AND owner != 'NOAA' AND latitude IS NOT NULL
                           ORDER BY site_id""")
        else:
            sys.exit('Incorrect station type')

        # convert query output to Pandas dataframe
        df = pd.DataFrame(cur.fetchall(), columns=['site_id', 'latitude', 'longitude', 'owner', 'name', 'county'])

        # Close cursor and database connection
        cur.close()
        conn.close()

        # return first row
        return(df)

    # If exception print error
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
            

# This function queriers the original noaa station table extracting station information, and  
# returns a dataframe. It uses the information from the table along with Nominatim 
# and ZipCodeDatabase to generate and address from latitude and longitude values. 
def addNOAAMeta(locationType):
    # Create instance of Nominatim, and ZipCodeDatabase
    geolocator = Nominatim(user_agent="geoapiExercises")
    zcdb = ZipCodeDatabase()

    # Get station data from original NOAA station table 
    df = getNOAAStations()
 
    # Define list for input into new columns 
    country = []
    state = []
    county = []
    geom = []

    # Loop through dataframe
    for index, row in df.iterrows():
        # Get longitude and latitude values 
        lon = row['lon']
        lat = row['lat']
       
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
    
    df['gauge_owner'] = 'NOAA/NOS'
    df['location_type'] = locationType 
    df['tz'] = 'gmt'
    df['country'] = country
    df['state'] = state
    df['county'] = county
    df['geom'] = geom
    df.columns= df.columns.str.lower()
    #df = df.rename(columns={'station': 'station_name'})
    df = df.rename(columns={'name': 'location_name'})
    newColsOrder = ["station_name","lat","lon","tz","gauge_owner","location_name","location_type","country","state","county","geom"]
    df=df.reindex(columns=newColsOrder)
    
    return(df)

def addNCEMMeta(locationType):
    # NEED TO GET THIS INFORMATION FROM CONTRAILS INSTEAD OF RELYING ON AN EXCEL FILE
    df = getNCEMStations(locationType.lower())

    df = df.rename(columns={'latitude':'lat','longitude':'lon','site_id':'station_name',
                            'name':'location_name','owner': 'gauge_owner'})
    df.columns= df.columns.str.lower()
    df['tz'] = 'gmt'
    df['location_type'] = locationType
    df['country'] = 'us'
    df['state'] = 'nc'
    newColsOrder = ["station_name","lat","lon","tz","gauge_owner","location_name","location_type","country","state","county"]
    df=df.reindex(columns=newColsOrder)
    df.reset_index(drop=True, inplace=True)
    
    geom = []
    
    for index, row in df.iterrows():
        geom.append(getGeometry(row['lon'], row['lat']))
        
    df['geom'] = geom
    
    return(df)

# Main program function takes args as input, which contains the outputDir, and outputFile values.  
@logger.catch
def main(args):
    # Add logger
    logger.remove()
    log_path = os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs'))
    logger.add(log_path+'/createIngestStationMeta.log', level='DEBUG')

    # Extract args variables
    outputDir = args.outputDir
    outputFile = args.outputFile

    # Get dataset and location type
    dataset = outputFile.split('_')[0]
    locationType = outputFile.split('_')[2]

    if dataset == 'noaa':
        # If dataset is noaa run the addNOAAMeta function and write output to csv file 
        logger.info('Start processing NOAA stations.')
        df = addNOAAMeta(locationType)
        df.to_csv(outputDir+'geom_'+outputFile, index=False) 
        logger.info('Finnished processing NOAA stations.')
    elif dataset == 'contrails':
        # If dataset is contrails run the addNCEMMeta function and write output to csv file
        logger.info('Start processing Contrails stations.')
        df = addNCEMMeta(locationType)
        df.to_csv(outputDir+'geom_'+outputFile, index=False)
        logger.info('Finished processing Contrails stations.')
    else:
        # If dataset is not noaa or contrails exit program with error message.
        logger.add(lambda _: sys.exit(1), level="ERROR")

# Run main function takes outputDir, and outputFile as input. 
if __name__ == "__main__":
    """ This is executed when run from the command line """
    parser = argparse.ArgumentParser()

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--outputDir", action="store", dest="outputDir")
    parser.add_argument("--outputFile", action="store", dest="outputFile")

    args = parser.parse_args()
    main(args)

