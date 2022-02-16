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

# This function takes not input, and returns a DataFrame that contains a list of NOAA stations that it extracted from the noaa_stations
# table. The data in the noaa_stations table was obtained from NOAA's api.tidesandcurrents.noaa.gov API.
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

        # Return DataFrame 
        return(df)

    # If exception print error
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

# This function takes a gauge location type (COASTAL, TIDAL or RIVERS) as input, and returns a DataFrame that contains a list of NCEM stations 
# that are extracted from the dbo_gages_all table. The dbo_gages_all table contains data from an Excel file (dbo_GAGES_ALL.xlsx) that was 
# obtained from Tom Langan at NCEM.
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

        # Return DataFrame 
        return(df)

    # If exception print error
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
            

# This function queriers the original NOAA station table (noaa_stations), using the getNOAAStations function, 
# extracting station information, and returns a dataframe. It uses the information from the table along with  
# Nominatim and ZipCodeDatabase to generate and address from latitude and longitude values. 
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
        
        # Check if address is in the US, if it is get county and state information. If it is not use blank string for county and state 
        # information. 
        if country_code == 'us':
            try:
                # Extract zipcode address using the ZipCodeDatabase instance, by inputing the zipcode from 
                # address extracted from Nominatim instance. The state and country values from the new zipcode 
                # address are appended to the state and country list
                zipcode = zcdb[address.get('postcode', '').strip().split('-')[0]]
                state.append(zipcode.state.lower())
                county.append(address.get('county', '').replace('County', '').strip())
            except:
                # If there is an exception get state information from the us module
                # NEED TO TAKE A CLOSER LOOK AT THIS, AND SEE IF I CAN USE AN IF STATEMENT TO FIND THE PROBLEM, INSTEAD OF USING EXCEPTION
                stateinfo = us.states.lookup(address.get('state', '').strip())
                try:
                    # Append state name and county name to the state and county variables 
                    state.append(stateinfo.abbr.lower())
                    county.append(address.get('county', '').replace('County', '').strip())
                except:
                    # If there is an exception check county information to see if county is Lajas, and if not check to see if county is Mayagüez,
                    # and if not define county as blank string 
                    # NEED TO TAKE A CLOSER LOOK AT THIS, AND SEE IF I CAN USE AN IF STATEMENT TO FIND THE PROBLEM, INSTEAD OF USING EXCEPTION 
                    countyname = address.get('county', '').replace('County', '').strip()
                    
                    # If countyname is Lajas define state as pr
                    if countyname == 'Lajas':
                        state.append('pr')
                        county.append(countyname)
                    else:
                        # Else if county is not Lajas, check to see if city is Mayagüez, and if it is define state as pr, and append city to county.
                        # If city is not Mayagüez, then append blank string for state.
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
       
        # Append geometry to geom variable 
        geom.append(getGeometry(lon, lat))
   
    # Add meta to DataFrame 
    df['gauge_owner'] = 'NOAA/NOS'
    df['location_type'] = locationType 
    df['tz'] = 'gmt'
    df['country'] = country
    df['state'] = state
    df['county'] = county
    df['geom'] = geom
    df.columns= df.columns.str.lower()
    df = df.rename(columns={'name': 'location_name'})

    # Reorder columns in DataFrame
    newColsOrder = ["station_name","lat","lon","tz","gauge_owner","location_name","location_type","country","state","county","geom"]
    df=df.reindex(columns=newColsOrder)
   
    # Return DataFrame 
    return(df)

# This function queriers the original NCEM station table (db_gages_all), using the getNCEMStations function,
# extracting station information, and returns a dataframe. 
def addNCEMMeta(locationType):
    # Run the getNCEMStation, which outputs a DataFrame the contains a list of NCEM stations queried from the 
    # db_gages_all table, which contains the original NCEM station meta data. 
    df = getNCEMStations(locationType.lower())

    # Rename columns 
    df = df.rename(columns={'latitude':'lat','longitude':'lon','site_id':'station_name',
                            'name':'location_name','owner': 'gauge_owner'})
    
    # Convert all column name to lower case
    df.columns= df.columns.str.lower()

    # Add variables to DataFrame
    df['tz'] = 'gmt'
    df['location_type'] = locationType
    df['country'] = 'us'
    df['state'] = 'nc'

    # Reorder column names and reset index values
    newColsOrder = ["station_name","lat","lon","tz","gauge_owner","location_name","location_type","country","state","county"]
    df=df.reindex(columns=newColsOrder)
    df.reset_index(drop=True, inplace=True)
   
    # Define geometry variable 
    geom = []

    # Loop from the DataFrame of stations, and use the lon and lat values to get the geomtry values, using the getGeometry function    
    for index, row in df.iterrows():
        geom.append(getGeometry(row['lon'], row['lat']))
       
    # Add geometry value to DataFrame 
    df['geom'] = geom
   
    # Return DataFrame 
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

    # Check if dataset is noaa, contrails
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

    # Parse arguments
    args = parser.parse_args()

    # Run main
    main(args)

