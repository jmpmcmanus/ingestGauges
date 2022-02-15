#!/usr/bin/env python
# coding: utf-8

# Import python modules
import argparse, glob, sys, os, datetime, psycopg2, pdb
import pandas as pd
from psycopg2.extensions import AsIs
from loguru import logger

# This function queries the harvest_gauge_data_file table using a file_name, an pulls out the 
# file_name, and file_date_time, if the file_name exists in the table.
def getFileDateTime(inputFile):
    try:
        # Create connection to database and get cursor
        conn = psycopg2.connect("dbname='apsviz_gauges' user='apsviz_gauges' host='localhost' port='5432' password='apsviz_gauges'")
        cur = conn.cursor()

        # Set enviromnent
        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")

        # Run query
        cur.execute("""SELECT dir_path, file_name, file_date_time, ingested, version, overlap_past_file_date_time
                       FROM drf_harvest_data_file_meta
                       WHERE file_name = %(input_file)s
                       ORDER BY file_date_time""",
                    {'input_file': inputFile})

        # convert query output to Pandas dataframe
        df = pd.DataFrame(cur.fetchall(), columns=['dir_path', 'file_name', 'file_date_time', 'ingested', 'version', 'overlap_past_file_date_time'])

        # Close cursor and database connection
        cur.close()
        conn.close()

        return(df)

    # If exception print error
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

# This function takes a input a directory path and outputFile, and used them to read the input file
# and add station_id(s) that are extracted from the apsviz_gauges database.
def createFileList(inputDir, inputDataset):
    dirInputFiles = glob.glob(inputDir+inputDataset+"*.csv")
   
    dirOutputFiles = []
 
    for dirInputFile in dirInputFiles:
        if dirInputFile.find('meta') == -1:
            #print(dirInputFile)
            dirOutputFiles.append(dirInputFile)
        else:
            continue

    outputList = []

    for dirOutputFile in dirOutputFiles:
        dir_path = dirOutputFile.split(inputDataset)[0]
        file_name = dirOutputFile.split('/')[-1] 
        data_date_time = file_name.split('_')[-1].split('.')[0]

        checkFile = getFileDateTime(file_name)
        checked_file = checkFile.count()
        #pdb.set_trace()
        if checked_file['file_date_time'] > 0:
            version = checkFile['version'][-1]+1
            #print('There is another file') 
        elif checked_file['file_date_time'] == 0:
            version = 1
            #print('There is not another file') 
        else:
            sys.exit('Somethings wrong with the query!')

        df = pd.read_csv(dirOutputFile)
        data_begin_time = df['TIME'].min()
        data_end_time = df['TIME'].max()

        file_date_time = datetime.datetime.fromtimestamp(os.path.getctime(dirOutputFile))
        source = inputDataset.split('_')[0]

        if source == 'adcirc': 
            content_info = file_name.split('_')[2]+'_'+file_name.split('_')[3]
        elif source == 'contrails':
            content_info = file_name.split('_')[2]
        elif source == 'noaa':
            content_info = 'None'

        ingested = 'False'
        overlap_past_file_date_time = 'False'

        outputList.append([dir_path,file_name,data_date_time,data_begin_time,data_end_time,file_date_time,source,content_info,ingested,version,overlap_past_file_date_time]) 

    df = pd.DataFrame(outputList, columns=['dir_path', 'file_name', 'data_date_time', 'data_begin_time', 'data_end_time', 'file_date_time', 'source', 'content_info', 'ingested', 'version', 'overlap_past_file_date_time'])

    first_time = df['data_date_time'][0]
    last_time = df['data_date_time'].iloc[-1] 

    return(df, first_time, last_time)

# Main program function takes args as input, which contains the outputDir, and outputFile values.
@logger.catch
def main(args):
    # Add logger
    logger.remove()
    log_path = os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs'))
    logger.add(log_path+'/createHarvestFileMeta.log', level='DEBUG')

    # Extract args variables
    inputDir = args.inputDir
    outputDir = args.outputDir
    inputDataset = args.inputDataset

    logger.info('Start processing source data for dataset '+inputDataset+'.')
    df, first_time, last_time = createFileList(inputDir, inputDataset)
    current_date = datetime.date.today()
    outputFile = 'harvest_files_'+inputDataset+'_'+first_time.strip()+'_'+last_time.strip()+'_'+current_date.strftime("%b-%d-%Y")+'.csv'
    df.to_csv(outputDir+outputFile, index=False)
    logger.info('Finished processing source data for dataset '+inputDataset+'.')

# Run main function takes outputDir, and outputFile as input.
if __name__ == "__main__":
    """ This is executed when run from the command line """
    parser = argparse.ArgumentParser()

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--inputDir", action="store", dest="inputDir")    
    parser.add_argument("--outputDir", action="store", dest="outputDir")
    parser.add_argument("--inputDataset", action="store", dest="inputDataset")

    args = parser.parse_args()
    main(args)

