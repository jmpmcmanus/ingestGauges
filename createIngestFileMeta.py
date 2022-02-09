#!/usr/bin/env python
# coding: utf-8

# Import python modules
import argparse, glob, sys, os, datetime
import pandas as pd
from loguru import logger

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

        outputList.append([dir_path,file_name,data_date_time,data_begin_time,data_end_time,file_date_time,source,content_info,ingested]) 

    df = pd.DataFrame(outputList, columns=['dir_path', 'file_name', 'data_date_time', 'data_begin_time', 'data_end_time', 'file_date_time', 'source', 'content_info', 'ingested'])

    first_time = df['data_date_time'][0]
    last_time = df['data_date_time'].iloc[-1] 

    return(df, first_time, last_time)

# Main program function takes args as input, which contains the outputDir, and outputFile values.
@logger.catch
def main(args):
    # Add logger
    logger.remove()
    log_path = os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs'))
    logger.add(log_path+'/createIngestFileMeta.log', level='DEBUG')

    # Extract args variables
    inputDir = args.inputDir
    outputDir = args.outputDir
    inputDataset = args.inputDataset

    logger.info('Start processing source data for dataset '+inputDataset+'.')
    df, first_time, last_time = createFileList(inputDir, inputDataset)
    outputFile = 'files_'+inputDataset+'_'+first_time.strip()+'_'+last_time.strip()+'.csv'
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

