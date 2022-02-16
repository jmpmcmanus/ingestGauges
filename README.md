# ingestGauges
This software is used to ingest gauge data from NOAA, NCEM, and ADICIRC. To begin ingesting data, you first need to install the apsviz-timeseriesdb repo which can bet downloaded from:  

https://github.com/RENCI/apsviz-timeseriesdb  

Flow the installation instractions for that repo. It creates and postgesql database, that serves data using a Django Rest Framework (DRF) app. 

The gauge data that is being ingested cans currently be accessed from the apsviz-timeseriesdb.edc.renci.org VM in the following directory:   

/projects/ees/TDS/DataHarvesting/DAILY_HARVESTING/ 

It was generated using the software in the DataHarvesterSources repo, which cat be downloaded from:  

https://github.com/RENCI/DataHarvesterSources.git


## Create list of data files, and ingest them into the database

The first step is to make sure you have created a directory to store the files that are going to be creating. Currently, we are using the follwing directory on the apsviz-timeseriesdb.edc.renci.org VM:  

/projects/ees/TDS/DataIngesting/DAILY_INGEST/

Next, create the files that the list of files that are to be ingested into the database. This can be down using the createHarvestFileMeta.py program by running the following commands:

python createHarvestFileMeta.py --inputDir /projects/ees/TDS/DataHarvesting/DAILY_HARVESTING/ --outputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --inputDataset adcirc_stationdata  
python createHarvestFileMeta.py --inputDir /projects/ees/TDS/DataHarvesting/DAILY_HARVESTING/ --outputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --inputDataset contrails_stationdata  
python createHarvestFileMeta.py --inputDir /projects/ees/TDS/DataHarvesting/DAILY_HARVESTING/ --outputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --inputDataset noaa_stationdata  

The information in these files can be ingested into the database using the ingestData.py program, by running the follwing command:  
 
python ingestData.py --inputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --ingestDir /home/DataIngesting/DAILY_INGEST/ --inputTask File --inputDataset None

## Create files containing list of stations, and ingest them into the database

The next step is the create files, that contain the guage stations. This can be down using the createIngestStationMeta.py program by running the following commands:

python createIngestData.py --outputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --inputDataset adcirc  
python createIngestData.py --outputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --inputDataset contrails  
python createIngestData.py --outputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --inputDataset noaa  

Before running these commands make sure you have ingested the original NOAA and NCEM gauge data by following the instalation instructions for the apsviz-timeseriesdb repo.

The information in these files can be ingested into the database using the ingestData.py program, by running the follwing command: 

python ingestData.py --inputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --ingestDir /home/DataIngesting/DAILY_INGEST/ --inputTask Station --inputDataset None

## Create files containing list of sources, and ingest them into the database

Now that the gauge stations have been ingested into the database, the source files can be created using the createIngestSourceMeta.py program. This program uses station data that was ingested in the previous section. It generates a list of source, using the station_id from the station table, and adds information about data sources. To create the source files run the following commands:  

python createIngestSourceMeta.py --outputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --outputFile adcirc_stationdata_TIDAL_namforecast_HSOFS_meta.csv  
python createIngestSourceMeta.py --outputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --outputFile adcirc_stationdata_TIDAL_nowcast_HSOFS_meta.csv  
python createIngestSourceMeta.py --outputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --outputFile adcirc_stationdata_COASTAL_namforecast_HSOFS_meta.csv  
python createIngestSourceMeta.py --outputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --outputFile adcirc_stationdata_COASTAL_nowcast_HSOFS_meta.csv  
python createIngestSourceMeta.py --outputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --outputFile adcirc_stationdata_RIVERS_namforecast_HSOFS_meta.csv  
python createIngestSourceMeta.py --outputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --outputFile adcirc_stationdata_RIVERS_nowcast_HSOFS_meta.csv  
python createIngestSourceMeta.py --outputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --outputFile noaa_stationdata_TIDAL_meta.csv  
python createIngestSourceMeta.py --outputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --outputFile contrails_stationdata_COASTAL_meta.csv  
python createIngestSourceMeta.py --outputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --outputFile contrails_stationdata_RIVERS_meta.csv  

The information in these files can be ingested into the database using the ingestData.py program, by running the follwing command: 

python ingestData.py --inputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --ingestDir /home/DataIngesting/DAILY_INGEST/ --inputTask Source --inputDataset None  

## Create files containing gauge data, and ingest them into the database

Now that both the both the station and source data has been ingested into the database, the files containing the gauge data cand be created by using the createIngestData.py program. This program reads in the data files in the DataHarvesting directory, and adds the source_id, from the source table, along the a timemark value along with the source_id, and timestep uniquely identifies each record in the data table. To create the data files run the following commands:  

python createIngestData.py --outputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --inputDataset adcirc  
python createIngestData.py --outputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --inputDataset contrails  
python createIngestData.py --outputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --inputDataset noaa 

The information in these files can be ingested into the database using the ingestData.py program, by running the follwing commands:

python ingestData.py --inputDir None --ingestDir /home/DataIngesting/DAILY_INGEST/ --inputTask Data --inputDataset adcirc  
python ingestData.py --inputDir None --ingestDir /home/DataIngesting/DAILY_INGEST/ --inputTask Data --inputDataset contrails  
python ingestData.py --inputDir None --ingestDir /home/DataIngesting/DAILY_INGEST/ --inputTask Data --inputDataset noaa  

## Create view of station, source and data tables 

The final step is the create a view combining the station, source and data tables. This can be down by running the following command:  

python ingestData.py --inputDir None --ingestDir None --inputTask View --inputDataset None  

 
