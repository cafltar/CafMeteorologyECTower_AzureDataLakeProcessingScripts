# -*- coding: utf-8 -*-
"""
Created on Mon Mar  5 11:37:05 2018

Updated: 2024-

@author: Eric Russell 
@author: Bryan Carlson
@author: Eddie Steiner
"""
import os
import pandas as pd
# Change to directory that houses the the AzureDataLakeAccess library
#os.chdir(r'C:\Users\russe\Documents\GitHub\AzureECTowerAccess')       
import AzureDataLakeAccess as ADLA
import TowerReportPlots as TRP
import pathlib

# Change only these things; are the controls to the script that are not within the set-up excel file
#Time = '30min' # Timestamp of the datafile
#col = 'Flux' # Column for the different paths between Flux or Met
#tag = '_AGCY2021Update.csv' # End tag of the file to be saved; added to the end of CEF varaible below

flux = {'col': 'Flux', 'Time': '30min'}
met = {'col': 'Met', 'Time': '15min'}

#DataTables = [flux, met]
DataTables = [flux, met]

#*********************************************************************
save = True # If want to save the aggregated file or not; default is True

Sites = ['CookEast','CookWest'] # Name of the sites wanted; can be as many as want but must be within square brackets

#Sites = ['CookEast','CookWest'] # Name of the sites wanted; can be as many as want but must be within square brackets
#Sites = ['CookWest','BoydNorth', 'BoydSouth']
#S_V = ['40826','40826','18329','18329']

# Get path to config file, assume cwd is at root project level
cwd = pathlib.Path.cwd()
configPath = cwd / '.secret' / 'DataLakeDownload.xlsx'

# Setup data paths
inputPath = cwd / 'data' / 'input'
inputPath.mkdir(parents=True, exist_ok=True)

workingPath = cwd / 'data' / 'working'
workingPath.mkdir(parents=True, exist_ok=True)

outputPath = cwd / 'data' / 'output'
outputPath.mkdir(parents=True, exist_ok=True)

#%% Download and aggregate the files from Azure blob storage
for dataTable in DataTables:

    col = dataTable['col']
    Time = dataTable['Time']

    for k in range (0,len(Sites)):
        # Different file structure and output locations for the different sites
        access = pd.read_excel(configPath, sheet_name =Sites[k],index_col = 'Variable').to_dict()

        # Add path information to access
        access[col]["inputPath"] = str(inputPath)
        access[col]["workingPath"] = str(workingPath)
        access[col]["outputPath"] = str(outputPath)

        # Directory should be where the base file starts. There needs to be some start file even if it is blank with the date of the start point; 
        # I haven't sorted out a "first" pass without a start file to be used. 
        colT = col + '_' + access[col]['Ver']
        #CEF = 'C:\\Users\\russe\\Desktop\\LTAR\\Problems\\Temp\\Aggregate\\'+Sites[k]+'*_'+colT+'*.csv' 
        #globString = Sites[k]+'*_'+colT+'*.csv'

        # {Site}\{Site}_{Met/Flux}_AggregateQC_CY{YYYY}_V{ProgramSignature}_{YYYYMMDD}.csv
        globString = Sites[k] + '_' + col + '_AggregateQC_CY*' + '_' + access[col]['Ver'] + '*.csv'
        #globString = Sites[k] + "\\" + Sites[k] + '_' + col + '_AggregateQC_CY*' + '_' + access[col]['Ver'] + '*.csv'
        CEF = str(outputPath / Sites[k] / col / globString)

        # Calls the function that access the Azure data lake using the options given in the first section. 
        # Can add the save and date options if want them to be different than the default
        
        df = ADLA.AccessAzure(Sites[k], col, Time, access, CEF, QC=False)
        

    if col =='Flux':
        TRP.TowerReport(str(outputPath))
#    if col == 'Met':
#        TRP.MetTowerReport(str(outputPath))

