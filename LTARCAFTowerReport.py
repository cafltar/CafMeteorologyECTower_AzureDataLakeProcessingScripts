# -*- coding: utf-8 -*-
"""
Created on Mon Mar  5 11:37:05 2018

@author: Eric Russell 
"""
import os
import pandas as pd
# Change to directory that houses the the AzureDataLackAccess library
os.chdir(r'C:\Users\russe\Documents\GitHub\AzureECTowerAccess')       
import AzureDataLakeAccess as ADLA
import TowerReportPlots as TRP

# Change only these things; are the controls to the script that are not within the set-up excel file
Time = '30T' # Timestamp of the datafile
col = 'Flux' # Column for the different paths between Flux or Met
tag = '_AGCY2021Update.csv' # End tag of the file to be saved; added to the end of CEF varaible below
#*********************************************************************
save = True # If want to save the aggregated file or not; default is True
Sites = ['CookEast','CookWest','BoydNorth', 'BoydSouth'] # Name of the sites wanted; can be as many as want but must be within square brackets
S_V = ['40826','40826','18329','18329']

#%% Download and aggregate the files from Azure blob storage
for k in range (0,len(Sites)):
    # Different file structure and output locations for the different sites
    access = pd.read_excel(r'C:\Users\russe\Desktop\LTAR\LTAR_Metadata_ECFlux\DataLakeDownload.xlsx', sheet_name =Sites[k],index_col = 'Variable')
    # Directory should be where the base file starts. There needs to be some start file even if it is blank with the date of the start point; 
    # I haven't sorted out a "first" pass without a start file to be used. 
    colT = col + '_V'+S_V[k]
    CEF = 'C:\\Users\\russe\\Desktop\\LTAR\\Problems\\Temp\\Aggregate\\'+Sites[k]+'*_'+colT+'*.csv' 
    # Calls the function that access the Azure data lake using the options given in the first section. 
    # Can add the save and date options if want them to be different than the default
    df = ADLA.AccessAzure(Sites[k], col, Time, access, CEF,tag, QC=True)
if col =='Flux':
    TRP.TowerReport()
# if col == 'Met':
    # TRP.MetTowerReport()