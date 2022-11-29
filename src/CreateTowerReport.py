import os
import pandas as pd
# Change to directory that houses the the AzureDataLakeAccess library
#os.chdir(r'C:\Users\russe\Documents\GitHub\AzureECTowerAccess')       
import TowerReportPlots as TRP
import pathlib

cwd = pathlib.Path.cwd()
outputPath = cwd / 'data' / 'output'

TRP.TowerReport(
    str(outputPath),
    None,
    '2022-04-11')