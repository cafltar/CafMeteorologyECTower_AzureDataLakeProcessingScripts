# AzureECTowerAccess
Code to access and download the EC tower data from the Azure data lake. The access protocols are left out of this repo for security reasons.

## Documents

### DataLakeDownloadTemplate:
- Contains the options, access values, and input/output filepaths for both a local machine and the Azure Datalake
- Is an Excel sheet with 5 workbooks, one for each site plus and explainer for the different rows and columns within each.
- Is required to run the scripts in, most of the options are contained here along with some QC components. A few minor considerations are still hard-coded within the driver script but will be discussed below.
- THe row and column headers are matched exactly within the code and messing with these will cause the code to error as a "Key Error" for a mismatched index or column.


## Scripts

### LTARCAFTowerReport:
- Driver script for rest of the function and library scripts. 
- Are a few options in this that need to change and are quasi-hardcoded (see Features still too add)
    - *os.chdir*: directory that contains this repo either locally or not, needs to be 
    - *Time*: The timestep for the column being used; for flux this is 30T, for the 15 minute met files this is 15T. Needs to match with the appropriate column. No safeguards to check.
    - *col*: Whether the script is being run for the flux or meteorology data. Two options are Met or Flux; need to change the Time to match
    - *save*: Default to true; if want to save the aggregated files
    - *Sites*: List of sites wanted to download and upload data for; can limit to just one site or all four, only need to change the names, can be in any order. The site names must be one word and are case senstive (e.g., CookEast)
    - *S_V*: The logger code version number for the list in the Sites variable; update to match the site list in the correct order. Could be moved to Excel sheet in future versions
    - *tag*: End tag for the files to be saved to the local copy; local copy does not version like the uploaded copy does; local copy is additive, uploaded iteration is versioned to the day created with new file for each new day the script is run.

### AzureDataLakeAccess:
- Library of functions to download and upload flux and meteorology data to the Azure datalake and aggregate files. Also includes the QC functions for the meteorology and flux data. Contains a few other minor scripts to facilitate the readin and general data completeness checks. A full list of the functions is below with varying degrees of description completeness.
    - *format_plot*: Used for the tower report to format the plots into a relatively consistent form and control axis ticks/labels
    - *indx_fill*: Fills in missing timesteps with blank rows to generate complete timeseries; aids in making sure data completion
    - *Fast_Read*: Reads in the data for both the downloaded and aggregated files, calls indx_fill and formatsand sorts datetimes to index values.
    -*AccessAzure*: Main function that controls the upload/download process. Is the only function called by LTARCAFTowerReport; reads in the Excel sheet for each site being updated and calls all the other functions to do the I/O, downloading, QC, and uploading/saving of files.
    - *wateryear*: Calculates and sends back the current water/cropr year (Oct 1-Sept30) to upload and label the aggregated files correctly.
    - *Data_Update_Azure*: Function that takes in the excel sheet and other timestamp information to check the last downloaded file in the locally saved aggregated file, then using that to build the directory paths based off the info from the access excel sheet for the particular site to download the correct data. Not sure it will handle the change of year yet, not added into the code as of this writing.
    - *AggregatedUploadAzure*: Uploads the aggregated file that is saved locally; still needs to be better commented and followed through closer to make sure it is doing what is expected; so far it does. Saves file into the appropriate work directory under the correct wateryear though some silliness with multiple year files/paths.
    - *readinfo*: Reads the QC parameters for the Grade_cs function from the excel sheet and assigned to correct variable; called in the Grade_cs function. 
    - *Grade_cs*: Function to QC the flux data; see function for details.
    - *METQC*: Function call to the main QC function and re-adds the data back to the main dataframe before sending back to the main upadte function.
    - *MetQAQC*: Function to QC the meteorology data in both the flux and met files

### TowerReportPlots
- 

# Features still to add:
- Meteorology plot report generation akin to the flux tower report generation
- Add further QC options to flux data (despike; localized QC for sensor removals) if warranted

# Acknowledgements

Original code was written by Dr. Eric Russell ([esrussell16](https://github.com/esrussell16))

# License

As a work of the United States government, this project is in the public domain within the United States.

Additionally, we waive copyright and related rights in the work worldwide through the CC0 1.0 Universal public domain dedication.