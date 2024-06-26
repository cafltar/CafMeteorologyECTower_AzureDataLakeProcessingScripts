# -*- coding: utf-8 -*-
"""
Created on Mon Jun  1 10:59:51 2020
Modified on ... look at git commit log, you lazy bum
@author: Eric Russell, Assistant Research Professor, CEE WSU
@author: Bryan Carlson, Ecoinformaticist, USDA-ARS
contact: eric.s.russell@wsu.edu
Library of functions for the Azure Data Lake download codeset; see the readme within this repo for more details about the different scripts used
Comments in this are specific to the functions
"""
# General library imports for functions; some functions have the import statements as part of the function
import pathlib
import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator

import glob
import datetime
#from datetime import date

def format_plot(ax,yf,xf,xminor,yminor,yl,yu,xl,xu):
    #subplot has to have ax as the axis handle
    # Does not accept blank arguments within the function call; needs to be a number of some sort even if just a 0.
    # Format the x and yticks
    plt.yticks(fontsize = yf)
    plt.xticks(fontsize = xf)
    minor_locator = AutoMinorLocator(xminor)
    ax.xaxis.set_minor_locator(minor_locator)
    minor_locator = AutoMinorLocator(yminor)
    ax.yaxis.set_minor_locator(minor_locator)
    ax.tick_params(axis='both',direction='in',length=12.5,width=2)
    ax.tick_params(axis='both',which = 'minor',direction='in',length=5)
    plt.ylim([yl,yu])
    plt.xlim([xl,xu])  
    return

def indx_fill(df_in, frq):   
    # Fills in missing index values for a continuous time series. Rows are left blank.
    df = df_in.copy()

    df.index = pd.to_datetime(df.index)
    
#    # Sort index in case it came in out of order, a possibility depending on filenames and naming scheme
#    df = df.sort_index()
#    # Remove any duplicate times, can occur if files from mixed sources and have overlapping endpoints
#    df = df[~df.index.duplicated(keep='first')]

    # Remove any duplicated rows; keep row with more data
    df['nan_count'] = pd.isna(df).sum(1)
    df = df.sort_values(['RECORD', 'nan_count']) # Can sort on RECORD here because values with null/na index were previously removed
    df = df[~df.index.duplicated(keep='first')]
    df = df.drop('nan_count',axis=1).sort_index()

        # Fill in missing times due to tower being down and pad dataframe to midnight of the first and last day
    idx = pd.date_range(df.index[0].floor('D'),df.index[len(df.index)-1].ceil('D'),freq = frq)
    # Reindex the dataframe with the new index and fill the missing values with NaN/blanks
    df = df.reindex(idx, fill_value=np.NaN)
    return df

def Fast_Read(filenames, hdr, idxfll, specified_dtypes = None):
    #Check to make sure there are files within the directory and doesn't error
    if len(filenames) == 0:
        print('No Files in directory, check the path name.')
        return  # 'exit' function and return error
    elif (len(filenames) > 0) & (hdr ==4): # hdr == 4 is for data direct from the data logger as there are four header lines
        #Initialize dataframe used within function
        Final = [];Final = pd.DataFrame(Final)
        for k in range (0,len(filenames)):
            #Read in data and concat to one dataframe; no processing until data all read in
            if specified_dtypes:
                try:
                    df = pd.read_csv(filenames[k],index_col = 'TIMESTAMP',header= 1,skiprows=[2,3],na_values='NAN',dtype=specified_dtypes)
                except:
                    continue
            else:
                try:
                    df = pd.read_csv(filenames[k],index_col = 'TIMESTAMP',header= 1,skiprows=[2,3],na_values='NAN',low_memory=False)
                except:
                    continue

            Final = pd.concat([Final,df], sort = False)
        
        # Fill missing index with blank values
        Out = indx_fill(Final, idxfll)
        # Convert to datetime for the index
        Out.index = pd.to_datetime(Out.index)
        # Sort index in chronological order; readin files not always in order depending on how files are read in or named
        Out = Out.sort_index()
    elif (len(filenames) > 0) & (hdr ==1): # hdr == 1 means there is only one header line and has been through some amount of processing
        #Initialize dataframe used within function
        Final = [];Final = pd.DataFrame(Final)
        for k in range (0,len(filenames)):
            #Read in data and concat to one dataframe; no processing until data all read in
            if specified_dtypes:
                df = pd.read_csv(filenames[k],index_col = 'TIMESTAMP',header= 0,dtype=specified_dtypes)
            else:
                df = pd.read_csv(filenames[k],index_col = 'TIMESTAMP',header= 0,low_memory=False)

            Final = pd.concat([Final,df], sort = False)
        # Convert time index
        Out = indx_fill(Final,idxfll)
        Out.index = pd.to_datetime(Out.index)
        Out = Out.sort_index()
    return Out # Return dataframe to main function.    

def download_data_from_datalake(access, s, col, siteName, endDate:datetime.date=None):
    # Import libraries needed to connect and credential to the data lake.
    from azure.storage.filedatalake import DataLakeServiceClient
    from azure.identity import ClientSecretCredential
    import datetime
    from datetime import date
    from dateutil.relativedelta import relativedelta
    import pathlib

    end_date = date.today()
    if endDate:
        end_date = endDate
        
    # Get today's date
    #today = date.today()

    # Pull the access information from the driver Excel workbook for the datalake in question
    storage_account_name =  access[col]['storageaccountname']
    client_id =  access[col]['CLIENTID']
    tenant_id = access[col]['TENANTID']
    client_secret = access[col]['CLIENTSECRET']
    access_path = access[col]['path']
    localfile = access[col]['LOCAL_DIRECT']
    # If localfile is not defined in xlsx file, then default to something like: input/CookEast/Met
    if pd.isnull(localfile):
        localfile = pathlib.Path(access[col]["inputPath"]) / siteName / col
        localfile.mkdir(parents=True, exist_ok=True)
    
    file_system = access[col]['file_system']
    back = access[col]['back']
    # Credential to the client and build the token
    credential = ClientSecretCredential(tenant_id,client_id, client_secret)

    # Connect to the Data Lake through this function with the access credentials; do not change this.
    try:  
        global service_client
        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=credential)
    except Exception as e:
            print(e)
    file_system_client = service_client.get_file_system_client(file_system)

    date_inc = datetime.date(s.year, s.month, 1)

    while date_inc <= end_date:
        
        paths = file_system_client.get_paths(f'{access_path}{date_inc.year:04d}/{date_inc.month:02d}')
        
        try:
            for path in paths:
                
                    # This gets all files for month; need to only download after specified day
                    z = path.name 
                    #Y = z[-19:-15]; M = z[-14:-12]; D = z[-11:-9]
                    #bd = datetime.date(int(Y), int(M), int(D))  

                    date_components = z.split('/')[-1].split('_')[3:6]
                    bd = datetime.date(
                        int(date_components[0]), 
                        int(date_components[1]), 
                        int(date_components[2]))
                            
                    if (bd >= s) & (bd<=end_date):
                        # If dates are within the correct range, downloads the file to the local directory
                        #local_file = open(localfile+z[back:],'wb'); print(local_file)                
                        filePath = localfile / pathlib.Path(z).name
                        if not filePath.is_file():
                            local_file = open(filePath, 'wb')
                            print(str(filePath))
                            file_client = file_system_client.get_file_client(z)
                            download = file_client.download_file()
                            downloaded_bytes = download.readall()
                            local_file.write(downloaded_bytes)
                            local_file.close()
                        else:
                            print(f'Skipping {filePath}')
        except Exception as e:
            print(e)
            pass

        date_inc = date_inc + relativedelta(months=1)


def Data_Update_Azure(access, s,col, siteName):
    raise Exception('Deprecated: use download_data_from_datalake instead') 
    # Import libraries needed to connect and credential to the data lake.
    from azure.storage.filedatalake import DataLakeServiceClient
    from azure.identity import ClientSecretCredential
    import datetime
    from datetime import date
    import pathlib

    # Pulls today's data from the computer and uses as the end date.
    e =  date.today()
    # Pull the access information from the driver Excel workbook for the datalake in question
    storage_account_name =  access[col]['storageaccountname']
    client_id =  access[col]['CLIENTID']
    tenant_id = access[col]['TENANTID']
    client_secret = access[col]['CLIENTSECRET']
    path = access[col]['path']
    localfile = access[col]['LOCAL_DIRECT']
    if pd.isnull(localfile):
        localfile = pathlib.Path(access[col]["inputPath"]) / siteName
        localfile.mkdir(parents=True, exist_ok=True)
    
    file_system = access[col]['file_system']
    back = access[col]['back']
    # Credential to the client and build the token
    credential = ClientSecretCredential(tenant_id,client_id, client_secret)
    # Collect the integer value of the month of the start date (s)
    month = int(s.month)
    year = int(s.year)
    td = date.today()
    # Connect to the Data Lake through this function with the access credentials; do not change this.
    try:  
        global service_client
        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=credential)
    except Exception as e:
            print(e)
    file_system_client = service_client.get_file_system_client(file_system)
    # Still need to deal with year in the path.
    # Checks that the month of the current date is the same or greater than the last month of the previous data's aggregation
    yrt = False
    while year<=td.year:
        if year<td.year:
            paths = file_system_client.get_paths(path+ str(s.year) +'/'+str(s.month))
            for path in paths:
                z = path.name
                Y = z[-19:-15]; M = z[-14:-12]; D = z[-11:-9]
                bd = datetime.date(int(Y), int(M), int(D))                    
                if (bd >= s)& (bd<=e):
                # If dates are within the correct range, downloads the file to the local directory
                    #local_file = open(localfile+z[back:],'wb'); print(local_file)                
                    filePath = localfile / pathlib.Path(z).name
                    if not filePath.is_file():
                        local_file = open(filePath, 'wb')
                        print(str(filePath))
                        file_client = file_system_client.get_file_client(z)
                        download = file_client.download_file()
                        downloaded_bytes = download.readall()
                        local_file.write(downloaded_bytes)
                        local_file.close()
                    else:
                        print(f'Skipping {filePath}')
            year = year+1
            yrt = True    
        if year == td.year:
            path = access[col]['path']
            if yrt: month = int(e.month)
            while td.month >= month:
        # Check if month int/string is correct or not; the path needs a 2-digit month and an int value will default to 1 digit is less than 10.
                if month < 10:
                    paths = file_system_client.get_paths(path+ str(e)[0:4] +'/0'+str(month))
                elif month >=10:
                    paths = file_system_client.get_paths(path+ str(e)[0:4] +'/'+str(month))
        # Loop over all the path names and build path to download to the local file.
                for path in paths:
                    z = path.name
            # Builds datetime of the current path and checks against the start and end dates
                    Y = z[-19:-15]; M = z[-14:-12]; D = z[-11:-9]
                    bd = datetime.date(int(Y), int(M), int(D))                    
                    if (bd >= s)& (bd<=e):
                # If dates are within the correct range, downloads the file to the local directory
                        local_file = open(localfile / z.split('/')[-1],'wb'); print(local_file)                
                        file_client = file_system_client.get_file_client(z)
                        download = file_client.download_file()
                        downloaded_bytes = download.readall()
                        local_file.write(downloaded_bytes)
                        local_file.close()
                month = month+1 # While loop so needs a way to exit the loop counter
                path = access[col]['path'] # Print path name of files downloaded for user to look at it and admire.
        year = year+1
        
def wateryear(calendar_date:datetime.date = datetime.date.today()):
    # Calculate what the wateryear is; checks if it is Ooctober or not; if so then adds one to the year to get to the correct water year. 

    if int(str(calendar_date).replace('-','')[4:6]) < 10:
        wateryear = str(calendar_date).replace('-','')[0:4]
    else:
        wateryear = str(int(str(calendar_date).replace('-','')[0:4])+1)
    return wateryear # Returns water year as a string.

def get_latest_file(files):
    """Takes a list of files (probably from glob) and returns the one with the latest date stamp (in form of _YYYYMMDD at end of the filename)
    """

    latest_file = files[0]

    for f in files:
        if get_datetime_from_filename(f) > get_datetime_from_filename(latest_file):
            latest_file = f

    return latest_file

def get_datetime_from_filename(filestring:str):
    """Takes a filename or filepath string and returns a datetime object representing the iso date in the filename
    """
    import datetime

    stem = pathlib.Path(filestring).stem
    isodate = stem.split('_')[-1]
    dt = datetime.datetime.strptime(isodate, '%Y%m%d')

    return dt


def get_latest_date_from_file(col, Time, CEF):
    aggregated_file = get_latest_file(glob.glob(CEF))

    CE = Fast_Read([aggregated_file],1, Time, get_dtypes(f'{col}Aggregated')) # Read in the previous aggregated file(s)
    s = str(CE.index[-1])[0:10]; s= s.replace('-', '') # Find the last index in the file and convert to a string
    s = datetime.date(int(s[0:4]), int(s[4:6]), int(s[6:])) - datetime.timedelta(days=1)

    return s

def get_last_date_of_wateryear(wateryear:int):
    dt = datetime.date(wateryear, 9, 30)

    return dt

def get_first_date_of_wateryear(wateryear:int):
    dt = datetime.date(wateryear-1, 10, 1)

    return dt

def AccessAzure(Sites, col, Time,access,CEF,save=True, QC = True,startDate:str=None,endDate:str=None):
    # Main driver function of the datalake access and QC functions, called from the main driver of the codeset.
    # If startDate defined but endDate=None: Downloads blobs from startDate to current date or to end of startDate's water year, if current date is later
    # If endDate defined but startDate=None: Searches for a file in the output folder (previously aggregated) and downloads files from the last date in the file (or from the endDate's water year, if file's date is earlier) until reaching endDate
    # If startDate and endDate defined: Downloads files between startDate and endDate as long as within same water year
    # If startDate=None and endDate=None: Searches for a file in the output folder (previously aggregated) and downloads files from the last date in the file (or from the endDate's water year, if file's date is earlier) to current date or to end of startDate's water year, if current date is later
    import glob
    import datetime
    import pandas as pd
    from datetime import date
    from dateutil import parser
    # Collect which column, met or flux
    ver = access[col]['Ver']
    #cy = wateryear() # Determine wateryear to build file path

    curr_date = date.today()

    if startDate and (endDate == None):
        # Downloads blobs from startDate to current date or to end of startDate's water year, if current date is later
        print("WARNING: This chunk of code has not been fully tested")

        start_date = parser.parse(startDate).date()
        start_date_wateryear = wateryear(start_date)

        current_wateryear = wateryear(curr_date)
        
        if current_wateryear == start_date_wateryear:
            end_date = curr_date
        else:
            end_date = get_last_date_of_wateryear(int(start_date_wateryear))

    elif (startDate == None) and endDate:
        # Searches for a file in the output folder (previously aggregated) and downloads files from 
        # the last date in the file (or from the endDate's water year, if file's date is earlier) until reaching endDate
        print("WARNING: This chunk of code has not been fully tested")

        end_date = parser.parse(endDate).date()
        end_date_wateryear = wateryear(end_date)

        # Throws an error if there are no files
        try:
            start_date = get_latest_date_from_file(col, Time, CEF)
        except:
            # Threw an error, assume no files in output folder so get first date of the water year
            start_date = get_first_date_of_wateryear(int(end_date_wateryear))
        else:
            start_date_wateryear = wateryear(start_date)

        if start_date_wateryear != end_date_wateryear:
            start_date = get_first_date_of_wateryear(int(end_date_wateryear))

    elif startDate and endDate:
        # Downloads files between startDate and endDate as long as within same water year
        start_date = parser.parse(startDate).date()
        end_date = parser.parse(endDate).date()

        start_date_wateryear = wateryear(start_date)
        end_date_wateryear = wateryear(end_date)

        if start_date_wateryear != end_date_wateryear:
            raise Exception ("The given dates are of different water years, this goes against the scripts prime assumptions and may cause your computer to explode. Reconsider your inputs.")

    elif (startDate == None) and (endDate == None):
        # If startDate=None and endDate=None: Searches for a file in the output folder (previously aggregated) 
        # and downloads files from the last date in the file (or from the endDate's water year, if file's date 
        # is earlier) to current date or to end of startDate's water year, if current date is later

        current_wateryear = wateryear(curr_date)

        try:
            start_date = get_latest_date_from_file(col, Time, CEF)
        except:
            # Threw an error, assume no files in output folder so get first date of the current water year
            start_date = get_first_date_of_wateryear(int(current_wateryear))
            end_date = curr_date
        else:
            start_date_wateryear = wateryear(start_date)

            if current_wateryear == start_date_wateryear:
                end_date = curr_date
            else:
                end_date = get_last_date_of_wateryear(int(start_date_wateryear))

    else:
        raise Exception("Script does not know how to proceed with the arguments given. Aborting...")

    if startDate == None:
        # No start date, so assume we're working off of a previously aggregated file. Grab data from that file
        try:
            aggregated_file = get_latest_file(glob.glob(CEF))
            CE = Fast_Read([aggregated_file],1, Time, get_dtypes(f'{col}Aggregated')) # Read in the previous aggregated file(s)
        except Exception as e: print(e)

#    if startDate == None:
#        # No start date, so assume we're working off of a previously aggregated file. Catch exception in case we're starting a fresh water year
#        try:
#            aggregated_file = get_latest_file(glob.glob(CEF))
#        except:
#            # Starting a fresh water year, so don't start a day before last data record
#            s = start_date
#        else:
#            # We have previous data so read it
#            CE = Fast_Read([aggregated_file],1, Time, get_dtypes(f'{col}Aggregated')) # Read in the previous aggregated file(s)
#            s = start_date - datetime.timedelta(days=1)
#        s = str(CE.index[-1])[0:10]; s= s.replace('-', '') # Find the last index in the file and convert to a string
#        s = datetime.date(int(s[0:4]), int(s[4:6]), int(s[6:])) - datetime.timedelta(days=1)
        #if int(s[6:])>1: # Check if it is the first day of the month or not to go back a day for the file collection later.
        #    s = datetime.date(int(s[0:4]), int(s[4:6]), int(s[6:])-1)
        #else: s = datetime.date(int(s[0:4]), int(s[4:6]), int(s[6:]))
#    else: s = parser.parse(startDate).date()
    
    
    print('Downloading files')
    # Call function to update the Azure data
    download_data_from_datalake(access, start_date, col, Sites, end_date)

    print('Reading '+ Sites)
    if not pd.isna(access[col]['LOCAL_DIRECT']):
        filenames = glob.glob(access[col]['LOCAL_DIRECT']+'\\*.dat') # Gather all the filenames just downloaded
        #globString = Sites[k] + '_' + col + '_AggregateQC_CY*' + '_' + access[col]['Ver'] + '*.csv'
    else: filenames = glob.glob(access[col]["inputPath"] + '\\' + Sites + '\\' + col + '\\*.dat')
    CEN = Fast_Read(filenames, 4,Time, get_dtypes(f'{col}Raw')) # Read in new files
    if 'CE' in locals():
        CE=pd.concat([CE,CEN], sort = False) # Concat new files the main aggregated file
    else: CE = CEN
    CE = CE.sort_index() # Sort index
    CE = CE.dropna(subset=['RECORD']) # Drop any row that has a NaN/blank in the "RECORD" number column; removes the overlap-extra rows added from the previous run
    CE = indx_fill(CE,Time) # Fill back in the index through to the end of the current day. Also removes duplicated values and inserts missing values.
    # CEFClean = CEF[:-4]+'NO_QC'+tag; CEFClean=CEFClean.replace('*','') # Replace something in a string; don't remember why.
    # CE.to_csv(CEFClean, index_label = 'TIMESTAMP') # Print new aggregated file to local machine for local copy
    if QC: # Boolean for QCing data
        if col == 'Met':
            print('QCing the Meteorology Data')
            CE = METQC(CE, col) # Calls met QC functions
        if col == 'Flux':
            print('QCing the Flux Data')
            CE = Grade_cs(CE, access) # Calls flux QC function    
            CE = METQC(CE, col) # Calls met QC function; flux data includes met data hence extra call.
    if save == True:
        print('Saving Data') 
        file_wateryear = wateryear(end_date) # assuming end and start dates are the same
        #CEF = (CEF[:-4]+tag).replace('*','') # replace wildcards that were used for glob
        
        today = str(date.today()).replace('-','') # Replace dashes within datestring to make one continuous string
        fname = Sites+'_'+col+'_AggregateQC_CY'+file_wateryear+'_'+ver+'_'+today+'.csv' # Build filename for uploaded file based on tyrannical data manager's specifications
        dpath = access[col]["outputPath"] + '\\' + Sites + '\\' + col
        if not os.path.exists(dpath):
            os.makedirs(dpath)
            
        fpath = access[col]["outputPath"] + '\\' + Sites + '\\' + col + '\\' + fname
        
        CE.to_csv(fpath, index_label = 'TIMESTAMP') # Print new aggregated file to local machine for local copy

        print('Uploading data')
        
        AggregatedUploadAzure(fname, access, col,fpath,file_wateryear) # Send info to upload function
    for f in filenames:
        os.remove(f)   # Delete downloaded files on local machines as no longer needed
    df=CE
    del CEN; del CE; return df # Delete variables for clean rerun as needed

def AggregatedUploadAzure(fname, access, col, CEF, cy):
    # Upload the aggregated file to the datalake
    from azure.storage.filedatalake import DataLakeServiceClient
    from azure.identity import ClientSecretCredential
    # Parse credentials from the access Excel workbook
    storage_account_name =  access[col]['storageaccountname']
    client_id =  access[col]['CLIENTID']
    tenant_id = access[col]['TENANTID']
    client_secret = access[col]['CLIENTSECRET']
    upload_dir = access[col]['UPLOAD']
    # Build client credential token
    credential = ClientSecretCredential(tenant_id,client_id, client_secret)
    service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format("https", storage_account_name), credential=credential)
    # Builds file path based on cropyear (water year) and upload directory
    file_system_client = service_client.get_file_system_client(upload_dir+cy+'/')
    file_client = file_system_client.get_file_client(fname) # Build filename for the upload
    file_client.create_file() # Creates the file in the datalake through the file client
    local_file = open(CEF,'r') # Opens the local copy of the aggregated file 
    file_contents = local_file.read() # Read local file copy
    file_client.upload_data(file_contents, overwrite=True) # Upload file to the datalake and overwrite if already exists; depending on how often code is run
    local_file.close() # Close file
    
    
#%% QC for the flux data for the Azure upload    
    
def readinfo(access):
    # Values pulled in from a separate *.csv file because easier and flexible; are the QC values for the flux qc function
    grade = int(access['Flux']['grade'])
    LE_B = [float(access['Flux']['LE_B']),float(access['Met']['LE_B'])]
    H_B = [float(access['Flux']['H_B']),float(access['Met']['H_B'])]
    F_B = [float(access['Flux']['F_B']),float(access['Met']['F_B'])]
    ustar = float(access['Flux']['ustar'])
    gg = [(access['Flux']['gg']),(access['Met']['gg']),(access['Val_3']['gg'])]
    col = [(access['Flux']['cls']),(access['Met']['cls']),(access['Val_3']['cls'])]
    return grade, LE_B, H_B, F_B, ustar, col, gg

def Grade_cs(data,access):
    # Basic flux qc function; more serious codeset not included.
    grade, LE_B, H_B, F_B, ustar,col,gg = readinfo(access)
    pd.options.mode.chained_assignment = None # Don't remember exactly why this is here; probably to avoid a warning statement somewhere 
    if (grade >9) | (grade<1): # Check that the grade value falls within acceptable bounds
        print('Grade number must be between 1-9.')
        return  # 'exit' function and return error 
    if (ustar<0): # Check that ustar is okay though default should be zero; no ustar filter should be used here.
        print('u-star must be a positive number.')
        return  # 'exit' function and return error 
    var = ['H_Flags','LE_Flags','Fc_Flags'] # Set flag column names
    if var[0] not in data: # Create flag columns if they do not already exist 
        Marker = [];Marker = pd.DataFrame(Marker, columns = var)
        data = data.join(Marker)
    for k in range (0,3): # Loops over the H, LE, and co2 flux columns; 
        df = data
        flux = col[k]
        if flux == col[1]: # Bounds checks for each of the flux values; set in driver sheet
            HL = (df[col[1]].astype(float) < LE_B[0]) | (df[col[1]].astype(float)>LE_B[1]) | df[col[1]].astype(float).isnull()
        elif flux ==col[0]:
            HL = (df[col[0]].astype(float) < H_B[0]) | (df[col[0]].astype(float)> H_B[1]) | df[col[0]].astype(float).isnull()
        elif flux ==col[2]:
            HL = (df[col[2]].astype(float) < F_B[0])|(df[col[2]].astype(float) > F_B[1])| df[col[2]].astype(float).isnull()
        data[(col[k]+'_Graded')] = data[col[k]] # Create the flux graded column
        data[var[k]] = '0'
        data[var[k]][HL] = '1' # Start building the flag values
        #QA/QC grade for data       
        Grade = df[gg[k]].astype(float) <= grade # Check flux again the developed turbulence grades
        data[var[k]][~Grade] = data[var[k]]+'1'
        data[var[k]][Grade] = data[var[k]]+'0'# add to the data flag
        # values for columns hardcoded assuming they do not change for the EasyFlux code; will need to be updated if column names change
        if 'Precipitation_Tot' in df.columns: # Check if recorded precip or not; if so, filter fluxes
            Precip = df['Precipitation_Tot'].astype(float) < 0.001
            data[var[k]][~Precip] = data[var[k]]+'1'
            data[var[k]][Precip] = data[var[k]]+'0'
        #10Hz sample Mask                  
        if 'CO2_sig_strgth_Min' in df.columns: # Check is co2 sig strength is high enough
            c_sig_strength = df['CO2_sig_strgth_Min'].astype(float) > 0.7
            data[var[k]][c_sig_strength] = data[var[k]]+'0'
            data[var[k]][~c_sig_strength] = data[var[k]]+'1'
        if 'H2O_sig_strgth_Min' in df.columns: # Check if h20 sig strength is high enough
            w_sig_strength = df['H2O_sig_strgth_Min'].astype(float) > 0.7
            data[var[k]][w_sig_strength] = data[var[k]]+'0'
            data[var[k]][~w_sig_strength] = data[var[k]]+'1'
        if 'sonic_samples_Tot' in df.columns: # Check if enough samples in the sonic column (80% coverage); 
            Samp_Good_Sonic = df['sonic_samples_Tot'].astype(float) > 14400 
            data[var[k]][~Samp_Good_Sonic] =data[var[k]]+'1'
            data[var[k]][Samp_Good_Sonic] = data[var[k]]+'0'
        if 'Fc_samples_Tot' in df.columns: # Check if enough samples in Fc column (80%) coverage
            Samp_Good_IRGA = df['Fc_samples_Tot'].astype(float)>14400
            data[var[k]][~Samp_Good_IRGA] = data[var[k]]+'1'
            data[var[k]][Samp_Good_IRGA] = data[var[k]]+'0'
        #Door Open Mask
        if 'door_is_open_Hst' in df.columns: # Check if door open meaning people at the site doing work
            Door_Closed = df['door_is_open_Hst'].astype(float) == 0
            data[var[k]][~Door_Closed] = data[var[k]]+'1'
            data[var[k]][Door_Closed] = data[var[k]]+'0'    
            Good = Precip & Grade & Door_Closed&~HL&c_sig_strength&w_sig_strength # Create single boolean from all the qc checks; only one fail will trigger fail
            Good = Good & (Samp_Good_Sonic | Samp_Good_IRGA) 
        else: # If door open is not part of the column set; should be with the logger data
            Good = Grade &~HL
            Good = Good & (Samp_Good_Sonic | Samp_Good_IRGA)
        data[(col[k]+'_Graded')][~Good] = np.NaN # Create column with nan/blank in the column if data is bad/filtered
        if k == 0: G = Good; 
        if k >0: G = pd.concat([G,Good],axis=1, sort = False)
        del Good # Delete Good variable for the next round of flux data.
    return data
    
#%%
    
def METQC(Data, col):
    # Driver for the met qc function to deal with some column shenanigans
    if col == 'Flux': # Different columns between the two for some reason; think has to do with the way the tables were constructed in the logger code
        Met_QC = Met_QAQC(RH=Data['RH_Avg'].astype(float),P=Data['amb_press_Avg'].astype(float), Tair = Data['amb_tmpr_Avg'].astype(float), 
                          WS = Data['rslt_wnd_spd'].astype(float), WD = Data['wnd_dir_compass'].astype(float), Precip = Data['Precipitation_Tot'].astype(float),
                          PAR =Data['PAR_density_Avg'].astype(float), Rn = Data['Rn_meas_Avg'].astype(float),VPD = Data['VPD_air'].astype(float),e = Data['e_Avg'].astype(float), e_s = Data['e_sat_Avg'].astype(float),z = 0.777)
    if col == 'Met': # 
        Met_QC = Met_QAQC(RH=Data['RH_Avg'].astype(float),P=Data['amb_press_Avg'].astype(float), Tair = Data['amb_tmpr_Avg'].astype(float), 
        WS = Data['rslt_wnd_spd'].astype(float), WD = Data['wnd_dir_compass'].astype(float), Precip = Data['Precipitation_Tot'].astype(float),
        PAR =Data['PAR_density_Avg'].astype(float), Rn = Data['Rn_meas_Avg'].astype(float),VPD = Data['VPD_air'].astype(float),e = Data['e'].astype(float), e_s = Data['e_sat'].astype(float),z = 0.777)
    if 'Tair_Filtered' in Data.columns: # Checks if the data has already been through the QC code or not; 
        for k in range(0,len(Met_QC.columns)):
            Data = Data.drop(columns=[Met_QC.columns[k]]) # Drops all columns in the metqc variable before readding them back; the QC occurs over the entire dataframe so will re-addd what was deleted; prevents adding multiple columns to the dataframe with the same header
            # Not sure why this is the case and this is a quick fix but don't like it
    Data = pd.concat([Data,Met_QC], axis = 1, sort=False) # Concat the metqc values to the dataframe.
    return Data


def Met_QAQC(**kwargs):
    Q = None
    if 'Tair' in kwargs.keys(): # Air temperature
        Tair = pd.DataFrame(kwargs['Tair'])
        Q = Tair; Q = pd.DataFrame(Q); 
        Q['Tair_Hard_Limit'] = (Q[Tair.columns[0]].astype(float) <= 50) & (Q[Tair.columns[0]].astype(float) >= -40) # Bounds check 
        Q['Tair_Change'] = ~(np.abs(Q[Tair.columns[0]].diff() >= 15)) & (np.abs(Q[Tair.columns[0]].diff() != 0)) # Check if data change between each time step
        Q['Tair_Day_Change'] = (Tair.resample('D').mean().diff !=0) # Checks if the daily average changes from zero
        Q['Tair_Filtered'] = Q[Tair.columns[0]][Q['Tair_Hard_Limit'] & Q['Tair_Change'] & Q['Tair_Day_Change']] #Adds filters and booleans together
        Q.drop(columns=[Tair.columns[0]],inplace=True) # Drops the columns that are filtered out; probably a better way to do this
    else:
        print('******Temperature not present******')
    
    if 'RH' in kwargs.keys():
        RH = pd.DataFrame(kwargs['RH']) 
        if Q is None:
            Q = RH; Q = pd.DataFrame(Q)
        else: Q= Q.join(RH)
        Q['RH_Hard_Limit'] = (Q[RH.columns[0]].astype(float) <= 103) & (Q[RH.columns[0]].astype(float) >= 0)
        Q['RH_gt_100'] = (Q[RH.columns[0]].astype(float) >= 100) & (Q[RH.columns[0]].astype(float) <= 103)
        Q['RH_Change'] = (np.abs(Q[RH.columns[0]].astype(float).diff() <= 50)) & (np.abs(Q[RH.columns[0]].diff() != 0))
        Q['RH_Day_Change'] = (RH.resample('D').mean().diff !=0)  
        Q['RH_Filtered'] = Q[RH.columns[0]][Q['RH_Hard_Limit']&Q['RH_Change']& Q['RH_Day_Change']]
        Q['RH_Filtered'] = Q['RH_Filtered'].replace(to_replace=Q['RH_Filtered'][Q['RH_gt_100']].tolist(), value = 100)
#        Q['RH_Filtered'][Q['RH_gt_100']]=100
        Q.drop(columns=[RH.columns[0]],inplace=True)

    else:
        print('**** RH not present ****')

    if 'P' in kwargs.keys():
        # Pressure checks; converts from pressure to MSLP as well; checks between the two
        P =  pd.DataFrame(kwargs['P']); 
        if Q is None:
            Q = P; Q = pd.DataFrame(Q)
        else: Q= Q.join(P)    
        Q['P_Hard_Limit'] = (Q[P.columns[0]].astype(float) <= 100) &(Q[P.columns[0]].astype(float) >= 80) 
        Q['P_Change'] = (np.abs(Q[P.columns[0]].diff() <= 3.1)) & (np.abs(Q[P.columns[0]].diff() != 0)) 
        Q['P_Filtered'] = Q[P.columns[0]][Q['P_Hard_Limit'] & Q['P_Change']]
        if ('Tair' in kwargs.keys()) & ('z' in kwargs.keys()):
            MSLP = []; 
            H = pd.DataFrame((8.314*(Tair[Tair.columns[0]]+273.15))/(0.029*9.81)/1000) # Scale height
            x = pd.DataFrame(-kwargs['z']/H[H.columns[0]]); 
            MSLP = P[P.columns[0]]/np.exp(x[x.columns[0]]) # Mean Sea Level Pressure
            MSLP = pd.DataFrame(MSLP);MSLP = MSLP.rename(columns={MSLP.columns[0]:"MSLP"})
            Q= Q.join(MSLP)
            Q['MSLP_Hard_Limit'] = (Q[MSLP.columns[0]].astype(float) <= 110) &(Q[MSLP.columns[0]].astype(float) >= 80)
            Q['MSLP_Change'] = (np.abs(Q[MSLP.columns[0]].diff() <= 31)) & (np.abs(Q[MSLP.columns[0]].diff() != 0)) #& (~np.isnan(Q[MSLP.columns[0]].diff())) 
            Q['MSLP_Filtered'] = Q[MSLP.columns[0]][Q['MSLP_Hard_Limit'] & Q['MSLP_Change']]
        else:
            print('**** Mean sea level pressure not present ****')
        Q.drop(columns=[P.columns[0]],inplace=True)
    else:
        print('**** Pressure not present ****')
        

    if 'WS' in kwargs.keys(): # Wind speed
        WS = pd.DataFrame(kwargs['WS'])
        if Q is None:
            Q = WS; Q = pd.DataFrame(Q)
        else: Q= Q.join(WS)
        Q['WS_Hard_Limit'] = (Q[WS.columns[0]].astype(float) < 60) & (Q[WS.columns[0]].astype(float) >= 0)
        Q['WS_Change'] = (np.abs(Q[WS.columns[0]].diff() <= 15)) & (np.abs(Q[WS.columns[0]].diff() != 0)) #& (~np.isnan(Q[WS.columns[0]].diff())) 
        Q['WS_Day_Change'] = (WS.resample('D').mean().diff !=0) 
        Q['WS_Filtered'] = Q[WS.columns[0]][Q['WS_Hard_Limit']&Q['WS_Change']&Q['WS_Day_Change']]
        Q.drop(columns=[WS.columns[0]],inplace=True)
    else:
        print('**** Wind Speed not present ****')
    
    if 'WD' in kwargs.keys(): # Wind direction
        WD = pd.DataFrame(kwargs['WD'])
        if Q is None:
            Q = WD; Q = pd.DataFrame(Q)
        else: Q= Q.join(WD)
        Q['WD_Hard_Limit'] = (Q[WD.columns[0]].astype(float) < 360) & (Q[WD.columns[0]].astype(float) >= 0)
        Q['WD_Change'] =  (np.abs(Q[WD.columns[0]].diff() != 0)) # (~np.isnan(Q[WD.columns[0]].diff())) &
        Q['WD_Filtered'] = Q[WD.columns[0]][Q['WD_Hard_Limit']&Q['WD_Change']]
        Q.drop(columns=[WD.columns[0]],inplace=True)
    else:
        print('**** Wind Direction not present ****')
    
    if 'PAR' in kwargs.keys():
        PAR = pd.DataFrame(kwargs['PAR']); 
        if Q is None:
            Q = PAR; Q = pd.DataFrame(Q)
        else: Q= Q.join(PAR)
        Q['PAR_Hard_Limit'] = (Q[PAR.columns[0]].astype(float) >= 0) & (Q[PAR.columns[0]].astype(float) < 5000)
        Q['PAR_Change'] = (np.abs(Q[PAR.columns[0]].diff() <= 1500))# & (~np.isnan(Q[PAR.columns[0]].diff()))
        Q['PAR_Day_Change'] = (PAR.resample('D').mean().diff != 0) # Causing problems for some reason
        Q['PAR_Filtered'] = Q[PAR.columns[0]][Q['PAR_Hard_Limit']&Q['PAR_Change']&Q['PAR_Day_Change']]
        Q.drop(columns=[PAR.columns[0]],inplace=True)
    else:
        print('**** PAR not present ****')
    
    if 'Rn' in kwargs.keys():
        Rn = pd.DataFrame(kwargs['Rn'])    
        if Q is None:
            Q = Rn; Q = pd.DataFrame(Q)
        else: Q= Q.join(Rn)
        Q['Rn_Hard_Limit'] = (Q[Rn.columns[0]].astype(float) >= -150) & (Q[Rn.columns[0]].astype(float) <= 1500)       
        Q['Rn_Change'] = (np.abs(Q[Rn.columns[0]].astype(float).diff() <= 500)) & (np.abs(Q[Rn.columns[0]].diff() != 0)) #& (~np.isnan(Q[Rn.columns[0]].astype(float).diff()))   
        Q['Rn_Day_Change'] = (Rn.resample('D').mean().diff !=0) 
        Q['Rn_Filtered'] = Q[Rn.columns[0]][Q['Rn_Hard_Limit']&Q['Rn_Change']&Q['Rn_Day_Change']]
        Q.drop(columns=[Rn.columns[0]],inplace=True)
    else:
        print('**** Net Radiations not present ****')
    
    if 'Precip' in kwargs.keys(): # Lot of filters because of the difference of precip is there is or is not RH and check for frozen precip with temperature as the tipping bucket is bad with snow
        Precip = pd.DataFrame(kwargs['Precip'])
        if Q is None:
            Q = P; Q = pd.DataFrame(Q)
        else: Q= Q.join(Precip)
        Q['Precip_Hard_Limit'] = (Q[Precip.columns[0]].astype(float) < 100) & (Q[Precip.columns[0]].astype(float) >= 0)
        Z_Precip = Q[Precip.columns[0]].astype(float) ==0
        if ('RH' in kwargs.keys()) & ('Tair' in kwargs.keys()): # Checks for temp and RH in correct ranges.
            Q['Precip_RH_gt_90'] = (Q[Precip.columns[0]].astype(float) > 0) & (Q['RH_Filtered'].astype(float) >= 90)
            Q['Precip_Tair_lt_Zero'] = (Q[Precip.columns[0]].astype(float) > 0) & (Q['Tair_Filtered'] < 0)
            Q['Precip_Filtered'] = Q[Precip.columns[0]][Q['Precip_Hard_Limit']&Q['Precip_RH_gt_90']&~Q['Precip_Tair_lt_Zero']]
            Q['Precip_Filtered'] = Q['Precip_Filtered'].replace(to_replace=Q['Precip_Filtered'][Z_Precip].tolist(), value = 0)
            Q.drop(columns=[Precip.columns[0]],inplace=True)
        elif ('RH' in kwargs.keys()) & ('Tair' not in kwargs.keys()):
            Q['Precip_RH_gt_90'] = (Q[Precip.columns[0]].astype(float) > 0) & (Q['RH_Filtered'].astype(float) >= 90)
            Q['Precip_Filtered'] = Q[Precip.columns[0]][Q['Precip_Hard_Limit']&Q['Precip_RH']]
            Q['Precip_Filtered'] = Q['Precip_Filtered'].replace(to_replace=Q['Precip_Filtered'][Z_Precip].tolist(), value = 0)
            Q.drop(columns=[Precip.columns[0]],inplace=True)
        elif ('RH' not in kwargs.keys()) & ('Tair' in kwargs.keys()):
            Q['Precip_Tair_lt_Zero'] = (Q[Precip.columns[0]].astype(float) > 0) & (Q['Tair_Filtered'] < 0)
            Q['Precip_Filtered'] = Q[Precip.columns[0]][Q['Precip_Hard_Limit']& ~Q['Precip_Tair_lt_Zero']]
            Q['Precip_Filtered'] = Q['Precip_Filtered'].replace(to_replace=Q['Precip_Filtered'][Z_Precip].tolist(), value = 0)
            Q.drop(columns=[Precip.columns[0]],inplace=True)
        else:
            Q['Precip_Filtered'] = Q[Precip.columns[0]][Q['Precip_Hard_Limit']]
            Q['Precip_Filtered'] = Q['Precip_Filtered'].replace(to_replace=Q['Precip_Filtered'][Z_Precip].tolist(), value = 0)
            Q.drop(columns=[Precip.columns[0]],inplace=True)
    else:
        print('**** Precipitation not present ****')
    
    if 'VPD' in kwargs.keys():
        VPD = pd.DataFrame(kwargs['VPD'])
        if Q is None:
            Q = VPD; Q = pd.DataFrame(Q)
        else: Q= Q.join(VPD)
        Q['VPD_Hard_Limit'] = (Q[VPD.columns[0]].astype(float) < 50) & (Q[VPD.columns[0]].astype(float) >= 0)
        Q['VPD_Change'] = (np.abs(Q[VPD.columns[0]].astype(float).diff() <= 10)) & (np.abs(Q[VPD.columns[0]].diff() != 0)) 
        Q['VPD_Day_Change'] = (VPD.resample('D').mean().diff !=0) 
        Q['VPD_Filtered'] = Q[VPD.columns[0]][Q['VPD_Hard_Limit']&Q['VPD_Change']&Q['VPD_Day_Change']]
        Q.drop(columns=[VPD.columns[0]],inplace=True)
    if 'e' in kwargs.keys():
        e = pd.DataFrame(kwargs['e'])
        if Q is None:
            Q = e; Q = pd.DataFrame(Q)
        else: Q= Q.join(e)
        Q['e_Hard_Limit'] = (Q[e.columns[0]].astype(float) < 50) & (Q[e.columns[0]].astype(float) >= 0)
        Q['e_Change'] = (np.abs(Q[e.columns[0]].astype(float).diff() <= 10)) & (np.abs(Q[e.columns[0]].diff() != 0)) 
        Q['e_Day_Change'] = (e.resample('D').mean().diff !=0) 
        Q['e_Filtered'] = Q[e.columns[0]][Q['e_Hard_Limit']&Q['e_Change']&Q['e_Day_Change']]
        Q.drop(columns=[e.columns[0]],inplace=True)
    if 'e_s' in kwargs.keys():
        e_s = pd.DataFrame(kwargs['e_s'])
        if Q is None:
            Q = e_s; Q = pd.DataFrame(Q)
        else: Q= Q.join(e_s)
        Q['e_s_Hard_Limit'] = (Q[e_s.columns[0]].astype(float) < 50) & (Q[e_s.columns[0]].astype(float) >= 0)
        Q['e_s_Change'] = (np.abs(Q[e_s.columns[0]].astype(float).diff() <= 10)) & (np.abs(Q[e_s.columns[0]].diff() != 0)) 
        Q['e_s_Day_Change'] = (e_s.resample('D').mean().diff !=0) 
        Q['e_s_Filtered'] = Q[e_s.columns[0]][Q['e_s_Hard_Limit']&Q['e_s_Change']&Q['e_s_Day_Change']]        
        Q.drop(columns=[e_s.columns[0]],inplace=True)
    return Q
 
def get_dtypes(dataset_type):
    dtypes = {}

    if dataset_type == "FluxRaw":
        dtypes = {
            'RECORD':'Int64',
            'Fc_molar':float,
            'Fc_mass':float,
            'Fc_qc_grade':'Int64',
            'Fc_samples_Tot':'Int64',
            'LE':float,
            'LE_qc_grade':'Int64',
            'LE_samples_Tot':'Int64',
            'H':float,
            'H_qc_grade':'Int64',
            'H_samples_Tot':'Int64',
            'Rn':float,
            'G_surface':float,
            'energy_closure':float,
            'Bowen_ratio':float,
            'tau':float,
            'tau_qc_grade':'Int64',
            'u_star':float,
            'T_star':float,
            'TKE':float,
            'amb_tmpr_Avg':float,
            'Td_Avg':float,
            'RH_Avg':float,
            'e_sat_Avg':float,
            'e_Avg':float,
            'amb_press_Avg':float,
            'VPD_air':float,
            'Ux_Avg':float,
            'Ux_Std':float,
            'Uy_Avg':float,
            'Uy_Std':float,
            'Uz_Avg':float,
            'Uz_Std':float,
            'Ts_Avg':float,
            'Ts_Std':float,
            'sonic_azimuth':float,
            'wnd_spd':float,
            'rslt_wnd_spd':float,
            'wnd_dir_sonic':float,
            'std_wnd_dir':float,
            'wnd_dir_compass':float,
            'CO2_molfrac_Avg':float,
            'CO2_mixratio_Avg':float,
            'CO2_Avg':float,
            'CO2_Std':float,
            'H2O_molfrac_Avg':float,
            'H2O_mixratio_Avg':float,
            'H2O_Avg':float,
            'H2O_Std':float,
            'CO2_sig_strgth_Min':float,
            'H2O_sig_strgth_Min':float,
            'T_probe_Avg':float,
            'e_probe_Avg':float,
            'e_sat_probe_Avg':float,
            'Td_probe_Avg':float,
            'H2O_probe_Avg':float,
            'RH_probe_Avg':float,
            'rho_a_probe_Avg':float,
            'rho_d_probe_Avg':float,
            'Precipitation_Tot':float,
            'Rn_meas_Avg':float,
            'NRLITE_SENS':float,
            'PAR_density_Avg':float,
            'QUANTUM_SENS':float,
            'cupvane_WS_Avg':float,
            'cupvane_WS_rslt_Avg':float,
            'cupvane_WD_rslt_Avg':float,
            'cupvane_WD_csi_Std':float,
            'Tsoil_Avg':float,
            'tdr31X_wc_Avg':float,
            'tdr31X_tmpr_Avg':float,
            'tdr31X_E_Avg':float,
            'tdr31X_bulkEC_Avg':float,
            'tdr31X_poreEC_Avg':float,
            'shf_plate_avg':float,
            'SHFP_1_SENS':float,
            'profile_tdr31X_wc_Avg(1)':float,
            'profile_tdr31X_wc_Avg(2)':float,
            'profile_tdr31X_wc_Avg(3)':float,
            'profile_tdr31X_wc_Avg(4)':float,
            'profile_tdr31X_wc_Avg(5)':float,
            'profile_tdr31X_wc_Avg(6)':float,
            'profile_tdr31X_tmpr_Avg(1)':float,
            'profile_tdr31X_tmpr_Avg(2)':float,
            'profile_tdr31X_tmpr_Avg(3)':float,
            'profile_tdr31X_tmpr_Avg(4)':float,
            'profile_tdr31X_tmpr_Avg(5)':float,
            'profile_tdr31X_tmpr_Avg(6)':float,
            'profile_tdr31X_E_Avg(1)':float,
            'profile_tdr31X_E_Avg(2)':float,
            'profile_tdr31X_E_Avg(3)':float,
            'profile_tdr31X_E_Avg(4)':float,
            'profile_tdr31X_E_Avg(5)':float,
            'profile_tdr31X_E_Avg(6)':float,
            'profile_tdr31X_bulkEC_Avg(1)':float,
            'profile_tdr31X_bulkEC_Avg(2)':float,
            'profile_tdr31X_bulkEC_Avg(3)':float,
            'profile_tdr31X_bulkEC_Avg(4)':float,
            'profile_tdr31X_bulkEC_Avg(5)':float,
            'profile_tdr31X_bulkEC_Avg(6)':float,
            'profile_tdr31X_poreEC_Avg(1)':float,
            'profile_tdr31X_poreEC_Avg(2)':float,
            'profile_tdr31X_poreEC_Avg(3)':float,
            'profile_tdr31X_poreEC_Avg(4)':float,
            'profile_tdr31X_poreEC_Avg(5)':float,
            'profile_tdr31X_poreEC_Avg(6)':float,
            'upwnd_dist_intrst':float,
            'FP_dist_intrst':float,
            'FP_max':float,
            'FP_40':float,
            'FP_55':float,
            'FP_90':float,
            'FP_Equation':object,
            'UxUy_Cov':float,
            'UxUz_Cov':float,
            'UyUz_Cov':float,
            'TsUx_Cov':float,
            'TsUy_Cov':float,
            'TsUz_Cov':float,
            'u_star_R':float,
            'u_Avg_R':float,
            'u_Std_R':float,
            'v_Avg_R':float,
            'v_Std_R':float,
            'w_Avg_R':float,
            'w_Std_R':float,
            'uv_Cov_R':float,
            'uw_Cov_R':float,
            'vw_Cov_R':float,
            'uTs_Cov_R':float,
            'vTs_Cov_R':float,
            'wTs_Cov_R':float,
            'uw_Cov_R_F':float,
            'vw_Cov_R_F':float,
            'wTs_Cov_R_F':float,
            'wTs_Cov_R_F_SND':float,
            'sonic_samples_Tot':'Int64',
            'no_sonic_head_Tot':'Int64',
            'no_new_sonic_data_Tot':'Int64',
            'sonic_amp_l_f_Tot':'Int64',
            'sonic_amp_h_f_Tot':'Int64',
            'sonic_sig_lck_f_Tot':'Int64',
            'sonic_del_T_f_Tot':'Int64',
            'sonic_aq_sig_f_Tot':'Int64',
            'sonic_cal_err_f_Tot':'Int64',
            'UxCO2_Cov':float,
            'UyCO2_Cov':float,
            'UzCO2_Cov':float,
            'UxH2O_Cov':float,
            'UyH2O_Cov':float,
            'UzH2O_Cov':float,
            'uCO2_Cov_R':float,
            'vCO2_Cov_R':float,
            'wCO2_Cov_R':float,
            'uH2O_Cov_R':float,
            'vH2O_Cov_R':float,
            'wH2O_Cov_R':float,
            'wCO2_Cov_R_F':float,
            'wH2O_Cov_R_F':float,
            'CO2_E_WPL_R_F':float,
            'CO2_T_WPL_R_F':float,
            'H2O_E_WPL_R_F':float,
            'H2O_T_WPL_R_F':float,
            'CO2_samples_Tot':'Int64',
            'H2O_samples_Tot':'Int64',
            'no_irga_head_Tot':'Int64',
            'no_new_irga_data_Tot':'Int64',
            'irga_bad_data_f_Tot':'Int64',
            'irga_gen_fault_f_Tot':'Int64',
            'irga_startup_f_Tot':'Int64',
            'irga_motor_spd_f_Tot':'Int64',
            'irga_tec_tmpr_f_Tot':'Int64',
            'irga_src_pwr_f_Tot':'Int64',
            'irga_src_tmpr_f_Tot':'Int64',
            'irga_src_curr_f_Tot':'Int64',
            'irga_off_f_Tot':'Int64',
            'irga_sync_f_Tot':'Int64',
            'irga_amb_tmpr_f_Tot':'Int64',
            'irga_amb_press_f_Tot':'Int64',
            'irga_CO2_I_f_Tot':'Int64',
            'irga_CO2_Io_f_Tot':'Int64',
            'irga_H2O_I_f_Tot':'Int64',
            'irga_H2O_Io_f_Tot':'Int64',
            'irga_CO2_Io_var_f_Tot':'Int64',
            'irga_H2O_Io_var_f_Tot':'Int64',
            'irga_CO2_sig_strgth_f_Tot':'Int64',
            'irga_H2O_sig_strgth_f_Tot':'Int64',
            'irga_cal_err_f_Tot':'Int64',
            'irga_htr_ctrl_off_f_Tot':'Int64',
            'alpha':float,
            'beta':float,
            'gamma':float,
            'height_measurement':float,
            'height_canopy':float,
            'surface_type_text':object,
            'displacement_user':float,
            'd':float,
            'roughness_user':float,
            'z0':float,
            'z':float,
            'L':float,
            'stability_zL':float,
            'iteration_FreqFactor':float,
            'latitude':float,
            'longitude':float,
            'separation_x_irga':float,
            'separation_y_irga':float,
            'separation_lat_dist_irga':float,
            'separation_lag_dist_irga':float,
            'separation_lag_scan_irga':float,
            'MAX_LAG':'Int64',
            'lag_irga':'Int64',
            'FreqFactor_uw_vw':float,
            'FreqFactor_wTs':float,
            'FreqFactor_wCO2_wH2O':float,
            'rho_d_Avg':float,
            'rho_a_Avg':float,
            'Cp':float,
            'Lv':float,
            'batt_V_Avg':float,
            'batt_sens_V_Avg':float,
            'array_V_Avg':float,
            'charge_I_Avg':float,
            'batt_V_slow_Avg':float,
            'heatsink_T_Avg':float,
            'batt_T_Avg':float,
            'reference_V_Avg':float,
            'ah_reset':float,
            'ah_total':float,
            'hourmeter':float,
            'alarm_bits':float,
            'fault_bits':float,
            'dip_num_Avg':float,
            'state_num_Avg':float,
            'pwm_duty_Avg':float,
            'door_is_open_Hst':float,
            'panel_tmpr_Avg':float,
            'batt_volt_Avg':float,
            'slowsequence_Tot':'Int64',
            'process_time_Avg':float,
            'process_time_Max':float,
            'buff_depth_Max':float
        }
    elif dataset_type == "FluxAggregated":
        dtypes = {
            'RECORD':'Int64',
            'Fc_molar':float,
            'Fc_mass':float,
            'Fc_qc_grade':'Int64',
            'Fc_samples_Tot':'Int64',
            'LE':float,
            'LE_qc_grade':'Int64',
            'LE_samples_Tot':'Int64',
            'H':float,
            'H_qc_grade':'Int64',
            'H_samples_Tot':'Int64',
            'Rn':float,
            'G_surface':float,
            'energy_closure':float,
            'Bowen_ratio':float,
            'tau':float,
            'tau_qc_grade':'Int64',
            'u_star':float,
            'T_star':float,
            'TKE':float,
            'amb_tmpr_Avg':float,
            'Td_Avg':float,
            'RH_Avg':float,
            'e_sat_Avg':float,
            'e_Avg':float,
            'amb_press_Avg':float,
            'VPD_air':float,
            'Ux_Avg':float,
            'Ux_Std':float,
            'Uy_Avg':float,
            'Uy_Std':float,
            'Uz_Avg':float,
            'Uz_Std':float,
            'Ts_Avg':float,
            'Ts_Std':float,
            'sonic_azimuth':float,
            'wnd_spd':float,
            'rslt_wnd_spd':float,
            'wnd_dir_sonic':float,
            'std_wnd_dir':float,
            'wnd_dir_compass':float,
            'CO2_molfrac_Avg':float,
            'CO2_mixratio_Avg':float,
            'CO2_Avg':float,
            'CO2_Std':float,
            'H2O_molfrac_Avg':float,
            'H2O_mixratio_Avg':float,
            'H2O_Avg':float,
            'H2O_Std':float,
            'CO2_sig_strgth_Min':float,
            'H2O_sig_strgth_Min':float,
            'T_probe_Avg':float,
            'e_probe_Avg':float,
            'e_sat_probe_Avg':float,
            'Td_probe_Avg':float,
            'H2O_probe_Avg':float,
            'RH_probe_Avg':float,
            'rho_a_probe_Avg':float,
            'rho_d_probe_Avg':float,
            'Precipitation_Tot':float,
            'Rn_meas_Avg':float,
            'NRLITE_SENS':float,
            'PAR_density_Avg':float,
            'QUANTUM_SENS':float,
            'cupvane_WS_Avg':float,
            'cupvane_WS_rslt_Avg':float,
            'cupvane_WD_rslt_Avg':float,
            'cupvane_WD_csi_Std':float,
            'Tsoil_Avg':float,
            'tdr31X_wc_Avg':float,
            'tdr31X_tmpr_Avg':float,
            'tdr31X_E_Avg':float,
            'tdr31X_bulkEC_Avg':float,
            'tdr31X_poreEC_Avg':float,
            'shf_plate_avg':float,
            'SHFP_1_SENS':float,
            'profile_tdr31X_wc_Avg(1)':float,
            'profile_tdr31X_wc_Avg(2)':float,
            'profile_tdr31X_wc_Avg(3)':float,
            'profile_tdr31X_wc_Avg(4)':float,
            'profile_tdr31X_wc_Avg(5)':float,
            'profile_tdr31X_wc_Avg(6)':float,
            'profile_tdr31X_tmpr_Avg(1)':float,
            'profile_tdr31X_tmpr_Avg(2)':float,
            'profile_tdr31X_tmpr_Avg(3)':float,
            'profile_tdr31X_tmpr_Avg(4)':float,
            'profile_tdr31X_tmpr_Avg(5)':float,
            'profile_tdr31X_tmpr_Avg(6)':float,
            'profile_tdr31X_E_Avg(1)':float,
            'profile_tdr31X_E_Avg(2)':float,
            'profile_tdr31X_E_Avg(3)':float,
            'profile_tdr31X_E_Avg(4)':float,
            'profile_tdr31X_E_Avg(5)':float,
            'profile_tdr31X_E_Avg(6)':float,
            'profile_tdr31X_bulkEC_Avg(1)':float,
            'profile_tdr31X_bulkEC_Avg(2)':float,
            'profile_tdr31X_bulkEC_Avg(3)':float,
            'profile_tdr31X_bulkEC_Avg(4)':float,
            'profile_tdr31X_bulkEC_Avg(5)':float,
            'profile_tdr31X_bulkEC_Avg(6)':float,
            'profile_tdr31X_poreEC_Avg(1)':float,
            'profile_tdr31X_poreEC_Avg(2)':float,
            'profile_tdr31X_poreEC_Avg(3)':float,
            'profile_tdr31X_poreEC_Avg(4)':float,
            'profile_tdr31X_poreEC_Avg(5)':float,
            'profile_tdr31X_poreEC_Avg(6)':float,
            'upwnd_dist_intrst':float,
            'FP_dist_intrst':float,
            'FP_max':float,
            'FP_40':float,
            'FP_55':float,
            'FP_90':float,
            'FP_Equation':object,
            'UxUy_Cov':float,
            'UxUz_Cov':float,
            'UyUz_Cov':float,
            'TsUx_Cov':float,
            'TsUy_Cov':float,
            'TsUz_Cov':float,
            'u_star_R':float,
            'u_Avg_R':float,
            'u_Std_R':float,
            'v_Avg_R':float,
            'v_Std_R':float,
            'w_Avg_R':float,
            'w_Std_R':float,
            'uv_Cov_R':float,
            'uw_Cov_R':float,
            'vw_Cov_R':float,
            'uTs_Cov_R':float,
            'vTs_Cov_R':float,
            'wTs_Cov_R':float,
            'uw_Cov_R_F':float,
            'vw_Cov_R_F':float,
            'wTs_Cov_R_F':float,
            'wTs_Cov_R_F_SND':float,
            'sonic_samples_Tot':'Int64',
            'no_sonic_head_Tot':'Int64',
            'no_new_sonic_data_Tot':'Int64',
            'sonic_amp_l_f_Tot':'Int64',
            'sonic_amp_h_f_Tot':'Int64',
            'sonic_sig_lck_f_Tot':'Int64',
            'sonic_del_T_f_Tot':'Int64',
            'sonic_aq_sig_f_Tot':'Int64',
            'sonic_cal_err_f_Tot':'Int64',
            'UxCO2_Cov':float,
            'UyCO2_Cov':float,
            'UzCO2_Cov':float,
            'UxH2O_Cov':float,
            'UyH2O_Cov':float,
            'UzH2O_Cov':float,
            'uCO2_Cov_R':float,
            'vCO2_Cov_R':float,
            'wCO2_Cov_R':float,
            'uH2O_Cov_R':float,
            'vH2O_Cov_R':float,
            'wH2O_Cov_R':float,
            'wCO2_Cov_R_F':float,
            'wH2O_Cov_R_F':float,
            'CO2_E_WPL_R_F':float,
            'CO2_T_WPL_R_F':float,
            'H2O_E_WPL_R_F':float,
            'H2O_T_WPL_R_F':float,
            'CO2_samples_Tot':'Int64',
            'H2O_samples_Tot':'Int64',
            'no_irga_head_Tot':'Int64',
            'no_new_irga_data_Tot':'Int64',
            'irga_bad_data_f_Tot':'Int64',
            'irga_gen_fault_f_Tot':'Int64',
            'irga_startup_f_Tot':'Int64',
            'irga_motor_spd_f_Tot':'Int64',
            'irga_tec_tmpr_f_Tot':'Int64',
            'irga_src_pwr_f_Tot':'Int64',
            'irga_src_tmpr_f_Tot':'Int64',
            'irga_src_curr_f_Tot':'Int64',
            'irga_off_f_Tot':'Int64',
            'irga_sync_f_Tot':'Int64',
            'irga_amb_tmpr_f_Tot':'Int64',
            'irga_amb_press_f_Tot':'Int64',
            'irga_CO2_I_f_Tot':'Int64',
            'irga_CO2_Io_f_Tot':'Int64',
            'irga_H2O_I_f_Tot':'Int64',
            'irga_H2O_Io_f_Tot':'Int64',
            'irga_CO2_Io_var_f_Tot':'Int64',
            'irga_H2O_Io_var_f_Tot':'Int64',
            'irga_CO2_sig_strgth_f_Tot':'Int64',
            'irga_H2O_sig_strgth_f_Tot':'Int64',
            'irga_cal_err_f_Tot':'Int64',
            'irga_htr_ctrl_off_f_Tot':'Int64',
            'alpha':float,
            'beta':float,
            'gamma':float,
            'height_measurement':float,
            'height_canopy':float,
            'surface_type_text':object,
            'displacement_user':float,
            'd':float,
            'roughness_user':float,
            'z0':float,
            'z':float,
            'L':float,
            'stability_zL':float,
            'iteration_FreqFactor':float,
            'latitude':float,
            'longitude':float,
            'separation_x_irga':float,
            'separation_y_irga':float,
            'separation_lat_dist_irga':float,
            'separation_lag_dist_irga':float,
            'separation_lag_scan_irga':float,
            'MAX_LAG':'Int64',
            'lag_irga':'Int64',
            'FreqFactor_uw_vw':float,
            'FreqFactor_wTs':float,
            'FreqFactor_wCO2_wH2O':float,
            'rho_d_Avg':float,
            'rho_a_Avg':float,
            'Cp':float,
            'Lv':float,
            'batt_V_Avg':float,
            'batt_sens_V_Avg':float,
            'array_V_Avg':float,
            'charge_I_Avg':float,
            'batt_V_slow_Avg':float,
            'heatsink_T_Avg':float,
            'batt_T_Avg':float,
            'reference_V_Avg':float,
            'ah_reset':float,
            'ah_total':float,
            'hourmeter':float,
            'alarm_bits':float,
            'fault_bits':float,
            'dip_num_Avg':float,
            'state_num_Avg':float,
            'pwm_duty_Avg':float,
            'door_is_open_Hst':float,
            'panel_tmpr_Avg':float,
            'batt_volt_Avg':float,
            'slowsequence_Tot':'Int64',
            'process_time_Avg':float,
            'process_time_Max':float,
            'buff_depth_Max':float,
            'H_Flags':'Int64',
            'LE_Flags':'Int64',
            'Fc_Flags':'Int64',
            'H_Graded':float,
            'LE_Graded':float,
            'Fc_molar_Graded':float,
            'Tair_Hard_Limit':object,
            'Tair_Change':object,
            'Tair_Day_Change':object,
            'Tair_Filtered':float,
            'RH_Hard_Limit':object,
            'RH_gt_100':object,
            'RH_Change':object,
            'RH_Day_Change':object,
            'RH_Filtered':float,
            'P_Hard_Limit':object,
            'P_Change':object,
            'P_Filtered':float,
            'MSLP':float,
            'MSLP_Hard_Limit':object,
            'MSLP_Change':object,
            'MSLP_Filtered':float,
            'WS_Hard_Limit':object,
            'WS_Change':object,
            'WS_Day_Change':object,
            'WS_Filtered':float,
            'WD_Hard_Limit':object,
            'WD_Change':object,
            'WD_Filtered':float,
            'PAR_Hard_Limit':object,
            'PAR_Change':object,
            'PAR_Day_Change':object,
            'PAR_Filtered':float,
            'Rn_Hard_Limit':object,
            'Rn_Change':object,
            'Rn_Day_Change':object,
            'Rn_Filtered':float,
            'Precip_Hard_Limit':object,
            'Precip_RH_gt_90':object,
            'Precip_Tair_lt_Zero':object,
            'Precip_Filtered':float,
            'VPD_Hard_Limit':object,
            'VPD_Change':object,
            'VPD_Day_Change':object,
            'VPD_Filtered':float,
            'e_Hard_Limit':object,
            'e_Change':object,
            'e_Day_Change':object,
            'e_Filtered':float,
            'e_s_Hard_Limit':object,
            'e_s_Change':object,
            'e_s_Day_Change':object,
            'e_s_Filtered':float
        }

    elif dataset_type == "MetRaw":
        dtypes = {
            'RECORD':float,
            'amb_tmpr_Avg':float,
            'rslt_wnd_spd':float,
            'wnd_dir_compass':float,
            'RH_Avg':float,
            'Precipitation_Tot':float,
            'amb_press_Avg':float,
            'PAR_density_Avg':float,
            'batt_volt_Avg':float,
            'panel_tmpr_Avg':float,
            'std_wnd_dir':float,
            'VPD_air':float,
            'Rn_meas_Avg':float,
            'e_sat':float,
            'e':float,
            'tdr31X_wc_Avg':float,
            'tdr31X_tmpr_Avg':float,
            'tdr31X_E_Avg':float,
            'tdr31X_bulkEC_Avg':float,
            'tdr31X_poreEC_Avg':float,
            'Tsoil_Avg':float,
            'profile_tdr31X_wc_Avg(1)':float,
            'profile_tdr31X_wc_Avg(2)':float,
            'profile_tdr31X_wc_Avg(3)':float,
            'profile_tdr31X_wc_Avg(4)':float,
            'profile_tdr31X_wc_Avg(5)':float,
            'profile_tdr31X_wc_Avg(6)':float,
            'profile_tdr31X_tmpr_Avg(1)':float,
            'profile_tdr31X_tmpr_Avg(2)':float,
            'profile_tdr31X_tmpr_Avg(3)':float,
            'profile_tdr31X_tmpr_Avg(4)':float,
            'profile_tdr31X_tmpr_Avg(5)':float,
            'profile_tdr31X_tmpr_Avg(6)':float,
            'profile_tdr31X_E_Avg(1)':float,
            'profile_tdr31X_E_Avg(2)':float,
            'profile_tdr31X_E_Avg(3)':float,
            'profile_tdr31X_E_Avg(4)':float,
            'profile_tdr31X_E_Avg(5)':float,
            'profile_tdr31X_E_Avg(6)':float,
            'profile_tdr31X_bulkEC_Avg(1)':float,
            'profile_tdr31X_bulkEC_Avg(2)':float,
            'profile_tdr31X_bulkEC_Avg(3)':float,
            'profile_tdr31X_bulkEC_Avg(4)':float,
            'profile_tdr31X_bulkEC_Avg(5)':float,
            'profile_tdr31X_bulkEC_Avg(6)':float,
            'profile_tdr31X_poreEC_Avg(1)':float,
            'profile_tdr31X_poreEC_Avg(2)':float,
            'profile_tdr31X_poreEC_Avg(3)':float,
            'profile_tdr31X_poreEC_Avg(4)':float,
            'profile_tdr31X_poreEC_Avg(5)':float,
            'profile_tdr31X_poreEC_Avg(6)':float,
            'shf_plate_avg':float,
            'SHFP_1_SENS':float
        }

    elif dataset_type == "MetAggregated":
        dtypes = {
            'RECORD':float,
            'amb_tmpr_Avg':float,
            'rslt_wnd_spd':float,
            'wnd_dir_compass':float,
            'RH_Avg':float,
            'Precipitation_Tot':float,
            'amb_press_Avg':float,
            'PAR_density_Avg':float,
            'batt_volt_Avg':float,
            'panel_tmpr_Avg':float,
            'std_wnd_dir':float,
            'VPD_air':float,
            'Rn_meas_Avg':float,
            'e_sat':float,
            'e':float,
            'tdr31X_wc_Avg':float,
            'tdr31X_tmpr_Avg':float,
            'tdr31X_E_Avg':float,
            'tdr31X_bulkEC_Avg':float,
            'tdr31X_poreEC_Avg':float,
            'Tsoil_Avg':float,
            'profile_tdr31X_wc_Avg(1)':float,
            'profile_tdr31X_wc_Avg(2)':float,
            'profile_tdr31X_wc_Avg(3)':float,
            'profile_tdr31X_wc_Avg(4)':float,
            'profile_tdr31X_wc_Avg(5)':float,
            'profile_tdr31X_wc_Avg(6)':float,
            'profile_tdr31X_tmpr_Avg(1)':float,
            'profile_tdr31X_tmpr_Avg(2)':float,
            'profile_tdr31X_tmpr_Avg(3)':float,
            'profile_tdr31X_tmpr_Avg(4)':float,
            'profile_tdr31X_tmpr_Avg(5)':float,
            'profile_tdr31X_tmpr_Avg(6)':float,
            'profile_tdr31X_E_Avg(1)':float,
            'profile_tdr31X_E_Avg(2)':float,
            'profile_tdr31X_E_Avg(3)':float,
            'profile_tdr31X_E_Avg(4)':float,
            'profile_tdr31X_E_Avg(5)':float,
            'profile_tdr31X_E_Avg(6)':float,
            'profile_tdr31X_bulkEC_Avg(1)':float,
            'profile_tdr31X_bulkEC_Avg(2)':float,
            'profile_tdr31X_bulkEC_Avg(3)':float,
            'profile_tdr31X_bulkEC_Avg(4)':float,
            'profile_tdr31X_bulkEC_Avg(5)':float,
            'profile_tdr31X_bulkEC_Avg(6)':float,
            'profile_tdr31X_poreEC_Avg(1)':float,
            'profile_tdr31X_poreEC_Avg(2)':float,
            'profile_tdr31X_poreEC_Avg(3)':float,
            'profile_tdr31X_poreEC_Avg(4)':float,
            'profile_tdr31X_poreEC_Avg(5)':float,
            'profile_tdr31X_poreEC_Avg(6)':float,
            'shf_plate_avg':float,
            'SHFP_1_SENS':float,
            'Tair_Hard_Limit':object,
            'Tair_Change':object,
            'Tair_Day_Change':object,
            'Tair_Filtered':float,
            'RH_Hard_Limit':object,
            'RH_gt_100':object,
            'RH_Change':object,
            'RH_Day_Change':object,
            'RH_Filtered':float,
            'P_Hard_Limit':object,
            'P_Change':object,
            'P_Filtered':float,
            'MSLP':float,
            'MSLP_Hard_Limit':object,
            'MSLP_Change':object,
            'MSLP_Filtered':float,
            'WS_Hard_Limit':object,
            'WS_Change':object,
            'WS_Day_Change':object,
            'WS_Filtered':float,
            'WD_Hard_Limit':object,
            'WD_Change':object,
            'WD_Filtered':float,
            'PAR_Hard_Limit':object,
            'PAR_Change':object,
            'PAR_Day_Change':object,
            'PAR_Filtered':float,
            'Rn_Hard_Limit':object,
            'Rn_Change':object,
            'Rn_Day_Change':object,
            'Rn_Filtered':float,
            'Precip_Hard_Limit':object,
            'Precip_RH_gt_90':object,
            'Precip_Tair_lt_Zero':object,
            'Precip_Filtered':float,
            'VPD_Hard_Limit':object,
            'VPD_Change':object,
            'VPD_Day_Change':object,
            'VPD_Filtered':float,
            'e_Hard_Limit':object,
            'e_Change':object,
            'e_Day_Change':object,
            'e_Filtered':float,
            'e_s_Hard_Limit':object,
            'e_s_Change':object,
            'e_s_Day_Change':object,
            'e_s_Filtered':float
        }

    return dtypes