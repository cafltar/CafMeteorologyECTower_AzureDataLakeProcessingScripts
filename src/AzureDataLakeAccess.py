# -*- coding: utf-8 -*-
"""
Created on Mon Jun  1 10:59:51 2020
@author: Eric Russell, Assistant Research Professor, CEE WSU
contact: eric.s.russell@wsu.edu
Library of functions for the Azure Data Lake download codeset; see the readme within this repo for more details about the different scripts used
Comments in this are specific to the functions
"""
# General library imports for functions; some functions have the import statements as part of the function
import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator

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

def indx_fill(df, frq):   
    # Fills in missing index values for a continuous time series. Rows are left blank.
    df.index = pd.to_datetime(df.index)
    # Sort index in case it came in out of order, a possibility depending on filenames and naming scheme
    df = df.sort_index()
    # Remove any duplicate times, can occur if files from mixed sources and have overlapping endpoints
    df = df[~df.index.duplicated(keep='first')]
        # Fill in missing times due to tower being down and pad dataframe to midnight of the first and last day
    idx = pd.date_range(df.index[0].floor('D'),df.index[len(df.index)-1].ceil('D'),freq = frq)
    # Reindex the dataframe with the new index and fill the missing values with NaN/blanks
    df = df.reindex(idx, fill_value=np.NaN)
    return df

def Fast_Read(filenames, hdr, idxfll):
    #Check to make sure there are files within the directory and doesn't error
    if len(filenames) == 0:
        print('No Files in directory, check the path name.')
        return  # 'exit' function and return error
    elif (len(filenames) > 0) & (hdr ==4): # hdr == 4 is for data direct from the data logger as there are four header lines
        #Initialize dataframe used within function
        Final = [];Final = pd.DataFrame(Final)
        for k in range (0,len(filenames)):
            #Read in data and concat to one dataframe; no processing until data all read in
            df = pd.read_csv(filenames[k],index_col = 'TIMESTAMP',header= 1,skiprows=[2,3],low_memory=False)
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
            df = pd.read_csv(filenames[k],index_col = 'TIMESTAMP',header= 0,low_memory=False)
            Final = pd.concat([Final,df], sort = False)
        # Convert time index
        Out = indx_fill(Final,idxfll)
        Out.index = pd.to_datetime(Out.index)
        Out = Out.sort_index()
    return Out # Return dataframe to main function.    

def Data_Update_Azure(access, s,col, siteName):
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
                    local_file = open(localfile+z[back:],'wb'); print(local_file)                
                    file_client = file_system_client.get_file_client(z)
                    download = file_client.download_file()
                    downloaded_bytes = download.readall()
                    local_file.write(downloaded_bytes)
                    local_file.close()
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
        
def wateryear():
    # Calculate what the wateryear is; checks if it is Ooctober or not; if so then adds one to the year to get to the correct water year. 
    from datetime import date
    if int(str(date.today()).replace('-','')[4:6]) < 10:
        wateryear = str(date.today()).replace('-','')[0:4]
    else:
        wateryear = str(int(str(date.today()).replace('-','')[0:4])+1)
    return wateryear # Returns water year as a string.
    
def AccessAzure(Sites, col, Time,access,CEF,save=True, QC = True,startDate=None):
    # Main driver function of the datalake access and QC functions, called from the main driver of the codeset.
    import glob
    import datetime
    import pandas as pd
    from datetime import date
    from dateutil import parser
    # Collect which column, met or flux
    ver = access[col]['Ver']
    cy = wateryear() # Determine wateryear to build file path
    if startDate is None:
        CE = Fast_Read(glob.glob(CEF),1, Time) # Read in the previous aggregated file(s)
        s = str(CE.index[-1])[0:10]; s= s.replace('-', '') # Find the last index in the file and convert to a string
        if int(s[6:])>1: # Check if it is the first day of the month or not to go back a day for the file collection later.
            s = datetime.date(int(s[0:4]), int(s[4:6]), int(s[6:])-1)
        else: s = datetime.date(int(s[0:4]), int(s[4:6]), int(s[6:]))
    else: s = parser.parse(startDate).date()
    
    print('Downloading files')
    Data_Update_Azure(access, s, col, Sites) # Call function to update the Azure data
    print('Reading '+ Sites)
    if not pd.isna(access[col]['LOCAL_DIRECT']):
        filenames = glob.glob(access[col]['LOCAL_DIRECT']+'\\*.dat') # Gather all the filenames just downloaded
    else: filenames = glob.glob(access[col]["inputPath"] + '\\' + Sites + '\\*.dat')
    CEN = Fast_Read(filenames, 4,Time) # Read in new files
    if 'CE' in locals():
        CE=pd.concat([CE,CEN], sort = False) # Concat new files the main aggregated file
    else: CE = CEN
    CE = CE.sort_index() # Sort index
    CE = CE.dropna(subset=['RECORD']) # Drop any row that has a NaN/blank in the "RECORD" number column; removes the overlap-extra rows added from the previous run
    CE = indx_fill(CE,Time) # Fill back in the index through to the end of the current day
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
        #CEF = (CEF[:-4]+tag).replace('*','') # replace wildcards that were used for glob
        
        today = str(date.today()).replace('-','') # Replace dashes within datestring to make one continuous string
        fname = Sites+'_'+col+'_AggregateQC_CY'+cy+'_'+ver+'_'+today+'.csv' # Build filename for uploaded file based on tyrannical data manager's specifications
        fpath = access[col]["outputPath"] + '\\' + Sites + '\\' + fname
        
        CE.to_csv(fpath, index_label = 'TIMESTAMP') # Print new aggregated file to local machine for local copy

        print('Uploading data')
        
        # TODO: Enable uploading to DL soon (removed during testing 01/27/2021 by brc)
        AggregatedUploadAzure(fname, access, col,fpath,cy) # Send info to upload function
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
        Q['RH_Filtered'] = Q['RH_Filtered'].replace(to_replace=Q['RH_Filtered'][Q['RH_gt_100']], value = 100)
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
            Q['Precip_Filtered'] = Q['Precip_Filtered'].replace(to_replace=Q['Precip_Filtered'][Z_Precip], value = 0)
            Q.drop(columns=[Precip.columns[0]],inplace=True)
        elif ('RH' in kwargs.keys()) & ('Tair' not in kwargs.keys()):
            Q['Precip_RH_gt_90'] = (Q[Precip.columns[0]].astype(float) > 0) & (Q['RH_Filtered'].astype(float) >= 90)
            Q['Precip_Filtered'] = Q[Precip.columns[0]][Q['Precip_Hard_Limit']&Q['Precip_RH']]
            Q['Precip_Filtered'] = Q['Precip_Filtered'].replace(to_replace=Q['Precip_Filtered'][Z_Precip], value = 0)
            Q.drop(columns=[Precip.columns[0]],inplace=True)
        elif ('RH' not in kwargs.keys()) & ('Tair' in kwargs.keys()):
            Q['Precip_Tair_lt_Zero'] = (Q[Precip.columns[0]].astype(float) > 0) & (Q['Tair_Filtered'] < 0)
            Q['Precip_Filtered'] = Q[Precip.columns[0]][Q['Precip_Hard_Limit']& ~Q['Precip_Tair_lt_Zero']]
            Q['Precip_Filtered'] = Q['Precip_Filtered'].replace(to_replace=Q['Precip_Filtered'][Z_Precip], value = 0)
            Q.drop(columns=[Precip.columns[0]],inplace=True)
        else:
            Q['Precip_Filtered'] = Q[Precip.columns[0]][Q['Precip_Hard_Limit']]
            Q['Precip_Filtered'] = Q['Precip_Filtered'].replace(to_replace=Q['Precip_Filtered'][Z_Precip], value = 0)
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
 