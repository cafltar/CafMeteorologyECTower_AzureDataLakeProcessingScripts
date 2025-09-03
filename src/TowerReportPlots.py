# -*- coding: utf-8 -*-
"""
Created on Thu Jul  9 15:55:41 2020

@author: russe
@author: Eddie Steiner
"""

import glob
import os
import pathlib
import matplotlib.pyplot as plt
import datetime
import numpy as np
from matplotlib.backends.backend_pdf import PdfPages
import AzureDataLakeAccess as ADLA

def TowerReport(pathToAggregatedFiles, startdate=None, enddate=None):
    #stations = ['CookEast', 'CookWest', 'BoydNorth', 'BoydSouth']
    stations = ['CookEast', 'CookWest']
    data_frames = {}
    missing_stations = []
    invalid_vars = []
    
    # Reading data for each station
    for station in stations:
        print(f'Reading {station}')
        filenames = glob.glob(f"{pathToAggregatedFiles}\\{station}\\Flux\\{station}*Flux*.csv")
        
        try:
            data = ADLA.Fast_Read([ADLA.get_latest_file(filenames)], 1, '30min')
            if data.empty:
                raise ValueError(f"No data found for {station}")
            data_frames[station] = data
        except Exception as e:
            print(f"Error reading data for {station}: {e}")
            missing_stations.append(station)
            continue
    
    # Check if any data was successfully read
    if not data_frames:
        print("No valid data found for any stations.")
        return
    
    # Filter by startdate and enddate
    if startdate is not None:
        for station in data_frames:
            data_frames[station] = data_frames[station][data_frames[station].index > startdate]
    if enddate is not None:
        for station in data_frames:
            data_frames[station] = data_frames[station][data_frames[station].index < enddate]
    
    if missing_stations:
        print(f"Missing data for the following stations: {', '.join(missing_stations)}")
    
    # Assuming at least one valid dataset is present
    valid_station = next(iter(data_frames))
    s = data_frames[valid_station].index[-1] - datetime.timedelta(days=+10)
    e = data_frames[valid_station].index[-1]
    
    for station in data_frames:
        data_frames[station] = data_frames[station][s:e]
    
    s = str(data_frames[valid_station].index[-1] - datetime.timedelta(days=+7))[0:10].replace('-', '')
    e = str(data_frames[valid_station].index[-1])[0:10].replace('-', '')
    
    path_to_drive = pathlib.Path('G:\Shared drives\CafMeteorologyECTower\Documents\TowerReports')
    path_to_file = path_to_drive / f'CAFLTARTowerReport{s}_{e}.pdf'
    pdf_pages = PdfPages(str(path_to_file))
    
    # Plotting for each variable group

    #UPDATE THESE WITH NEW VARIABLES **********************************************************************
   
    variable_groups = {
        "Heat and Energy Fluxes": ['H', 'LE', 'FC_mass'],
        "Temperature Variables": ['TA_1_1_1', 'TA_1_1_2', 'T_SONIC'],
        "Humidity and Precipitation": ['RH_1_1_1', 'RH_1_1_3', 'P'],
        "Wind and Friction": ['USTAR', 'FETCH_90'],
        "Radiation and Photosynthetically Active Radiation": ['PPFD_IN'],
        "Wind Components": ['Ux', 'Uy', 'Uz'],
        "Flux Sample Totals": ['FC_samples', 'LE_samples', 'H_samples'],
        "Signal Strengths": ['CO2_sig_strgth_Min', 'H2O_sig_strgth_Min'],
        "Soil Temperature and Water Content (Shallow)": ['TS_TDR31X_1_1_1']
    }

    #***************************************************************************************************

    for category_label, vars_to_plot in variable_groups.items():
        fig = plt.figure(figsize=(8, 8)) 
        figure_plotted = False  # Track if any data is plotted for this figure
        
        # Add category label as a title
        fig.suptitle(category_label, fontsize=14, fontweight='bold')

        for idx, var in enumerate(vars_to_plot, 1):
            ax = plt.subplot(len(vars_to_plot), 1, idx)
            plotted = False  # Track if any data is plotted for this variable
            
            for station in data_frames:
                if var in data_frames[station].columns:
                    if not data_frames[station][var].empty:
                        plt.plot(data_frames[station][var].astype(float), label=station)
                        plotted = True
                        figure_plotted = True  # Mark figure as having plotted data
                    else:
                        print(f"Warning: {var} for {station} is empty.")
                else:
                    print(f"Warning: {var} not found in {station}.")
            
            if plotted:
                plt.legend(fontsize=8)
            else:
                print(f"Warning: No data plotted for {var} in any station.")
                invalid_vars.append(var)
            
            plt.ylabel(f'{var}', fontsize=12)
            plt.xticks(rotation=45, ha='right')  # Rotate x-axis labels for clarity

        if figure_plotted:  # Save the figure only if at least one variable was plotted
            plt.tight_layout(rect=[0, 0.03, 1, 0.95])  # Adjust layout to fit title
            pdf_pages.savefig(fig)
        else:
            plt.close(fig)  # Close the figure without saving if no data was plotted

    print("Variables not found or empty: ", invalid_vars)

    pdf_pages.close()
