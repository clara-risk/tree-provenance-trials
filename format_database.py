# -*- coding: cp1252 -*- #Need to include to use the degree symbol in output file 
import math
import os
import ntpath 
import warnings
from datetime import date
warnings.simplefilter(action='ignore', category=RuntimeWarning)
import sys
import csv
import pandas as pd
import numpy as np 
import format_clim_data_for_databases as fc

    
def reformat_mean_height(origin_dataframe):
    '''Convert the mean height data in the database to the following format: Mean Height (m), Age at Measurement.
    This is for easier data analysis.'''
    headers = list(origin_dataframe)

    var = 'Mean Height'
    index = [i for i, elem in enumerate(headers) if var in elem]
    if len(index) > 0:
        if len(index) == 1: 
            col_val = headers[index[0]]
            #We just need to create a column in the df for Age at Measurement
            age = [int(x) for x in col_val.split() if x.isdigit()]
            origin_dataframe = origin_dataframe.assign(col_name=age[0]) #Add the age column
            origin_dataframe.rename(columns={'col_name':'Age at Height Measurement (years)'}, inplace=True)
            origin_dataframe.rename(columns={col_val:'Mean Height (m)'}, inplace=True)
            new_df = origin_dataframe.copy()


        elif len(index) > 1:
            col_val_list = []
            ages = []
            for idx in index:
                col_val_list.append(headers[idx])

            for header_name in col_val_list:
                #age= [float(x) for x in header_name.split(' ') if x.isnumeric()] #can't catch the decimal for 1.5 years
                header_sep =header_name.split(' ')

                for element in header_sep:
                    try:
                        ages.append([float(element)])
                    except:
                        pass

            origin_dataframe = origin_dataframe.assign(col_name=ages[0][0])
            origin_dataframe.rename(columns={'col_name':'Age at Height Measurement (years)'}, inplace=True)
            origin_dataframe.rename(columns={col_val_list[0]:'Mean Height (m)'}, inplace=True)
            count = 1
            copied_dfs = []

            for header_name in col_val_list[1:]:#Skip the first one, which name we just overwrote, will stay the same.
                if count >= 1:

                    copy_df = origin_dataframe.copy()
                    copy_df['Mean Height (m)'] = copy_df[header_name]
                    copy_df['Age at Height Measurement (years)'] = ages[count][0]
                    copy_df.drop(header_name, axis=1, inplace=True)
                    origin_dataframe.drop(header_name, axis=1, inplace=True)  
                    copied_dfs.append(copy_df)
                    count+=1
                else:
                    count+=1

            new_df = origin_dataframe.append(copied_dfs) #Append all the edited, copied dataframes
            new_headers = list(new_df)
            for header_name in col_val_list:
                if header_name in new_headers:
                    new_df.drop(header_name, axis=1, inplace=True)  
            new_df['ID'] = np.arange(len(new_df))

            #print(new_df[['Provenance ID','Mean Height (m)','Age at Height Measurement (years)']])

    else:
        print('No mean height record detected in table.')
        #In this case, add empty column with -9999
        origin_dataframe = origin_dataframe.assign(col_name1=-9999) #Add the height column
        origin_dataframe.rename(columns={'col_name1':'Mean Height (m)'}, inplace=True)
        origin_dataframe = origin_dataframe.assign(col_name2=-9999) #Add the age column
        origin_dataframe.rename(columns={'col_name2':'Age at Height Measurement (years)'}, inplace=True)
        new_df = origin_dataframe.copy()


    return new_df

def reformat_survival(origin_dataframe):
    '''Convert the survival data in the database to the following format: Survival (%), Age at Measurement.
    This is for easier data analysis.'''
    headers = list(origin_dataframe)

    var = 'Survival'
    index = [i for i, elem in enumerate(headers) if var in elem]
    if len(index) > 0:
        if len(index) == 1: 
            col_val = headers[index[0]]
            #We just need to create a column in the df for Age at Measurement
            age = [float(x) for x in col_val.split() if x.isdigit()]
            origin_dataframe = origin_dataframe.assign(col_name=age[0]) #Add the age column
            origin_dataframe.rename(columns={'col_name':'Age at Survival Measurement (years)'}, inplace=True)
            origin_dataframe.rename(columns={col_val:'Survival (%)'}, inplace=True)
            new_df = origin_dataframe.copy()
            try: 
                first_cols = ['ID','Provenance ID','Origin','Latitude','Longitude','Elevation (m)','Mean Height (m)','Survival (%)',\
                          'Age at Height Measurement (years)','Age at Survival Measurement (years)']
                new_df = new_df[ first_cols + [ col for col in new_df.columns if col not in first_cols ] ]
            except:
                first_cols = ['ID','Provenance ID','Origin','Latitude','Longitude','Mean Height (m)','Survival (%)',\
                          'Age at Height Measurement (years)','Age at Survival Measurement (years)']
                new_df = new_df[ first_cols + [ col for col in new_df.columns if col not in first_cols ] ]
            #print(new_df[['Provenance ID','Survival (%)','Age at Survival Measurement (years)']])

        elif len(index) > 1:

            col_val_list = []
            ages = []
            for idx in index:
                col_val_list.append(headers[idx])
            for header_name in col_val_list:
                header_sep =header_name.split(' ')
                for element in header_sep: 
                    try: 
                        ages.append([float(element)])
                    except:
                        pass
            
            origin_dataframe = origin_dataframe.assign(col_name=ages[0][0])
            origin_dataframe.rename(columns={'col_name':'Age at Survival Measurement (years)'}, inplace=True)
            origin_dataframe.rename(columns={col_val_list[0]:'Survival (%)'}, inplace=True)
            count = 0
            copied_dfs = []
            for header_name in col_val_list:
                if count >= 1: #Skip the first one, which name we just overwrote, will stay the same. 
                    copy_df = origin_dataframe.copy()
                    copy_df['Survival (%)'] = copy_df[header_name]
                    copy_df['Age at Survival Measurement (years)'] = ages[count][0]
                    copy_df.drop(header_name, axis=1, inplace=True)
                    origin_dataframe.drop(header_name, axis=1, inplace=True)  
                    copied_dfs.append(copy_df)
                else:
                    pass
                count+=1

            new_df = origin_dataframe.append(copied_dfs) #Append all the edited, copied dataframes
            new_df['ID'] = np.arange(len(new_df))
            try: 
                first_cols = ['ID','Provenance ID','Origin','Latitude','Longitude','Elevation (m)','Mean Height (m)','Survival (%)',\
                          'Age at Height Measurement (years)','Age at Survival Measurement (years)']
                new_df = new_df[ first_cols + [ col for col in new_df.columns if col not in first_cols ] ]
            except: 
                first_cols = ['ID','Provenance ID','Origin','Latitude','Longitude','Mean Height (m)','Survival (%)',\
                          'Age at Height Measurement (years)','Age at Survival Measurement (years)']
                new_df = new_df[ first_cols + [ col for col in new_df.columns if col not in first_cols ] ]


    else:
        print('No survival record detected in table.')
        #In this case, add empty column with -9999
        origin_dataframe = origin_dataframe.assign(col_name1=-9999) #Add the height column
        origin_dataframe.rename(columns={'col_name1':'Survival (%)'}, inplace=True)
        origin_dataframe = origin_dataframe.assign(col_name2=-9999) #Add the age column
        origin_dataframe.rename(columns={'col_name2':'Age at Survival Measurement (years)'}, inplace=True)
        new_df = origin_dataframe.copy()
        try: 
            first_cols = ['ID','Provenance ID','Origin','Latitude','Longitude','Elevation (m)','Mean Height (m)','Survival (%)',\
                          'Age at Height Measurement (years)','Age at Survival Measurement (years)']
            new_df = new_df[ first_cols + [ col for col in new_df.columns if col not in first_cols ] ]
        except: 
            first_cols = ['ID','Provenance ID','Origin','Latitude','Longitude','Mean Height (m)','Survival (%)',\
                          'Age at Height Measurement (years)','Age at Survival Measurement (years)']
            new_df = new_df[ first_cols + [ col for col in new_df.columns if col not in first_cols ] ]
    return new_df


def print_to_csv(pandas_dataframe,file_name,export_path):
    '''Print the pandas dataframe to a .csv file.
    '''
    pandas_dataframe.to_csv(export_path+file_name, sep=',', encoding='cp1252',index=False)
    
def print_to_txt(pandas_dataframe,file,export_path):
    '''Print the pandas dataframe to a .txt file.
    '''
    tfile = open(export_path+file[:-4]+'.txt', 'a') #'a' == append
    string_format= pandas_dataframe.to_string(index=False).split('\n')
    df = [','.join(element.split()) for element in string_format]
    df = [element.replace('_',' ') for element in df]
    df = '\r'.join(df)
    tfile.write(df)
    tfile.close()

    
if __name__ == "__main__": 
    #Headers
##    Mean Diurnal Range (°C),Isothermality 2/7,Temperature Seasonality (°C),Max Temperature Warmest Period (°C),Min Temperature Coldest Period (°C),\
##    Temperature Annual Range (°C),Mean Temperature of Wettest Quarter (°C),Mean Temperature Driest Quarter (°C),Mean Temperature Warmest Quarter (°C),\
##    Mean Temperature Coldest Quarter (°C),Annual Precipitation (mm) ,Precipitation of Wettest Period (mm),Precipitation of Driest Period (mm),\
##    Precipitation Seasonality,Precipitation of Wettest Quarter (mm),Precipitation of Driest Quarter (mm),Precipitation of Warmest Quarter (mm),\
##    Precipitation of Coldest Quarter (mm),Start of Growing Season (Julian day),End of Growing Season (Julian day),Length of Growing Season (# days),\
##    Total Precipitation for Period 1 (mm),Total Precipitation for Period 3 (mm),Gdd Above Base_Temp for Period 3 (Degree days),Annual Mean Temperature (°C),\
##    Annual Minimum Temperature (°C),Annual Maximum Temperature (°C),Mean Temperature Period 3 (°C),Temperature Range for Period 3 (°C),Jan Mean Monthly Min Temp (°C),\
##    Feb Mean Monthly Min Temp (°C),Mar Mean Monthly Min Temp (°C),Apr Mean Monthly Min Temp (°C),May Mean Monthly Min Temp (°C),Jun Mean Monthly Min Temp (°C),\
##    Jul Mean Monthly Min Temp (°C),Aug Mean Monthly Min Temp (°C),Sep Mean Monthly Min Temp (°C),Oct Mean Monthly Min Temp (°C),Nov Mean Monthly Min Temp (°C),\
##    Dec Mean Monthly Min Temp (°C),Jan Mean Monthly Max Temp  (°C),Feb Mean Monthly Max Temp  (°C),Mar Mean Monthly Max Temp  (°C),Apr Mean Monthly Max Temp  (°C),\
##    May Mean Monthly Max Temp  (°C),Jun Mean Monthly Max Temp  (°C),Jul Mean Monthly Max Temp (°C),Aug Mean Monthly Max Temp  (°C),Sep Mean Monthly Max Temp  (°C),\
##    Oct Mean Monthly Max Temp  (°C),Nov Mean Monthly Max Temp  (°C),Dec Mean Monthly Max Temp  (°C),Jan Mean Monthly Precipitation (mm),\
##    Feb Mean Monthly Precipitation (mm),Mar Mean Monthly Precipitation (mm),Apr Mean Monthly Precipitation (mm),May Mean Monthly Precipitation (mm),\
##    Jun Mean Monthly Precipitation (mm),Jul Mean Monthly Precipitation (mm),Aug Mean Monthly Precipitation (mm),Sep Mean Monthly Precipitation (mm),\
##    Oct Mean Monthly Precipitation (mm),Nov Mean Monthly Precipitation (mm),Dec Mean Monthly Precipitation (mm)

    file_path = '' #path to species folder, i.e. folder containing all black spruce files 
    export_path = '' #path to folder you want to save the new files to

    for file in os.listdir(file_path):
        print(file)
        original_dataframe = fc.get_table(file_path+file,True)
        reformat_df1 = reformat_mean_height(original_dataframe)
        reformat_df2 = reformat_survival(reformat_df1)
        print_to_csv(reformat_df2,file,export_path)
        print_to_txt(reformat_df2,file,export_path)
