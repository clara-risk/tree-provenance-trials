import csv
import os
import pandas as pd
import glob
import math, statistics

def get_stat(file_path,stat_type):
    dict_of_val = {} 
    dates = {}
    test_sites = []
    result ={}
    cleaning = {} 

    #Get the file with the widest range of dates
    for subdir, dirs, files in os.walk(file_path):
        for filename in files:
            if filename.endswith('.csv'): 
                test_site = filename[:-5][11:]
                if len(test_site) > 4:
                    test_sites.append(test_site)
                else:
                    test_site = filename[:-5][6:]
                    test_sites.append(test_site)

    test_sites_dates = []
    for unique_site in test_sites:
        test_sites_dates = []
        for subdir, dirs, files in os.walk(file_path): 
            for filename in files:
                if filename.endswith('.csv'):

                    dict_of_appearances = {x:test_sites.count(x) for x in set(test_sites)}

                    Tcount = dict_of_appearances[unique_site]
                    count = 0
                    if count <= Tcount:

                        if filename[:-5][11:] == unique_site:
                            if (filename[0:4],filename[5:9],) not in test_sites_dates: 
                        
                                test_sites_dates.append((filename[0:4],filename[5:9],))
                                dates[unique_site] = test_sites_dates #It will auto-overwrite
                                count += 1
                            

    #For each variable, extract the average (etc.) and store in dictionary                   
    for key in dates.keys():
        #Last character should be a number
        if key[-1].isdigit(): 
        #Get the date
            if len(dates[key]) ==1: #Just 1 date
                
                #Open the csv
                for subdir, dirs, files in os.walk(file_path):
                    for filename in files:
                        if filename.endswith('.csv'):
                            if filename[0].isdigit(): 
                                with open(subdir+'/'+filename, mode='r') as infile:
                                    reader = csv.reader(infile)

                                    sub_dict = {rows[1]:rows[2] for rows in reader}
                                    dict_of_val[filename[:-5][11:]] = sub_dict


            elif len(dates[key]) > 1: #Filter out the longer time periods
                potential_files = dates[key]

                potential_year_combos = {}
                differences = {}
                for years in potential_files:


                    difference = int(years[1])-int(years[0])
                    differences[years] = difference
                    potential_year_combos[key] = differences
                #Get the year key with the greatest difference
                v=list(potential_year_combos[key].values())
                k=list(potential_year_combos[key].keys())
                max_key = k[v.index(max(v))]

                for subdir, dirs, files in os.walk(file_path):
                    for filename in files:
                        if filename.endswith('.csv'):
                            if filename[0].isdigit(): 
                                
                                if max_key[0] in filename and max_key[1] in filename:

                                    with open(subdir+'/'+filename, mode='r') as infile:
                                        reader = csv.reader(infile)
                                        sub_dict = {rows[1]:rows[2] for rows in reader}
                                        dict_of_val[filename[:-5][11:]] = sub_dict 
                                        
            else:
                pass

    listK = dict_of_val.keys()
    for key in listK:
        if key[-1].isdigit() and len(key) > 1:

            cleaning[key] = dict_of_val[key]
            
    for _,value in cleaning.items():
        for key,val in value.items():
            if val[-1].isdigit(): 
                result.setdefault(key, []).append(float(val))

    final_mean = {i:round(sum(result[i])/len(result[i]),2) for i in result}

    #Check if correct length
    length_list = [] 
    for k,v in result.items():
        length_list.append(len(v)) 

    if len(set(length_list)) > 1:
        print('Not enough values for one of the variables! Please check data!')

    #Calculate the average of the values in the dictionary for each variable
    #Dictionary is structured as follows test_site | variable | value
        
    out_path = os.getcwd()
    #stats = mean, stdev, max, min 

    with open(out_path+'/'+stat_type+'.csv', 'w', newline='') as csv_file:  
        writer = csv.writer(csv_file)
        if stat_type == 'mean': 
            for key, value in final_mean.items():
               writer.writerow([key, value])
        elif stat_type == 'stdev':
            #Take the standard deviation of the means of each test site
            final_stdev = {i:round(statistics.stdev(result[i]),2) for i in result}
            for key, value in final_stdev.items():
               writer.writerow([key, value])
        elif stat_type == 'max':
            final_max = {i:round(max(result[i]),2) for i in result}
            for key, value in final_max.items():
               writer.writerow([key, value])
        elif stat_type == 'min':
                   
            final_min = {i:round(min(result[i]),2) for i in result}
            for key, value in final_min.items():
               writer.writerow([key, value])

        else:
            print('That is not a valid stat type!') 
               
def get_provenance_stat(file_path,var):
    overall_dict = {} 
    for subdir, dirs, files in os.walk(file_path):
        for filename in files:
            if filename.endswith('.csv'):
                try:

                    df = pd.read_csv(subdir+'/'+filename,engine='python')
                    lats = df['Latitude']
                    lons = df['Longitude']
                    vals = df[var]

                    for Lat,Lon,Val in zip(lats,lons,vals):
                        if Val != -9999: #in case of no data 
                            overall_dict[(Lat,Lon,)] = Val
                            
                except:
                    print(var)
                    print(filename)

    print(overall_dict) 
    if len(overall_dict.keys()) > 0:
        mean_val = round(sum(overall_dict.values())/len(overall_dict.values()),2)
        stdev_val = round(statistics.stdev(overall_dict.values()),2)
        min_val = round(min(overall_dict.values()),2)
        max_val = round(max(overall_dict.values()),2)
        print(mean_val)
        return mean_val,stdev_val,min_val,max_val
    else:
        return -9999,-9999,-9999,-9999
                          

def combine_provenance_stat(database_path,value_names,stat_type):
    dictionary_send_to_file = {}
    for var_name in value_names:
        averaged,stdev,minimum,maximum = get_provenance_stat(database_path,var_name)
        if stat_type == 'mean': 
            dictionary_send_to_file[var_name] = averaged
        elif stat_type == 'stdev':
            dictionary_send_to_file[var_name] = stdev
        elif stat_type == 'min':
            dictionary_send_to_file[var_name] = minimum
        elif stat_type == 'max':
            dictionary_send_to_file[var_name] = maximum
        else:
            print('That is not a valid statistics type!') 
            
    out_path = os.getcwd()
    with open(out_path+'/'+stat_type+'_RES.csv', 'w', newline='') as csv_file:  
        writer = csv.writer(csv_file)
        
        for key, value in dictionary_send_to_file.items():
            writer.writerow([key, value])

if __name__ == "__main__":
    file_path = '' #insert path to test sites database
    get_stat(file_path,'min') #mean, stdev, max, min 
    file_path_trial = '' #insert path to trial sites database
    
    value_names = ['Mean Diurnal Range (°C)','Isothermality 2/7','Temperature Seasonality (°C)','Max Temperature Warmest Period (°C)','Min Temperature Coldest Period (°C)',\
    'Temperature Annual Range (°C)','Mean Temperature of Wettest Quarter (°C)','Mean Temperature Driest Quarter (°C)','Mean Temperature Warmest Quarter (°C)',\
    'Mean Temperature Coldest Quarter (°C)','Annual Precipitation (mm)','Precipitation of Wettest Period (mm)','Precipitation of Driest Period (mm)',\
    'Precipitation Seasonality','Precipitation of Wettest Quarter (mm)','Precipitation of Driest Quarter (mm)','Precipitation of Warmest Quarter (mm)',\
    'Precipitation of Coldest Quarter (mm)','Start of Growing Season (Julian day)','End of Growing Season (Julian day)','Length of Growing Season (# days)',\
    'Total Precipitation for Period 1 (mm)','Total Precipitation for Period 3 (mm)','Gdd Above Base_Temp for Period 3 (Degree days)','Annual Mean Temperature (°C)',\
    'Annual Minimum Temperature (°C)','Annual Maximum Temperature (°C)','Mean Temperature Period 3 (°C)','Temperature Range for Period 3 (°C)','Jan Mean Monthly Min Temp (°C)',\
    'Feb Mean Monthly Min Temp (°C)','Mar Mean Monthly Min Temp (°C)','Apr Mean Monthly Min Temp (°C)','May Mean Monthly Min Temp (°C)','Jun Mean Monthly Min Temp (°C)',\
    'Jul Mean Monthly Min Temp (°C)','Aug Mean Monthly Min Temp (°C)','Sep Mean Monthly Min Temp (°C)','Oct Mean Monthly Min Temp (°C)','Nov Mean Monthly Min Temp (°C)',\
    'Dec Mean Monthly Min Temp (°C)','Jan Mean Monthly Max Temp  (°C)','Feb Mean Monthly Max Temp  (°C)','Mar Mean Monthly Max Temp  (°C)','Apr Mean Monthly Max Temp  (°C)',\
    'May Mean Monthly Max Temp  (°C)','Jun Mean Monthly Max Temp  (°C)','Jul Mean Monthly Max Temp  (°C)','Aug Mean Monthly Max Temp  (°C)','Sep Mean Monthly Max Temp  (°C)',\
    'Oct Mean Monthly Max Temp  (°C)','Nov Mean Monthly Max Temp  (°C)','Dec Mean Monthly Max Temp  (°C)','Jan Mean Monthly Precipitation (mm)',\
    'Feb Mean Monthly Precipitation (mm)','Mar Mean Monthly Precipitation (mm)','Apr Mean Monthly Precipitation (mm)','May Mean Monthly Precipitation (mm)',\
    'Jun Mean Monthly Precipitation (mm)','Jul Mean Monthly Precipitation (mm)','Aug Mean Monthly Precipitation (mm)','Sep Mean Monthly Precipitation (mm)',\
    'Oct Mean Monthly Precipitation (mm)','Nov Mean Monthly Precipitation (mm)','Dec Mean Monthly Precipitation (mm)']
    combine_provenance_stat(file_path2,value_names,'min') #mean, stdev, max, min 
    
