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
               


if __name__ == "__main__":
    file_path = '' #insert path to trial sites database
    get_stat(file_path,'min') #mean, stdev, max, min 
