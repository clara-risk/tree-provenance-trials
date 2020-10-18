# -*- coding: cp1252 -*- #Need to include to use the degree symbol in output file 
import math
import os
from os import path
import warnings
from datetime import date
warnings.simplefilter(action='ignore', category=RuntimeWarning)

import csv
import pandas as pd


# Access the specified table
def get_table(file_path,header):
    '''Get the pandas dataframe of the table you want to join the climate data to.''' #Table must be comma-delimited. 
    if header == False: 
    #Convert to pandas dataframe
        df = pd.read_csv(file_path,header=None) #So the program doesn't think the first row is actually the header (which would cause this record to be lost) 
    else: 
        df = pd.read_csv(file_path) 
    return df #Return the pandas dataframe 

def get_primary_key(pandas_dataframe,data_source_num,species_code):
    '''Get the unique provenance ids from the original provenance id, data source number, and species code.'''
    part_1 = df['Provenance ID'].astype(int).astype(str) #Get all the provenance ids from the dataframe (this is a column) and convert to string
    #part_1 = df['Provenance ID'].astype(str) #Use this line instead of the previous one if the Provenance ID is a string, such as "A, B, C" etc. 
    part_2 = data_source_num 
    part_3 = species_code
    primary_key = part_3+part_2+part_1 #For each incidence in the dataframe, add part 1 (the provenance id) to the other two parts to get the unique id 
    

    return primary_key #return the column of unique ids 
    
def match_primary_key(primary_key,lookup_file): #Elevation is in a separate file so we must handle it separately. 
    '''Function to find the elevation data for the primary keys supplied by get_primary_key.'''
    lookup = lookup_file
    lookup.columns = ['key','long','lat','elevation'] #Specify the columns of the dataframe
    keys = [] 
    for key in primary_key:
        keys.append(key) #Obtaining the list of keys from the dataframe column
    elev = {} #Create a dictionary for the elevation values
    for code in keys: #Loop through each unique code 
        find = lookup.loc[lookup['key'] == code] #Find where the unique code is in the dataframe
        elev[code] = find['elevation'].values.astype(str) #Send the elevation value to the dictionary with the unique code as the key 
    
    return elev #Return the dictionary 


def get_years(years,file_path,test_site_code):
    '''Function to collect the records for each year for each supplied test site code.'''
    data = {}
    for code in test_site_code: #Loop through each test site unique code
        year_dict = {}
        for year in years: #Loop through each year between establishment and mesurement 
            storage = [] 
            if str(year) in os.listdir(file_path): #Select the file that has the correct year in the directory 
                new_path = file_path+'/'+str(year)+'/out'+str(year)+'.txt' #Obtain the file path of the txt file that is inside that folder 
                with open(new_path) as clim_info: 
                    next(clim_info) #Skip the header 
                    for line in clim_info: #Loop through each line 
                        row = line.rstrip('\n').split(' ') #It is space-delimited so break each value by a space 
                        if row[0] == code: #If the first record matches the unique code, proceed 
                            year_dict[year] = row[0:] #Store that row in a dictionary keyed by the year 
        data[code] = year_dict #Store that dictionary in another one with the test site code as the key 
                        
        clim_info.close()

    return data #Return the final dictionary 
    
def get_between_measurements(dictionary,test_site_code):
    '''Function to collect all the values for the climate variables between the planting and measurement years.'''
    dictionary_of_lists = {}
    for code in test_site_code: #Loop through each test site code 
        storage = {}
        length = []
        count = 0 
        for year in dictionary[code].keys(): #Loop through the years
            sub_storage = {}
            information = dictionary[code][year] #Get the climate information for that test site & the specific year 
            if count == 0: 
                length.append(len(information))
            count += 1
            for idx in range(0,len(information)): #Here we are going to index the columns (there should be just over 65, I think 68) and loop through each index
                sub_storage[idx] = information[idx] #Store that value at that index in the dictionary, keyed by the index 
            storage[year] = sub_storage #Store by year in another dictionary 
        
        years = dictionary[code].keys()
        count = 0
        list_idx = list(range(0,length[0]))
        list_str = [str(i) for i in list_idx]
        list_lst = [[] for x in xrange(0,len(list_str))] #Preprepare a list in the correct format 
        for year in years:
            vals = storage[year]
            for key in vals.keys():
                for num in list_str: 
                    if key == int(num):
                        if int(num) >= 4:  
                            list_lst[int(num)].append(vals[key]) #We are going to store a list of each value over the years 

        
       
        dictionary_of_lists[code] = list_lst


    return dictionary_of_lists

        
import math

def calc_between(dictionary_of_lists,export_loc,dictionary,years):
    '''Function to export the tables for the mean, standard deviation, minimum, maximum for the trial sites.'''
    var = 'Test Site Code,Longitude,Latitude,Elevation (m),Mean Diurnal Range (°C),Isothermality 2/7,Temperature Seasonality (°C),Max Temperature Warmest Period (°C),Min Temperature Coldest Period (°C),\
    Temperature Annual Range (°C),Mean Temperature of Wettest Quarter (°C),Mean Temperature Driest Quarter (°C),Mean Temperature Warmest Quarter (°C),\
    Mean Temperature Coldest Quarter (°C),Annual Precipitation (mm) ,Precipitation of Wettest Period (mm),Precipitation of Driest Period (mm),\
    Precipitation Seasonality,Precipitation of Wettest Quarter (mm),Precipitation of Driest Quarter (mm),Precipitation of Warmest Quarter (mm),\
    Precipitation of Coldest Quarter (mm),Start of Growing Season (Julian day),End of Growing Season (Julian day),Length of Growing Season (# days),\
    Total Precipitation for Period 1 (mm),Total Precipitation for Period 3 (mm),Gdd Above Base_Temp for Period 3 (Degree days),Annual Mean Temperature (°C),\
    Annual Minimum Temperature (°C),Annual Maximum Temperature (°C),Mean Temperature Period 3 (°C),Temperature Range for Period 3 (°C),Jan Mean Monthly Min Temp (°C),\
    Feb Mean Monthly Min Temp (°C),Mar Mean Monthly Min Temp (°C),Apr Mean Monthly Min Temp (°C),May Mean Monthly Min Temp (°C),Jun Mean Monthly Min Temp (°C),\
    Jul Mean Monthly Min Temp (°C),Aug Mean Monthly Min Temp (°C),Sep Mean Monthly Min Temp (°C),Oct Mean Monthly Min Temp (°C),Nov Mean Monthly Min Temp (°C),\
    Dec Mean Monthly Min Temp (°C),Jan Mean Monthly Max Temp  (°C),Feb Mean Monthly Max Temp  (°C),Mar Mean Monthly Max Temp  (°C),Apr Mean Monthly Max Temp  (°C),\
    May Mean Monthly Max Temp (°C),Jun Mean Monthly Max Temp  (°C),Jul Mean Monthly Max Temp (°C),Aug Mean Monthly Max Temp  (°C),Sep Mean Monthly Max Temp  (°C),\
    Oct Mean Monthly Max Temp (°C),Nov Mean Monthly Max Temp  (°C),Dec Mean Monthly Max Temp  (°C),Jan Mean Monthly Precipitation (mm),\
    Feb Mean Monthly Precipitation (mm),Mar Mean Monthly Precipitation (mm),Apr Mean Monthly Precipitation (mm),May Mean Monthly Precipitation (mm),\
    Jun Mean Monthly Precipitation (mm),Jul Mean Monthly Precipitation (mm),Aug Mean Monthly Precipitation (mm),Sep Mean Monthly Precipitation (mm),\
    Oct Mean Monthly Precipitation (mm),Nov Mean Monthly Precipitation (mm),Dec Mean Monthly Precipitation (mm)'
    var2 = var.split(',')

    for code in dictionary_of_lists.keys():
        print(code) 
        original_vals = dictionary[code][years[0]]
        list_of_lists = dictionary_of_lists[code]
        list_idx = list(range(0,len(list_of_lists)))
        list_str = [str(i) for i in list_idx]
        list_lst = [[] for x in xrange(0,len(list_str))] #Premake a list to send the calculated variables to 
        count = 0
        for sub_list in list_of_lists:
            #float_list = [float(i) for i in sub_list]
            if count >= 4:
                float_list = [float(i) for i in sub_list] #Convert to float 
                ave = sum(float_list) / len(float_list) #Get the average 
                variance  = sum(pow(val-ave,2) for val in float_list) / len(float_list)  
                stdev  = math.sqrt(variance) #Get the standard deviation 
                minimum = min(float_list)
                maximum = max(float_list)
                list_lst[count].append([round(ave,2),round(stdev,2),round(minimum,2),round(maximum,2)]) #Send to the empty list each variable consequtively 
            count+=1
        count2 = 0
        length = len(years)-1
        file_name = str(years[0])+'-'+str(years[length])+' '+'('+code+').txt'
        f = open(export_loc+file_name,'w+')

        f.write('Variable,Average,Standard Deviation, Minimum, Maximum\n') #Export to the output file 
        for num in range(0,len(var2)):
            for num2 in range(0,len(list_lst)):
                if num == num2:
                    if num >= 4:
                        list_str = [str(i) for i in list_lst[num][0]]
                        to_str = ','.join(list_str)
                        f.write(var2[num]+','+to_str)
                        f.write('\n')
        f.close() 
            
       


def match_primary_key_climate(primary_key,lookup_file): #Very similar to the function for elevation, but this is for climate, which has more than one variable 
    '''Function to search the lookup table for the supplied provenance primary key and send the data from the lookup file to a new dictionary.'''
    lookup = lookup_file 
    lookup_keys = map(str, range(68 + 1)) #The climate variables are denoted with numbers, 0-68 (but we only need 65) 
    keys = [] 
    for key in primary_key:
        keys.append(key)
    vals = {} 
    for code in keys:    
        find = lookup.loc[lookup['0'] == code] #Find the code in the climate file 
        val_list = find.values.astype(str).tolist() #Send the row to a list 
        try: 
            val_str = ','.join(val_list[0][5:]) #Comma-delimit the list 
            vals[code] = val_str #Store in the dictionary 
            print val_str
        except IndexError: #In case there is a code that is not in the lookup table 
            vals[code] = '-9999' #In this case, add a no-data value 
            print('Code not present in lookup table.') 
    
    return vals 



def add_climate_data(file_path,value_dictionary,export_path):
    '''Function to output the trial results with the climate data appended.'''
    new_rows = []
    with open(file_path, 'r') as csvfile:
        next(csvfile)
        for row in csvfile:
            data = row.rstrip('\n').split(',')
            for key in value_dictionary.keys():
                if str(int(float(data[1]))) == key[5:]: #key[5:] is the provenance id #Comment this line out if the provenance id is not a number. 
                #if data[1] == key[5:]: #Uncomment this in the case that the provenance id is not a number. 
                    new_row = data+[str(value_dictionary[key])]
                    new_rows.append(new_row)
            
    f = open(export_path+'output.txt','w+') #Create an output file and write the data with the climate data appended 
    for line in new_rows:
        string = ','.join(line) #Comma-delimit the row 
        f.write(string)
        print(string)
        f.write('\n')

def export_ave_temp(file_path,export_path,lat_lon_lookup): #Only purpose of this function is to calculate the 1961-1990 mean MAT and put the data into a format to be imported to ArcMap 
    '''Function to obtain a summary txt file for the long-term temperature averages for EACH test site in one file, along with the latitude & longitude.'''
    codes = []
    ave = []
    lon = []
    lat = [] 
    for file_name in os.listdir(file_path):
        code = file_name[11:]
        code_formatted = code[:-5]
        codes.append(code_formatted)
        with open(file_path+file_name) as txtfile:
            count = 0
            next(txtfile)
            for line in txtfile:
                row = line.rstrip('\n').split(',')
                if count == 29: #24 is the index of the average temperature 
                    ave.append(row[1])
                count+=1
        txtfile.close() 
        with open(lat_lon_lookup) as lookup:
            for val in lookup:
                row2 = val.rstrip('\n').split(' ')

                if row2[0] == code_formatted:
                    lon.append(row2[1])
                    lat.append(row2[2])
        lookup.close() 
                    
    
        
    f = open(export_path+'output_JMMT.txt','w+')
    for num in range(0,len(codes)):
        f.write(str(codes[num])+','+str(ave[num])+','+str(lat[num])+','+str(lon[num]))
        print(str(codes[num])+','+str(ave[num])+','+str(lat[num])+','+str(lon[num]))
        f.write('\n')   
            
                        
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

    
##    # Here is what to fill out to append climate data to the table with the results 
    file_path = '' #File path to your trial results .csv file 
    df = get_table(file_path,True)
    data_source_num = '28' # If it's a new source, this will be a new number 
    species_code = 'pjp' #pjp - stands for pine jack provenance (likewise, swp would be spruce white provenance) 
    primary_key = get_primary_key(df,data_source_num,species_code)
    lookup = get_table('',True) #Path to climate data file
    val_dict = match_primary_key_climate(primary_key,lookup)
    export_path = '' #Where you want to save the new file. 
    add_climate_data(file_path,val_dict,export_path)

    # Here is what to fill out to get the between measurements (or just one year, in that case, one year is in the list) 
    year1 = 1961 #Replace with your years 
    year2 = 1990 +1 # +1 means that this year (here, 1990, for example) is included. 
    years = list(range(year1,year2))
    print(years)
    dirname = os.path.dirname(__file__)
    file_path = os.path.join(dirname, '') #Path to climate data 
    test_site_codes = [] #Either just one test site or comment out this to run for all the test sites at the same time 
    with open('') as clim_info: #Path to the climate data, such as "ou1961.txt" 
        next(clim_info) 
        for line in clim_info:
            row = line.rstrip('\n').split(' ')
            test_site_codes.append(row[0])
    print(test_site_codes)
    clim_info.close()
    export_loc = '' #Export location 
    dictionary = get_years(years,file_path,test_site_codes)
    print('completed step 1')
    dictionary_of_lists= get_between_measurements(dictionary,test_site_codes)
    print('completed step 2') 
    calc_between(dictionary_of_lists,export_loc,dictionary,years)
    print('exported') 

    #Here is what to fill out for the 1961-1990 summary file for temperature for each test site
    #In the format test site code, lat, lon, mean temp 1961-1990 
    file_path = '' #Path to test site files 
    export_path = '' #Export location 
    lat_lon_lookup = '' #Path to lat lon lookup file, for us it was "testsites_dem.txt" 
    export_ave_temp(file_path,export_path,lat_lon_lookup)
