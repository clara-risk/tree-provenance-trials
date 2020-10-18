import math
import os
from os import path
import warnings
from datetime import date
warnings.simplefilter(action='ignore', category=RuntimeWarning)

def get_new_dates(start_date,file_path,col_num,export_path):
    '''This function takes the number of days from a certain date and converts it into number of days since January 1
    of that year. Whether or not the year is a leap year is considered.'''

    new_dates = [] # Create an empty list to store the converted dates until they are written to the output file. 
    year = start_date[0] # Access the input start date year. 
    month = start_date[1] # Access the input start date month. 
    day = start_date[2] # Access the input start date day. 
    d_end = date(year,month,day) # Day from which the days are measured in the input dataset. 
    d_start = date(year, 1, 1) # The start date is January 1. 
    num_days = d_end - d_start # Number of days between January 1 and the date used for measurement in the input dataset. 
    convert_to_days = int(num_days.days)+1 # Add one to include d_end in the conversion number. 

    with open(file_path) as original_dates: #Accessing the input dataset. 
        next(original_dates) #Skip the header, because we don't need to convert this. 
        for line in original_dates: # Loop through each line in the input dataset. 
            new_line = line.rstrip('\n').split(',') # It is a comma-delimited txt file, so split values based on this. 
            original_date = new_line[col_num] # Obtain the value from the column we are interested in converting. 
            if float(original_date) == -9999.: # Account for the no data value, which is -9999. Do not convert this value, instead write it to the output file as the same value. 
                new_dates.append(-9999) # Send the no data value to the new_dates list. 
            else: 
                new_dates.append(float(original_date)+convert_to_days) # Send the converted date to the new_dates list.  

    f = open(export_path+'plattsmouth_output.txt','w+') # Create the output file. 
    f.write('Days from January 1,'+str(year)) # Write the header. 
    f.write('\n')
    for val in new_dates: # Write the new_dates list to the empty file, making a new line for each value. 
        f.write(str(val))
        f.write('\n')
    f.close()
    
                      
                        
if __name__ == "__main__": # Change the values of the variables here to convert a new file. 
    start_date = [1975,9,23] #the date that the number of days is measured from in the original study, in the form [year,month,day]
    file_path = '' #File path to original table with the data, include the file name with the .txt ending
    col_num = 13 #This is the column number that contains the dates you want to convert in the data text file
    export_path = '' #Where you would like to export the new file 
    get_new_dates(start_date,file_path,col_num,export_path)

