# tree-provenance-trials
Code for processing data available in the database located at: https://knb.ecoinformatics.org/view/doi%3A10.5063%2FF1G15Z7R

This repository contains the following Python scripts: 

format_clim_data_for_databases.py
---------------------------------
This is a series of simple functions to perform the following tasks: 
- Append climate data to the table with the results. 
- Calculate the average/standard deviation/minimum/maximum of the climate variables 
for each test site. 
- Get the 1961-1990 climate averages for the test sites. 

Example of the unique codes for the test sites & provenances: 
pjp281 = P = pine, J = jack, P = provenance, 28 = data source number, 1 = provenance ID 

pjt281 = P = pine, J = jack, T = test site, 28 = data source number, 1 = test site number

(1) get_table

Input Parameters: file_path (to the txt/csv file with the trial results),header (True/False indicating whether the txt/csv file has column headers) 

Returns: a pandas dataframe of the trial results table

Notes: Gets the pandas dataframe of the table you want to join the climate data to

(2) get_primary_key

Input Parameters: pandas_dataframe (function 1),data_source_num,species_code- data_source_num & species_code refer to the unique code used for the provenances & test sites, such as '28' and 'pjt' 

Returns: the complete unique code 
	
Notes: Get the unique provenance ids from the original provenance id, data source number, and species code, for example, pjt + 28 + original id = code 

(3) match_primary_key

Input Parameters: primary_key (function 2),lookup_file (function 1) 

Returns: the elevations for each unique code stored in a dictionary with the code as the key
	
Notes: function to find the elevation data for the primary keys supplied by get_primary_key

(4) get_years
	Input Parameters: years (list of years between establishment & measurement),
	file_path (path to the climate data),test_site_code (unique test site code)
	Returns: dictionary of the row corresponding to the test site code for each
	year in the climate files 
	Notes: function to collect the records for each year for each supplied test 
	site code

(5) get_between_measurements 

Input Parameters: dictionary (function 4),test_site_code 

Returns: a dictionary that contains each value for the climate variable over the time period set by years (list of years between establishment & measurement). 

Notes: function to collect all the values for the climate variables between the planting and measurement years

(6) calc_between

Input Parameters: dictionary_of_lists (function 5),export_loc,dictionary (function 4),years

Returns: a text file of the average, standard deviation, minimum, and maximum values for each list in the dictionary (each variable over the specified years) 

(7) match_primary_key_climate
	Input Parameters: primary_key (function 2),lookup_file (function 1)
	Returns: dictionary (key: unique provenance id) of provenance data 
	Notes: function to search the lookup table for the supplied provenance 
	primary key and send the data from the lookup file to a new dictionary

(8) add_climate_data

Input Parameters: file_path (file path to the txt/csv of trial results),value_dictionary (function 7),export_path

Returns: txt file with the climate data appended to the provenance trial data 

Notes: function to output the trial results with the climate data appended

(9) export_ave_temp

Input Parameters: file_path (file path to the a folder that contains a summary file for each test site for 1961-1990),export_path,lat_lon_lookup (a lookup file where the program can find the lat/lon of each test site)

Returns: a file that contains the 1961-1990 average temperature + the lat/lon of the test sites (for importing into ArcMap)

Notes: function was just used to create a map of 1961-1990 temp for the test sites, since we just have annual data. Function to obtain a summary txt file for the long-term temperature averages for EACH test site in one file
	

date_from_jan_1.py
------------------
One python function (get_new_dates) to convert phenology data to number of days since 
January 1 of the measurement year. Under if __name__ == "__main__":, you can enter
the start date (the original day the variable was measured from), the column number of 
the data you want to convert, and the file path to the text file of the trial results. 

(1) get_new_dates

Input Parameters: start_date (the original measurement date, i.e. if it is # days since April 1, 1975, you enter this date in the format [1975,4,1]), file_path (to trial results table, comma-delimited),col_num (column with the data you are converting, count from 0),export_path (where you want the new file to be stored)
	
Returns: a text file continaining a column with the phenology data that is converted to days since January 1 of the measurement year

Notes: this function takes the number of days from a certain date and converts it into number of days since January 1 of that year. Whether or not the year is a leap year is considered.
