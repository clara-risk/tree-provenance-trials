
import numpy as np
import matplotlib.pyplot as plt
import os,sys
import math, statistics
import win32com.client
import pypyodbc
import csv

#Export all files from access to .csv


def export_frm_access(access_file,file_out,tbl):


     #Connect to the access database
     pypyodbc.lowercase = False
     conn = pypyodbc.connect(
         r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};" +
         r"Dbq="+str(access_file))
     cur = conn.cursor()
     #Get the header 
     header = cur.execute("SELECT * FROM "+'['+tbl+"] WHERE 1=0")
     columnList = [tuple[0] for tuple in header.description]

     #Connect to the cursor and get everything from the table 

     cur.execute("SELECT * FROM "+'['+tbl+'];');

     #Write to the .csv 
     with open(file_out+tbl+'.csv', 'w', newline='') as f:
         writer = csv.writer(f)
         writer.writerow(columnList) 
         for row in cur.fetchall() :
             writer.writerow(row)

     cur.close()
     conn.close()

def rename_no_spaces(file_path):
     '''Rename the csv files so that they do not have spaces or commas, which makes it easier
     for user to import files to programs such as ArcMap.'''

     [os.rename(os.path.join(file_path, f), os.path.join(file_path, f).replace(' ', '_'))\
      for f in os.listdir(file_path)]
     [os.rename(os.path.join(file_path, f), os.path.join(file_path, f).replace(',', ''))\
      for f in os.listdir(file_path)]
     
if __name__ == "__main__":
     access_file = '' #Path to .accdb
     file_out = '' #Path to output location
     #tbl_list = ['Cloquet Forest Research Center, MN (1,2)','Deblois, ME (3)','Dyer, ME (3)',\
                 #'Lakehead University Greenhouse, ON (28)','Plattsmouth, NE (4)',\
                 #'Raith, ON (28)','Thunder Bay, ON (28)']
##     tbl_list = ['Angus, ON (29)','Dryden, ON (29)','Englehart, ON (29)',\
##                     'Grand Rapids, MN (8)','Kakabeka, ON (29)','Longlac, ON (29)',\
##                     'Passadumkeag, ME (7)','Petawawa Research Forest, ON (9)','Petawawa, ON (29)']
##     tbl_list = ['Alfred, ME Site 1 (12)','Alfred, ME Site 2 (12)','Alfred, ME Site 3 (12)'\
##                 ,'Carroll County, MD (15)','Essex Junction, VT (12)','Ganaraska Forest, ON (11)'\
##                 ,'Horseshoe Run Recreation Area, WV (12)','Kennett Square, PA (12)',\
##                 'London, KY (12)','Manistique, MI (13,14)','Nelsonville, OH (12)',
##                 'Orono, ME (12)','Paul Smiths, NY (12)','Pike Bay, MN (13,14)',\
##                 'Pine River, MI (13,14)','Rhinelander, WI (Hugo Sauer Nursery) (13,14)',\
##                 'Rison, MD (12)','Sandy Point State Park, MD (15)','Savage River State Forest, MD (12)',\
##                 'St Williams, ON (Nursery) (11)','Standing Stone, PA (12)','Turkey Point, ON (11)'\
##                 ,'Wabeno, WI (13,14)','Warren, PA (12)','Watersmeet, MI (Toumey Nursery) (13,14)',
##                 'Williamstown, MA (12)']
##     tbl_list = ['Alfred, ME (24)','Black Brook, NB (24)','Lac St Ignace, QC (24)',\
##                 'Millerton Junction Road, NL (5)','Piscataquis County, ME (6)','Roddickton, NL (24)',
##                 'Roddickton, NL (5)','Serpentine Lake, NL (5)']
##     tbl_list = ['Apple Creek, OH (16,17,18)','Augusta, MI (16,17,18)','Georgia Tower, IN (16)',\
##                 'Monticello, IL (16)','Plattsmouth, NE (16,17,18,27)','Tuttle Creek, KS (16)',\
##                 'West Lafayette, IN (16,17,18)']
##     tbl_list = ['Acadia Forest Experiment Station, NB (22)','Avondale, NL (23)','Bottom Brook, NL (23)',\
##                 'Cold Brook, NL (23)','Mount Zion, WV (26)','North Pond, NL (23)','South Range, MI 1967 Plantation (21)',\
##                 'South Range, MI 1968 Plantation (21)','Thunder Bay, ON (19)','West Virginia University Forest, Morgantown, WV (20)',\
##                 'Windsor Lake, NL (23)']
##     tbl_list = ['Lake Tomahawk, WI (25)']
     tbl_list = ['Latitude & Longitude Accuracy','Guide to Data Sources','Elevation of Test Sites'\
                 ,'Data Availability']
     for tbl in tbl_list: 
          export_frm_access(access_file,file_out,tbl)
