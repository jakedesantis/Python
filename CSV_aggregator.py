#!/usr/bin/python

#  THIS SCRIPT DOES:
#  Creates an "aggretate" file from the monthly processed trades.

#  THIS SCRIPT DOES NOT:  
#  Break down an "aggregate" file of processed trades into 'manageable in Excel' pieces.  

#  THIS SCRIPT DOES NOT:  
#  Process information in quotes into trades.

#  ONLY USE THIS SCRIPT WHEN:
#  You need to create an "aggregate" file 


# INSTRUCTIONS:
# This script must be called with the name of the driectory of RAW files for which the PROCESSED files are to be broken into pieces as only argument.
# The directory name must be absoulte, in double quotes, and with double \\.
# This script overwrites existing files.


import datetime
import os 
import sys
import math


if (len(sys.argv) == 1):
	dir_name = input("Enter directory to be processed, in quotes and with double \\\:  ")
elif (len(sys.argv) == 2):
	dir_name = sys.argv[1]

output_dir = dir_name.replace("DataRaw", "DataProcessed")

print("")
dir_chars = int(input("How many characters are there in this commodity's directory name (2 or 3)?: "))


if dir_chars == 2:
	aggregate_file = output_dir + dir_name[20:22] + "_Aggregate.csv"
else:
	aggregate_file = output_dir + dir_name[20:23] + "_Aggregate.csv"

print("Now creating aggregated file: " + aggregate_file)
print("")
af = open(aggregate_file, "w")
af.write(",".join(['RIC','Trade Timestamp','Price','Volume','Qualifier','Previous Bid','PB Timestamp','Previous Ask','PA Timestamp','Following Bid','FB Timestamp','Following Ask','FA Timestamp\n']))

#Loop through directory writing from 'proccesed' files and skipping first line.
for fn in os.listdir(output_dir):

    	#Only process the intended files
	if (fn.find("processed") != -1):
		print("Starting to process " + fn)
		row = 1
		processed_file = output_dir + fn
		with open(processed_file) as pf:
			for line in pf:
				if row > 1:
					af.write(line)	
				row = row + 1
		pf.close()

af.close()
print("")
print("Finished writing " + str(aggregate_file) + ".")
print("")



