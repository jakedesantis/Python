#!/usr/bin/python


#  THIS SCRIPT DOES:  
#  Break down an "aggregate" file of processed trades into 'manageable in Excel' pieces.  

#  THIS SCRIPT DOES NOT:  
#  Process information in quotes into trades.

#  ONLY USE THIS SCRIPT WHEN:
#  You need to break an "aggregate" file into smaller pieces


# INSTRUCTIONS:
# This script must be called with the name of the 'aggreate' file to be broken into pieces as only argument.
# The number of lines in the piece files (e.g., 1,000,000) should be entered below as "max_lines".
# The file name must be absoulte, in double quotes, and with double \\.
# Source files can have any set of columns, each line is rewritten verbatim
# Sourcefiles should be named [CM]_Aggregate.csv
# New "piece" files will be named [CM]_Aggregate_Piece[x].csv
# These files will be created in the same directory as the "aggregate" file.
# This script overwrites existing files.
# It's recommended to move both "aggregate" file and "piece" files out of the [CM]DataProcessed\ directory.


#Set the size of the "piece" files
max_lines = 500000





import datetime
import os 
import sys
import math

#  Pull in Aggregate file name
aggregate_file_name = sys.argv[1]     

starttime = (datetime.datetime.now())
print("")
print("")
print ("Starting job at " + str(starttime))
print("")
print("")
print ("Creating piece files with " + str(max_lines) + " lines per file.")
print("")
print("")

line = 1
af = open(aggregate_file_name)


#  Count lines in Aggregate file
line_count = 0
af = open(aggregate_file_name)
for line   in af:
	line_count = line_count + 1 
print("")
print("line_count: " + str(line_count))
print("")


#  Reposition pointer to beginning of file
af.seek(0,0)


#  Create one file for every "max_lines" lines
print("")
print("Number of piece files will be " + str(math.floor(line_count / max_lines) + 2))
print("")
for pf_number in range(1, math.floor(line_count / max_lines) + 2): 
	
	#  Create name for new piece file
	pf_name = "Aggregate_Piece" + str(pf_number)
	pf_name = aggregate_file_name.replace("Aggregate", pf_name)  
	print("")
	print("pf_name: " + pf_name)
	print("")

	#  Write to new piece file
	pf = open(pf_name,  'w')
	for i in range(1, max_lines):
		line = af.readline()	
		pf.write(line)

	pf.close()

af.close()


endtime = (datetime.datetime.now())
print("")
print("")
print ("Finished job at " + str(endtime))
print("")
print("")

