#!/usr/bin/python

#  TODO:  
#  Add date and time to processed and aggregate file names? 

# INSTRUCTIONS:
# This script must be called with the directory of CSVs to be processed as only argument.
# The name of the directory must be [CM]DataRaw\
# The directory name must be absoulte, in double quotes, and with double \\.
# Source files must have fields RIC, [Domain], Timestamp, Type, Price, Volume, Bid Price, [Bid Size], Ask Price, [Ask Size], Qualifiers
# Sourcefiles should be named CMYYYYMM.csv
# A new directory parallel to the [CM]DataRaw\ directory named CMDataProcessed\ will be created
# This will hold the Processed files and an Aggregate of these Processed files.
# Processes 100-150k records per second in cloud environment.
# This script overwrites existing DataProcessed\ directory, 'Processed' files, and 'Aggregate' files.

# COMMENTS ON THE SPLITTER:
# New "piece" files will be named [CM]_Aggregate_Piece[x].csv
# These files will be created in the same directory as the "aggregate" file.
# This script overwrites existing files.
# It's recommended to move both "aggregate" file and "piece" files out of the [CM]DataProcessed\ directory.


#Set the size of the "piece" files for the splitter here:
#max_lines = 500000


import datetime
import os
import sys
import math
print ("")

if (len(sys.argv) == 1):
	dir_name = input("Enter directory to be processed, in quotes and with double \\\:  ")
elif (len(sys.argv) == 2):
	dir_name = sys.argv[1]



#pull in directory name
    

print("")
max_lines = int(input("How many lines per aggregated file?: "))

#initialize job-wide variables
total_records = 0
total_trades = 0
files = 0

starttime = (datetime.datetime.now())
print("")
print("")
print("")
print ("Starting job at " + str(starttime))
print("")
print("")

#Create dirctory for processeed and aggregate files.

output_dir = dir_name.replace("DataRaw", "DataProcessed")

if os.path.exists(output_dir) == False:
	os.mkdir(output_dir)


#Loop through all RawData files.
for fn in os.listdir(dir_name):

    #Only process the intended files
    if (fn.find("gz") == -1 and fn.find("processed") == -1 and fn.find("ggregate") == -1):
        files = files + 1
        print (fn + " being processed at " + str(datetime.datetime.now()))
        source_file = dir_name + fn
        output_file = output_dir + fn.replace(".csv", "_processed.csv")  

        #Initialize file-scoped variables
        current_bid = 0
        current_ask = 0
        trades = [['','','','','','','','','','','','']]
        trade_count = 0 # not needed??
        file_record = 1
        file_trades = 0
        max_trades_in_memory = 0
        trades_to_delete = []
        
        #Create file and write header
        of = open(output_file, 'w') 
        of.write(",".join(['RIC','Trade Timestamp','Price','Volume','Qualifier','Previous Bid','PB Timestamp','Previous Ask','PA Timestamp','Following Bid','FB Timestamp','Following Ask','FA Timestamp\n']))
        
        #Loop through Quotes and Trades by line
        with open(source_file) as sf:
            for line in sf:
                if file_record == 2: 
                    trades = [['','','','','','','','','','','','','']]
                line_string = line.split(",")
                
                # if line contains a Bid Price
                if(line_string[6]):
                    
                    #Update current Bid Price
                    current_bid = line_string[6]
                    current_bid_ts = line_string[2]
                    
                    #Check each trade in memory for Following Bid.  If not present, add one.
                    for t in range(0,len(trades)):
                        if(trades[t][0] !='' and trades[t][9] == ''):
                            trades[t][9] = current_bid
                            trades[t][10] = current_bid_ts
                                  
                # if line contains a Ask Price                  
                if (line_string[8]):
                    
                    #Update current Ask Price
                    current_ask = line_string[8]
                    current_ask_ts = line_string[2]
                    
                    #Check each trade in memory for Following Ask.  If not present, add one.
                    for t in range(0,len(trades)):
                        if(trades[t][0] != '' and trades[t][11] == ''):
                            trades[t][11] = current_ask
                            trades[t][12] = current_ask_ts
                                  
                # if line contains a Trade with both Price and Volume                     
                if (line_string[5] and line_string[4]): 
                    
                    #Add new row in memory to recieve new Trade entry
                    if len(trades)>0: 
                        trades.append(['','','','','','','','','','','','',''])
                    
                    #Add Trade details other than Following information
                    trades[-1][0] = line_string[0] #RIC
                    trades[-1][1] = line_string[2] #Trade TS
                    trades[-1][2] = line_string[4] #Trade Price
                    trades[-1][3] = line_string[5] #Volume
                    trades[-1][4] = line_string[10].strip() #Qualifier (remove newline tag!)
                    trades[-1][5] = current_bid #Previous Bid
                    trades[-1][6] = current_bid_ts #PB TS
                    trades[-1][7] = current_ask #Previous Ask
                    trades[-1][8] = current_ask_ts #PA TS
                    max_trades_in_memory = max(max_trades_in_memory, len(trades))
                  
                #  check Trades in memory for complete Following market          
                for tt in range(0, len(trades)):
                    if(trades[tt][9] and trades[tt][11]):    # and trades[tt][0] != '#RIC'):  <--add back if any errors
                        
                        #if Trade is complete, write to file and mark for deletion in trades_to_delete variable
                        trades [tt][12] = trades[tt][12] + '\n'
                        of.write(",".join(trades[tt]))
                        file_trades = file_trades + 1
                        trades_to_delete.append(tt)
                        
                #  delete trades from memory if marked        
                for ttd in reversed(trades_to_delete):
                    
                    #delete trade
                    del(trades[ttd])
                    
                    #delete mark for deletion from list
                    del(trades_to_delete[ttd-1])
                    if trades == []:
                        trades = [['','','','','','','','','','','','','']]
                
                #increment row counter - not used for anything but keeping track of progress
                file_record = file_record + 1
        
       
	    # Report file-level statistics
            sf.close()
            #print("  " + str(file_record) + " lines processed into " + str(file_trades) + " lines for " + str(int(file_record/file_trades)) + " records per trade.")
            print("    Max trades in memory = " + str(max_trades_in_memory))
            total_records = total_records + file_record
            total_trades = total_trades + file_trades
            print("")
        newlines = 0		
        of.close()

# Report job-level statistics
endtime = (datetime.datetime.now())  
print ("Ending job at " + str(endtime))
print("")
print(str(total_records) + " total records resulting in " + str(total_trades) + " total trades.")
print("")
#print("An average of " + str(int(total_records/total_trades)) + " records for every trade over " + str(files) + " files.")

#st = datetime.datetime.strptime(starttime, "%Y-%m-%d %H:%M:%S.%f")
#et = datetime.datetime.strptime(endtime, "%Y-%m-%d %H:%M:%S.%f")
print("")
print("Elapsed time of " + str(endtime - starttime))
print("")
diff = endtime - starttime  # in seconds
print(str(int(total_records/diff.seconds)) + " records per second processed.")
print("")


#Create aggregate file
if dir_name[20] != "1":
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
print("Finished writing " + str(aggregate_file) + " with " + str(total_trades) + " trades.")
print("")




#  Break the aggregate file into piece files

starttime = (datetime.datetime.now())
print("")
print("")
print ("Starting to break aggregate file into pieces at " + str(starttime))
print("")
print("")
print ("Creating piece files with " + str(max_lines) + " lines per file.")
print("")
print("")


line = 1
af = open(aggregate_file)

#  Count lines in Aggregate file
line_count = 0
af = open(aggregate_file)
for line   in af:
	line_count = line_count + 1 
print("")
print("line_count: " + str(line_count))
print("")
print("")
print("Number of piece files will be " + str(math.floor(line_count / max_lines) + 1))
print("")

#  Reposition pointer to beginning of file
af.seek(0,0)


for pf_number in range(1, math.floor(line_count / max_lines) + 2): 
	
	#  Create name for new piece file
	pf_name = "Aggregate_Piece" + str(pf_number)
	pf_name = aggregate_file.replace("Aggregate", pf_name)  
	print("")
	print("Writing " + pf_name + " now.")
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
print ("Finished breaking aggregate file into piece files at " + str(endtime))
print("")
print("")



print("Bye.")
