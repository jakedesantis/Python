#!/usr/bin/python

#  THIS SCRIPT DOES:  
#   Process monthly raw data files into monthly "processed" files (trades only, matched
#   with previous and follosing bids and asks), and aggregates these files into a single
#   "aggregate" file.  

#  THIS SCRIPT DOES NOT:  
#   Break down the aggregate file into 'manageable in Excel' "piece" files.  Use the
#   newer version (CSV_processor.py) for that.   This script is meant as a back-up in 
#   case the 'piece maker' portion of that script fails and can't be fixed.



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

import datetime
import os
import sys

#pull in directory name
dir_name = sys.argv[1]         

print("")
#print (dir_name[20:22])

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
                                  
                # if line contains a Trade                     
                if (line_string[5]): 
                    
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
            print("  " + str(file_record) + " lines processed into " + str(file_trades) + " lines for " + str(int(file_record/file_trades)) + " records per trade.")
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
print("An average of " + str(int(total_records/total_trades)) + " records for every trade over " + str(files) + " files.")

#st = datetime.datetime.strptime(starttime, "%Y-%m-%d %H:%M:%S.%f")
#et = datetime.datetime.strptime(endtime, "%Y-%m-%d %H:%M:%S.%f")
print("")
print("Elapsed time of " + str(endtime - starttime))
print("")
diff = endtime - starttime  # in seconds
print(str(int(total_records/diff.seconds)) + " records per second processed.")
print("")


#Create aggregate file
aggregate_file = output_dir + dir_name[20:22] + "_Aggregate.csv"
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
print("Bye.")
