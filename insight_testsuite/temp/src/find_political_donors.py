#################################################################
# Author: Rui La
# University of California, San Diego
# Email: larui529@gmail.com
# this is extractor for insightDataScience data engineer program
# things to pay attention to in extraction:
#1 OTHER_ID, ignore record when OTHER_ID is not empty check
#2 TRANSACTION_DT, input the record to zip not dt if T_DT is empty check
#3 ZIP_CODE, only take first five digits check
#4 Take data with invalid zipcode in date.txt, but not zip.txt check
#5 ignore data with empty "CMTE_ID" or "TRANSACTION_AMT" check
#################################################################

import pandas as pd
import numpy as np
import datetime
import time
import sys

start_time = time.time()
#open the file and read all the lines
input_file = open(sys.argv[1])
#output_file1 = open(sys.argv[2], 'a')
#output_file2 = open(sys.argv[3], 'a')
lines = input_file.readlines()
#create a dataframe with the col name of features of interest
#name of features of interest
column_names = ['CMTE_ID', 'ZIP_CODE','TRANSACTION_DT',
				'TRANSACTION_AMT','OTHER_ID']
df_init = pd.DataFrame(columns = (column_names))

#split each line and extract feafetures of interests 
for i, line in enumerate(lines):
	info = line.split('|')
	df_init.loc[i] = [info[0], info[10], info[13],info[14],info[15]]
#the following coments are used for debug and test during coding.
#print (df_init)
#test for invalid data
#df_init.loc[7]=['C00177436',32454, '013132017', '142', '']

#convert ZIP_CODE to only five digits. 
df_init['ZIP_CODE'] = df_init['ZIP_CODE'].astype(str).str[:5]

#drop the not-empty 'CMTE_ID', and empty 'CMTE_ID''TRANSACTION_AMT' rows
df_init.replace('', np.nan,inplace = True)
df_init = df_init[df_init['OTHER_ID'].isnull()]
df_init = df_init[df_init['CMTE_ID'].notnull()]
df_init = df_init[df_init['TRANSACTION_AMT'].notnull()]
#convert 'TRANSACTION_AMT' to int 
df_init['TRANSACTION_AMT'] = pd.to_numeric(df_init['TRANSACTION_AMT'], 
	                         errors='coerce').fillna(0).astype(np.int64)
#Purpose: function to add record_time to show appearance with different 
#'CMTE_ID' and "ZIP_CODE"
#Output: dataframe to be written into "medianvals_by_zip.txt" file
def process_for_zip (dataframe):
	
	dataframe = df_init
	dataframe = dataframe.drop(['TRANSACTION_DT', 'OTHER_ID'],1)
	grouped_df = dataframe.groupby(['CMTE_ID', 'ZIP_CODE'])
	record = grouped_df.cumcount()+1 # find cummulative count of elements
	total_above = grouped_df.cumsum() #find cummulative sum of the elements
	cum_median = grouped_df.apply(lambda x: 
		pd.Series([np.median(x[:i]) 
			for i in range(1,len(x)+1)],x.index)).reset_index()
	cum_median.iloc[:,-1] = cum_median.iloc[:,-1].round().astype(np.int64)
	cum_median = cum_median.set_index('level_2')
	dataframe = dataframe.assign(CUM_MEDIAN = cum_median.iloc[:,-1])
	dataframe = dataframe.assign(RECORD_TIME = record) 
	dataframe = dataframe.assign(TOTAL_ABOVE = total_above)
	dataframe = dataframe.drop(['TRANSACTION_AMT'],1)
	return dataframe

#Purpose: calculate total donation amount, median and frequency of donation 
#         under same 'CMTE_CODE' and 'ZIP_CODE'
#Output: dataframe to be written into "medianvals_by_date.txt"
def process_for_date(dataframe):
	#first get rid of data with invalid zip_code, and empty transaction_dt
	dataframe = dataframe[dataframe['ZIP_CODE'].astype(str).str.len() == 5]
	dataframe = dataframe[dataframe['TRANSACTION_DT'].astype(str).str.len()==8]
	dataframe = dataframe[dataframe['TRANSACTION_DT'].notnull()]
	dataframe = dataframe.drop(['ZIP_CODE','OTHER_ID'], 1)
	#use groupby to find data with same CMTE_ID and TRACTION_DT 
	grouped_data = dataframe.groupby(['CMTE_ID', 'TRANSACTION_DT'])
	TOTAL_AMT = grouped_data.sum() # sum of grouped data
	MEDIAN_AMT = grouped_data.median().round().astype(np.int64) #median
	COUNT = grouped_data.count() #total number of apperance
	out_df = MEDIAN_AMT.assign(COUNT = COUNT, 
		TOTAL_AMT = TOTAL_AMT).reset_index()
	return out_df
#create two dataframe ready to be written out
zip_df = process_for_zip(df_init)
date_df = process_for_date(df_init)

#print (zip_df)
#print (date_df)
#export zip_df to "medianvals_by_zip.txt"
np.savetxt(sys.argv[2],zip_df, fmt = '%s', delimiter = '|')
#export zip_df to "medianvals_by_date.txt"
np.savetxt(sys.argv[3],date_df, fmt = '%s',delimiter = '|')

#find out the total excution time. 
print("The total running time is {}".format(time.time()-start_time))


