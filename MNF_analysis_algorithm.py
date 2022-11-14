# MNF analysis algorithm

# Algorithm inputs.
# reads:                dictionary of reads per MSN                 -- i.e. dictionary where key:value pairs are - keys : uniqie MSNs | values : Pandas DataFrame of meter reads for specific MSN
# MNF_start:            datetime.time start period of MNF analysis  -- i.e. for 02:00:00 start time MNF_start = datetime.strptime('02:00:00', '%H:%M:%S').time()
# MNF_finish:           datetime.time end period of MNF analysis    -- i.e. for 04:00:00 finish time MNF_finish = datetime.strptime('02:00:00', '%H:%M:%S').time()
# time_col:             column heading of time column               -- Column heading for datetime.time values within DataFrame of meter reads for each MSN i.e 'reading_times', where reads[MSN]['reading_times'] returns the series of reading_times when each SM meter reading was transmitted
# consumption_col:      column heading of consumption column        -- Similar to above but for the consumption column (floats) rather that time (datetime.time) i.e. 'Consumption'
# date_col:             column heading of date column               -- Like above, but for reading Dates rather than consumption.

# Algorithm outputs: 
# MNF_analysis:         Dictionary of key:value pairs where - keys : unique MSNs | values : Pandas DataFrame of nightly MNF values (minimum flow and average flow during the nightline period)
# MNF_analysis_average: Dictionary of key:value pairs where - keys : unique MSNs | values : tuples of average MNF values per MSN where each tuple consists of averaged (min_MNF, avg_MNF) i.e. [0]: averaged nightly minimum flow values | [1]: averaged average nightly flow values


def MNF(reads, MNF_start, MNF_finish, time_col, date_col, consumption_col):
    print('\nConducting MNF analysis')
    
    i=0
    MNF_analysis = {} 
    for MSN in reads.keys(): # For each MSN analysed
        values = []

        for date in reads[MSN][date_col].unique(): 	# For every night available


            filt = (reads[MSN][time_col] >= MNF_start) & (reads[MSN][time_col] <= MNF_finish) & (reads[MSN][date_col] == date) # Filter to select only relevant data

            min_MNF = reads[MSN][filt][consumption_col].min()
            avg_MNF = reads[MSN][filt][consumption_col].mean()

            #i.e. [0]: date | [1]: min_MNF | [2]: avg_MNF
            vals = (date, min_MNF, avg_MNF)
            values.append(vals)

        i += 1
        MNF_analysis[MSN] = pd.DataFrame(values, columns = [date_col, 'min_MNF', 'avg_MNF'])
        print(str(i/len(MSNs)*100) + '% Complete.')
    
    MNF_analysis_average = {} 
    for MSN in MFN_analysis:
        MNF_analysis_average[MSN] = (MNF_analysis[MSN].min_MNF.mean(), MNF_analysis[MSN].avg_MNF.mean())
        #Values are tuples of (min_MNF, avg_MNF)
    
    return MNF_analysis, MNF_analysis_average
