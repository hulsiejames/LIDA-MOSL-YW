# Continuous flow detection algorithm. 

# This algorithm iterates a sliding window of width [period] over a specific column [col] within 
# a dataframe [df] searching for continuous flows. A continuous flow is found when the minimum value
# within the window is greater than or equal to a pre-determined threshold [threshold]

# Algorithm inputs. 
# df:				DataFrame of MSN specific meter readings, crucially this must contain a consumption column
# imputed_data:		Remembers indicies where missing data has been imputed due to meter downtime in the df above. If a continuous flow is detected that contains imputed data we  must acknowledge this (may not truthfully be a cont. flow as meter was down) 
# col: 				User specified column name of DataFrame column that holds consumption values i.e. 'Consumption'
# threshold:		User specified minimum flow threshold to classify a continuous flow i.e. 1.0
# preriod:      	The period (in intervals of the datagranularity) in which non-zero consumption must be sustained for classification as a continuous flow i.e. for 15min data granularity a 14 day period = 14days x 24hrs x 4(4 15mins in 1 hr) = 14x24x4 = 1344
# granularity:  	The granularity of data within df to be entered as a frequency i.e. 15 mins = '15T', hourly = '1H', every three hours = '3H'. for more info see https://pandas.pydata.org/docs/user_guide/timeseries.html#timeseries-offset-aliases


# Note that all three outputs are in the form of lists where each instace detected is a string stating wheter imputed data is used, the start and end time of detected continuous flow.  
# Algorithm outputs. 
# issues:			All instaces of continuous flows that are detected
# actual issues:	Only instances of continuous flows that contain real data
# imputed issues: 	Only instances of continuous flows that contain imputed data


def enhanced_resampled_cont_flow(df, imputed_data, col, threshold, period, granularity):
    issues = []
    actual_issues = []
    imputed_issues = []

    recent_window = set()
    
    for i in range(len(df[col]) - period):
        
        #Checks if i is in a recently discovered window of cont. flow - if so skip to next i.
        if i in recent_window:
            continue

        #Checks if minimum flow value is above user-defined threshold.
        min_val = df[col].iloc[i:(i + period)].min()

        if min_val >= threshold:
            #A continuous flow has been detected

            #sets the current window range - check this against imputed values
            period_range = pd.date_range(start=df.iloc[i].name, freq=granularity, periods=period)
            

            #Checks if any data within the flow window is from imputed data.
            for times in period_range:
                
                #If a cont. flow containing imputed data has been found, this stops the algo re-detecting the same flow by continually skipping to next time interval.
                if i in recent_window:
                    continue

                #If the window contains imputed data - save this cont flow as an imputed one then add window indicies to set to stop re-detection of this flow. 
                if times in imputed_data:
            		#The current continuous flow contains imputed data
                    entry = ('Imputed Cont. Flow in Range ' + str(df.iloc[i].name) + ' to ' + str(df.iloc[i+period].name))
                    
                    imputed_issues.append(entry)
                    issues.append(entry)

                    recent_window = set(range(i, (i+period+1)))

                else:
            		#This timestamp in the window is not imputed, move to next timestamp and check again
                    continue 

            #Non of the timestamps were from imputed data so flow must be from real data (passed the min_val>threshold).
            
            #Check if the flow has already been recorded 
            if i in recent_window:
                continue
            
            else: #Flow hasn't been recorded. 
                entry = ('Cont. Flow in Range ' + str(df.iloc[i].name) + ' to ' + str(df.iloc[i+period].name))
                
                issues.append(entry)
                actual_issues.append(entry)

            recent_window = set(range(i,(i+period+1)))
            
    return issues, actual_issues, imputed_issues