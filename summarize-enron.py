import pandas as pd
import os
import string
import seaborn as sns
import ast
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import time
import sys
import warnings
warnings.filterwarnings("ignore")

#datapath = './enron-event-history-all.csv'

def readcleanandprocess(datapath):
    print("Opening, preprocessing and cleaning the data...")
    if os.path.exists(datapath):
        df = pd.read_csv(datapath, header=None)                                                 # csv has no headers
        df.rename(columns={0: 'time', 1: 'message_id', 2:'sender', 3:'recipients',
                          4:'topic', 5:'mode'}, inplace=True)
        df.drop(['topic', 'mode'], axis = 1, inplace = True)                                    # drop useless columns
        df['time'] = pd.to_datetime(df['time'], unit='ms')                                      # convert to datetime
        print("We only have "+str(len(df[df.isnull().any(axis=1)])/len(df))+"% NaN values so we drop those rows")
        df = df.dropna() 
        return df
    else:
        print("Cannot locate .csv file, make sure it is in the same folder!")
    
    
def calculateTotalPeople(df):                                                               # for validation purposes
    senderlist = df['sender'].unique()

    df2 = pd.DataFrame()
    df2['time'] = df['time']                                                                # df2 will be needed further on
    df2['sender'] = df['sender']
    df2['recipients'] = df['recipients']

    def stringtolist(recepientstring):
        return recepientstring.split("|")

    df2['RecipientList'] = df2.apply(lambda row: stringtolist(row['recipients']), axis=1)   # split on '|' to get list

    recipients = df2['RecipientList'].apply(pd.Series).stack()                              # get all elements from all lists in a list
    recipients = recipients.unique()                                                        # keep each only once
    people = list(set(list(recipients)+list(senderlist)))                                   # convert to set to drop duplicates, then list 
    people.remove('')                                                                       # remove the empty string from the data
    return df2, people
     
def calculateSenders(df):                                                                   # calculate how many times each person is a sender
    opsdict = {'timesAsSender':'count'}                                    
    df['timesAsSender'] = 0
    dfsenders = df.groupby('sender').agg(opsdict) 
    df.drop(['timesAsSender'], inplace=True, axis=1)
    return dfsenders

def calculateRecipients(df2):
    def splitDataFrameList(df,target_column,separator):                                     # explode list of recipients to one per row
        row_accumulator = []
        def splitListToRows(row, separator):
            for s in row[target_column]:
                new_row = row.to_dict()
                new_row[target_column] = s
                row_accumulator.append(new_row)

        df.apply(splitListToRows, axis=1, args = (separator, ))
        new_df = pd.DataFrame(row_accumulator)
        return new_df


    df3 = splitDataFrameList(df2, 'RecipientList', ',')                                   # we are going to need this for part3          
    df3['timesAsRecipient'] = df3['RecipientList']

    opsdict = {'timesAsRecipient':'count'}          

    dfrecipients = df3.groupby('RecipientList').agg(opsdict) 
    dfrecipients.reset_index(inplace=True)
    dfrecipients.rename(columns={'RecipientList': 'recipient'}, inplace=True)
    dfrecipients = dfrecipients.iloc[1:]                                                    # drop 1st row - it is useless in our case
    return dfrecipients, df3
    
def exportToCsv(people, dfsenders, dfrecipients):
    print("Exporting CSV of 1st part...")
    df_total = pd.DataFrame()
    df_total['person'] = people

    lol = pd.merge(df_total, dfsenders, left_on='person', right_on='sender', how="outer")
    lol.timesAsSender.fillna(0, inplace=True)

    lol2 = pd.merge(df_total, dfrecipients, left_on='person', right_on='recipient', how="outer")
    lol2.timesAsRecipient.fillna(0, inplace=True)

    df_total['timesAsSender'] = lol['timesAsSender'].astype(int)
    df_total['timesAsRecipient'] = lol2['timesAsRecipient'].astype(int)
    df_total = df_total.sort_values(by=['timesAsSender'], ascending=False)
    df_total.head()

    output1path = './summarydata.csv'

    df_total.to_csv(output1path)
    return df_total

def exportLineChart(df_total, df):
    print("Exporting Linechart for 2nd part...")
    bestsenders = list(df_total.head(20)['person'])                                         # Get 20 top senders
    outputpath = './top_senders_plot'
    count=0
    plt.figure(figsize=(16,10))
    sns.set(style="darkgrid")

    if not os.path.exists(outputpath):
        os.mkdir(outputpath)

    for senderperson in bestsenders:
        count = count+1
        tempdf = df[df['sender']==senderperson][['time','sender']]
        tempdf.set_index('time',inplace=True)

        agg = tempdf.resample('M').count()                                                  # resample by month   
        agg.reset_index(inplace=True)                                                       # needed for the TS plot

        ax = sns.lineplot(x="time", y="sender", label=str(senderperson), data=agg)

        #ax.xaxis.set_major_locator(mdates.MONTHS_PER_YEAR)

        ax.set(xlabel='Months', ylabel='Sent emails of Top 20 Senders')
        plt.gcf().autofmt_xdate()                                                           # fix tick labels


    fig = ax.get_figure()                               
    fig.savefig(outputpath+'/senders.png')                                                  # save figure to .png
    
def exportLineChart2(df3, df_total):
    print("Exporting Linechart for 3rd part...")
    bestsenders = list(df_total.head(20)['person'])             # Get 20 top senders
    plt.figure(figsize=(16,10))
    sns.set(style="darkgrid")
    outputpath2 = './unique_senders_plot'
    if not os.path.exists(outputpath2):
        os.mkdir(outputpath2)
    for person in bestsenders:
        tempdf = df3[df3['RecipientList']==person]
        #print(tempdf)
        tempdf.drop(['RecipientList'], axis = 1, inplace=True)
        tempdf.set_index('time',inplace=True)
        agg = tempdf.resample('M').nunique()
        agg.reset_index(inplace=True)                       # needed for the TS plot
        
        ax = sns.lineplot(x="time", y="sender", label=str(person), data=agg)

        #ax.xaxis.set_major_locator(mdates.MONTHS_PER_YEAR)

        ax.set(xlabel='Months', ylabel='Unique senders over Time')
        plt.gcf().autofmt_xdate()                           # fix tick labels


    fig = ax.get_figure()                               
    fig.savefig(outputpath2+'/recievers.png')                  # save figure to .png
    
                                              # save figure to .png

def exportLineChart3(df, df3, df_total):
    print("Exporting ratio Linechart for 3rd part...")
    bestsenders = list(df_total.head(20)['person'])             # Get 20 top senders
    plt.figure(figsize=(16,10))
    sns.set(style="darkgrid")
    outputpath3 = './ratio_plot'
    if not os.path.exists(outputpath3):
        os.mkdir(outputpath3)
        
    for person in bestsenders:
        #print(str(person))
        #tempdf = df3[df3['RecipientList']==person]
        
        tempdf1 = df[df['sender']==person][['time','message_id']]
        tempdf2 = df3[df3['RecipientList']==person].drop(['RecipientList'], axis = 1)
        
        tempdf1.set_index('time',inplace=True)
        tempdf2.set_index('time',inplace=True)
        
        agg1 = tempdf1.resample('M').count()          # how many emails they sent per month
        agg1.reset_index(inplace=True)
        agg2 = tempdf2.resample('M').nunique()        # how many unique people sent them emails per month
        agg2.reset_index(inplace=True)
        
        final = pd.merge(agg1, agg2, on='time')
        #print("agg1 is "+str(len(agg1))+", agg2 is "+str(len(agg2))+" and final is "+str(len(final)))
        final.rename(columns={'message_id': 'emails_sent', 'sender': 'unique_senders'}, inplace=True)
        
        final['ratio'] = (final['emails_sent']+1)/(1+final['unique_senders'])
        
#         if str(person)=='pete davis':
#             print(final)
        ax = sns.lineplot(x="time", y="ratio", label=str(person), data=final)

        #ax.xaxis.set_major_locator(mdates.MONTHS_PER_YEAR)

        ax.set(xlabel='Months', ylabel='Ratio of Email sent/Unique senders over Time')
        plt.gcf().autofmt_xdate()                           # fix tick labels


    fig = ax.get_figure()                               
    fig.savefig(outputpath3+'/ratio_plot.png')                  # save figure to .png
      
    


def main(file):
    start = time.time()

    datapath = './'+str(file)
    #datapath = './enron-event-history-all.csv'
    
    
    df = readcleanandprocess(datapath)
    df2, people = calculateTotalPeople(df)                                                      # df2 will be needed further - it has the recipients UNPACKED
    #print("There are "+str(len(people))+" unique 'people' in the dataset")
    dfsenders = calculateSenders(df)
    dfrecipients, df3 = calculateRecipients(df2)
    df_total = exportToCsv(people, dfsenders, dfrecipients)
    exportLineChart(df_total, df)
    exportLineChart2(df3, df_total)
    exportLineChart3(df, df3, df_total) 
       
    end = time.time()
    print("Total execution time: "+str(end - start)+" seconds")
    print("Plots are located in ./top_senders_plot ./unique_senders_plot and ./ratio_plot folders respectively")

if __name__ == '__main__':
    file = str(sys.argv[1])
    main(file)