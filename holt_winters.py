from __future__ import print_function
import sqlite3
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np

db = r'I:\FINANCE\ACCOUNTING\Maranon Reports\2015\New Keane 13-wk Cash\OperatingMetrics\submittals.sqlite'
conn = sqlite3.connect(db)
cursor = conn.cursor()
sql = ''' SELECT * FROM pel_submittals WHERE submittal_dte > '20160101' '''
df = pd.read_sql(sql, conn, parse_dates='submittal_dte')

df.set_index('submittal_dte', inplace=True)
df_daily = df.resample('D', how='sum').dropna()             #dropna because the resample includes all days (i.e. weekends included)
df_weekly = df.resample('W', how='sum')
df_monthly = df.resample('M', how='sum')


def normalize_months_for_daily_subs(df = df_daily):
    ''' will need to change date ranges and revisit monthly stuff to smooth
    into 21 seasons everytime that we want to re-run holt-winters and optimize
    exponents for final algo'''
    
    norm = pd.date_range('20160101', '20160430', freq='B')
    norm = pd.DataFrame(index=norm)
    df2 = norm.merge(df, how='left', left_index=True, right_index=True)
    df2.fillna(limit=1, method='ffill', inplace=True)        #pad nans where we didn't submit on a 'bus date'; bus dates populated from 'norm'
    df2.fillna(0, inplace=True)                              #pad Jan1 date to zero
    df2.drop(datetime(2016, 03, 01), inplace=True)           #drop first of month submittal to line up seasons
    df2.drop(datetime(2016, 03, 15), inplace=True)           #drop middle of month submittal to line up seasons
    df2['month'] = df2.index.month
    return df2

    
def initial_trend(series, slen):
    sum = 0.0
    for i in range(slen):
        sum += float(series[i+slen] - series[i]) / slen
    return sum / slen

def initial_seasonal_components(series, slen):
    seasonals = {}
    season_averages = []
    n_seasons = len(series)/slen
    # compute season averages
    for j in range(n_seasons):
        season_averages.append(sum(series[slen*j:slen*j+slen])/float(slen))
    # compute initial values
    for i in range(slen):
        sum_of_vals_over_avg = 0.0
        for j in range(n_seasons):
            sum_of_vals_over_avg += series[slen*j+i]-season_averages[j]
        seasonals[i] = sum_of_vals_over_avg/n_seasons
    return seasonals

def triple_exponential_smoothing(series, slen, alpha, beta, gamma, n_preds):
    result = []
    seasonals = initial_seasonal_components(series, slen)
    for i in range(len(series)+n_preds):
        if i == 0: # initial values
            smooth = series[0]
            trend = initial_trend(series, slen)
            result.append(series[0])
            continue
        if i >= len(series): # we are forecasting
            m = len(series) - i + 1
            result.append((smooth + m*trend) + seasonals[i%slen])
        else:
            val = series[i]
            last_smooth, smooth = smooth, alpha*(val-seasonals[i%slen]) + (1-alpha)*(smooth+trend)
            trend = beta * (smooth-last_smooth) + (1-beta)*trend
            seasonals[i%slen] = gamma*(val-smooth) + (1-gamma)*seasonals[i%slen]
            result.append(smooth+trend+seasonals[i%slen])
    return result

def daily_optimize(df = normalize_months_for_daily_subs()):
    seasons = 21
    rng_len = 50
    divisor = 50
    res = []
    for i in range(rng_len):
        for j in range(rng_len):
            for k in range(rng_len):
                alpha, beta, gamma = .02+float(i)/divisor, .02+float(j)/divisor, .02+float(k)/divisor
                trp = triple_exponential_smoothing(df['rev_amt'].values[:-21], seasons, alpha, beta, gamma, 21)
                res.append((trp, (alpha, beta, gamma)))
    
    sse_list = []
    for i in range(len(res)):
        l1 = df['rev_amt'].values[-21:]
        l2 = res[i][0][-21:]
        sse = sum((np.array(l1) - np.array(l2)) ** 2)
        sse_list.append((sse, res[i][1]))
    
    df = pd.DataFrame(sse_list)
    df.sort(0, inplace=True)
    return df

def weekly_optimize(df = df_weekly):
    rng_len = 50
    res = []
    for i in range(rng_len):
        for j in range(rng_len):
            for k in range(rng_len):
                alpha, beta, gamma = .02+float(i)/rng_len, .02+float(j)/rng_len, .02+float(k)/rng_len
                trp = triple_exponential_smoothing(df.values[:-8], 4, alpha, beta, gamma, 8)
                res.append((trp, (alpha, beta, gamma)))
    
    sse_list = []
    for i in range(len(res)):
        l1 = df.values[-8:]
        l2 = res[i][0][-8:]
        sse = sum((np.array(l1) - np.array(l2)) ** 2)
        sse_list.append((sse, res[i][1]))
    
    return sse_list                 

#dly = daily_optimize()
#wkly = weekly_optimize()

norm_daily_subs = normalize_months_for_daily_subs()
#trp = triple_exponential_smoothing(norm_daily_subs['rev_amt'].values, 21, .04, .14, .12, 60)     #can play with series and n_preds to compare against actuals
trp = triple_exponential_smoothing(norm_daily_subs['rev_amt'].values, 21, .035, .168, .106, 231)  #add'l precision by running monte carlo around ranges from original optimization
#plt.plot(trp, '-o')
#plt.plot(norm_daily_subs['rev_amt'].values, '-x')
#plt.axhline(y=0, linewidth=1, linestyle='--', color='k')
#plt.show()


triple = zip(*(iter(trp[-231:]),) * 21)

def to_sales_proj(triple, month_no):
    keane_holidays = ['2016-01-01', '2016-01-18', '2016-02-15', '2016-05-30', '2016-07-04', '2016-07-05', 
                  '2016-09-05', '2016-11-24', '2016-11-25', '2016-12-23', '2016-12-26', '2016-12-30', '2017-01-02', 
                  '2017-01-23', '2017-02-13', '2017-05-29', '2017-07-03', '2017-07-04', '2017-09-04', '2017-11-23', 
                  '2017-11-24', '2017-12-22', '2017-12-25', '2017-12-29']

    df = pd.DataFrame(index=pd.bdate_range(start = datetime.today() - pd.DateOffset(day=1), 
                                            end = datetime.today() + pd.DateOffset(months=11) + pd.DateOffset(day=31),
                                            freq='B'))
    [df.drop(pd.Timestamp(i), inplace=True) for i in keane_holidays if i in df.index]
    
    df['rev_amt'] = 0
    
    ##TODO!! NEED TO FIGURE OUT HOW TO CYCLE THROUGH RANGE WHICH ALLOWS TO CROSS OVER YEARS -- WILL NEED TO REPLACE
    ##DF.INDEX.MONTH CRITERIA IN THE IF STATEMENTS BELOW -- HAVE TRIED TAKING DF INTO GROUPBY TO APPLY GRP['COUNTER'] == range(len(grp)
    ##BUT WOULD NEED TO BROADCAST BACK TO ORIGINAL DATAFRAME, AND THEN INDEX ON GRP['COUNTER'] WHEN GOING THROUGH IF STATEMENTS
    
    j = 0
    for i in range(month_no + 1, month_no + 13):
    
        if len(df[df.index.month == i]) == len(triple[j]):
            ct = len(triple[j]) - len(df[df.index.month == i])
            df['rev_amt'][df.index.month == i] = triple[j]
            j += 1
        
        elif len(df[df.index.month == i]) < len(triple[j]):
            ct = len(triple[j]) - len(df[df.index.month == i])
            df['rev_amt'][df.index.month == i] = triple[j][ct:]
            j += 1
        
        elif len(df[df.index.month == i]) > len(triple[j]):
            ct = len(df[df.index.month == i]) - len(triple[j])
            trp = list(triple[j])
            for c in range(ct):
                trp.insert(0,0)
            df['rev_amt'][df.index.month == i] = trp
            j += 1
    
    df['line_of_bus'] = 'PEL'
    df['as_of_dte'] = '20160430'                                    ## change this date for new forecast
    df.reset_index(inplace=True)
    df.rename(columns={'index':'proj_dte'}, inplace=True)
    df['proj_dte'] = df['proj_dte'].apply(lambda x: x.strftime('%Y%m%d'))
    return df

a = to_sales_proj(triple, 4)                                         ## dataframe to view before sending to sales_proj database

def insert_into_sales_proj(df, conn, cursor):
    sql_sales_proj = '''INSERT INTO sales_proj (proj_dte, rev_amt, line_of_bus, as_of_dte) VALUES
                        (?,?,?,?)'''
                        
    tup = [tuple(i) for i in df.values]
    for row in tup:
        try:
            cursor.execute(sql_sales_proj, row)
        except sqlite3.IntegrityError:
            print (''' **ERROR: {0} FORECAST FOR {1} HAS ALREADY BEEN SUBMITTED TO SALES_PROJ DATABASE **'''.format(row[4],row[3]))
            
    conn.commit()
    conn.close()

#insert_into_sales_proj(a, conn, cursor)

