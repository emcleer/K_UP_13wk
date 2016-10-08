import pandas as pd
from dateutil.relativedelta import relativedelta, FR, MO
from datetime import datetime
import numpy as np

def dte_range():
    end = datetime.today() + relativedelta(weekday=FR(-1))
    start = end + relativedelta(weekday=MO(-4))
    end = end.strftime('%Y%m%d')
    start = start.strftime('%Y%m%d')
    return {'start': start, 'end': end}

def sql_queries(d):
    pelsubs = '''SELECT * FROM pel_submittals WHERE submittal_dte BETWEEN {0} AND {1}'''.format(d['start'], d['end'])
    morsubs = '''SELECT * FROM mor_submittals WHERE submittal_dte BETWEEN {0} AND {1}'''.format(d['start'], d['end'])
    lcssubs = ''' fill in '''
    pelcurves = ''' SELECT c.w1, c.w2, c.w3, c.w4, c.w5, c.w6, c.w7, c.w8, c.w9, c.w10, c.w11, c.w12, c.w13 FROM cash_curves as c
                        WHERE line_of_bus = 'PEL' AND date = '20160429' '''
    morcurves = ''' SELECT c.w1, c.w2, c.w3, c.w4, c.w5, c.w6, c.w7, c.w8, c.w9, c.w10, c.w11, c.w12, c.w13 FROM cash_curves as c
                        WHERE line_of_bus = 'MOR' AND date = '20160429' '''
    lcscurves = ''' fill in '''
    
    cashfcst1 = '''INSERT INTO cash_forecast (line_of_bus, date, w1, w2, w3, w4, w5, w6, w7, w8, w9, w10, w11, w12, w13) VALUES
                    (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
    
    cashfcst2 = '''INSERT INTO cash_forecast (w1, w2, w3, w4, w5, w6, w7, w8, w9, w10, w11, w12, w13, line_of_bus, date) VALUES
                    (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
    
    pelsalesproj = ''' SELECT proj_dte, rev_amt FROM sales_proj WHERE line_of_bus = 'PEL' 
                        AND as_of_dte = (SELECT MAX(as_of_dte) FROM sales_proj) '''

    return {'pelsubs':pelsubs, 'morsubs':morsubs, 'lcssubs':lcssubs, 'pelcurves':pelcurves, 'morcurves':morcurves, 'lcscurves':lcscurves,
                'cashfcst1':cashfcst1, 'cashfcst2':cashfcst2, 'pelsalesproj':pelsalesproj}

def pel_proj_sales(sql, conn):
    df = pd.read_sql(sql['pelsalesproj'], conn, parse_dates=['proj_dte'])
    df.set_index('proj_dte', inplace=True)
    df2 = df.resample('W-FRI', how='sum')
    df2['age_weeks'] = np.floor(((datetime.today() + relativedelta(weekday=FR(-1)) - df2.index) / np.timedelta64(1, 'W')))
    df3 = df2[df2['age_weeks'] < 0]
    df4 = df3.head(13).copy()
    df4.sort('age_weeks', inplace=True)
    return df4

def mor_prior_four_wk_avg(sql, conn):
    df = pd.read_sql(sql['morsubs'], conn, parse_dates=['submittal_dte'])
    df['age_weeks'] = np.ceil(((datetime.today() + relativedelta(weekday=FR(-1))) - df.submittal_dte) / np.timedelta64(1, 'W'))
    avg = float(df.groupby('age_weeks')['rev_amt'].sum().sum() / 4)
    d = {}
    for i in range(-13,0):
        d[i] = avg
    df2 = pd.DataFrame(data=list(d.values()), index=list(d.keys())).reset_index()
    df2.rename(columns={'index':'age_weeks', 0:'rev_amt'}, inplace=True)
    df2.sort(columns='age_weeks', inplace=True)
    return df2

def pel_pending():
    d = (datetime.today() + relativedelta(weekday=FR(-1))).strftime('%Y-%m-%d')
    df = pd.read_csv(r'I:\FINANCE\ACCOUNTING\UPRR\Pending Reports\PEL\{0}.csv'.format(d), parse_dates=['Process Date'])
    df['age_weeks'] = np.ceil(((datetime.today() + relativedelta(weekday=FR(-1))) - df['Process Date']) / np.timedelta64(1, 'W'))
    df['rev_amt'] = df['Proc.Fee $'] + df['BOI Fee Amount'] + df['Proc.Fee Amount "DIV"']
    grp = df.groupby('age_weeks')['rev_amt'].sum()
    df2 = pd.DataFrame(grp).reset_index()
    return df2

def mor_pending():
    d = (datetime.today() + relativedelta(weekday=FR(-1))).strftime('%Y-%m-%d')
    df = pd.read_csv(r'I:\FINANCE\ACCOUNTING\UPRR\Pending Reports\MOR\{0}.csv'.format(d), parse_dates=['Process Date'])
    df['age_weeks'] = np.ceil(((datetime.today() + relativedelta(weekday=FR(-1))) - df['Process Date']) / np.timedelta64(1, 'W'))
    df['rev_amt'] = df['Proc.Fee Amount'] + df['BOI Fee Amount']
    grp = df.groupby('age_weeks')['rev_amt'].sum()
    df2 = pd.DataFrame(grp).reset_index()
    return df2

def pel(sql, conn, dtes):
    df1 = pel_proj_sales(sql,conn)
    df2 = pel_pending()
    df3 = pd.concat([df1, df2], axis=0, ignore_index=True)
    vals = df3.set_index('age_weeks').values
    d = {}
    curves = pd.read_sql(sql['pelcurves'], conn).T.values
    for i in range(1,14):
        fcst = vals[13:26] * curves
        d[i] = fcst.sum()
        vals[13:26] -= fcst
        vals = np.roll(vals, 1)
    d[-1], d[0] = 'PEL', dtes['end']
    return d

def mor(sql, conn, dtes):
    df1 = mor_prior_four_wk_avg(sql,conn)
    df2 = mor_pending()
    df3 = pd.concat([df1, df2], axis=0, ignore_index=True)
    vals = df3.set_index('age_weeks').values
    d = {}
    curves = pd.read_sql(sql['morcurves'], conn).T.values
    for i in range(1,14):
        fcst = vals[13:26] * curves
        d[i] = fcst.sum()
        vals[13:26] -= fcst
        vals = np.roll(vals, 1)
    d[-1], d[0] = 'MOR', dtes['end']
    return d