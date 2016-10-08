from __future__ import print_function, division
from datetime import datetime
from dateutil.relativedelta import relativedelta, MO, FR, SA
import pandas as pd
import keane_db


def manual_dte_rnge():
    d = {  'as400start'    : '20160404'   ,      #can't have dashes for dataframe filter
           'as400end'      : '2016-04-08' ,      #needs dashes for pending reports file name
           'keastonestart' : '2016-04-29' ,      #needs dashes for SQL query params
           'keastoneend'   : '2016-05-01' }      #needs dashes for SQL query params
    return d

def dte_rnge():  
    l_fri = datetime.today() + relativedelta(weekday=FR(-1))
    l_mon = l_fri + relativedelta(weekday=MO(-1))
    two_sats_string = (l_fri + relativedelta(weekday=SA(-1))).strftime('%Y-%m-%d') #needs dashes for SQL query params
    last_sat_string = (l_fri + relativedelta(weekday=SA(1))).strftime('%Y-%m-%d')  #needs dashes for SQL query params
    l_fri_string = l_fri.strftime('%Y-%m-%d') #need dashes to read correct string for filepath
    l_mon_string = l_mon.strftime('%Y%m%d')   #need without dashes to filter correct dates in pending reports
    return {'as400start': l_mon_string, 'as400end': l_fri_string, 'keastonestart':two_sats_string, 'keastoneend':last_sat_string}
    
def pel_submittals(d):
    df = pd.read_csv(r'I:\FINANCE\ACCOUNTING\UPRR\Pending Reports\PEL\{0}.csv'.format(d['as400end']), dtype={'Process Date':str})
    df2 = df[df['Process Date'] >= d['as400start']].copy()
    df2['rev_amt'] = df2['Proc.Fee $'] + df2['BOI Fee Amount'] + df2['Proc.Fee Amount "DIV"']
    df2 = df2[['LT#', 'Job#', 'Report#', 'Case Number', 'Process Date', 'rev_amt', 'Trans.Agent Name']]
    df2.rename(columns={'Job#':'job', 'LT#':'lt', 'Case Number':'case_number', 'Report#':'recap', 'Process Date':'submittal_dte', 'Trans.Agent Name':'ta'}, inplace=True)
    df2['rev_amt'] = df2['rev_amt'].astype(int)
    return df2

def mor_submittals(d):
    df = pd.read_csv(r'I:\FINANCE\ACCOUNTING\UPRR\Pending Reports\MOR\{0}.csv'.format(d['as400end']), dtype={'Process Date':str})
    df2 = df[df['Process Date'] >= d['as400start']].copy()
    df2['rev_amt'] = df2['Proc.Fee Amount'] + df2['BOI Fee Amount']
    df2 = df2[['LT#', 'Job#', 'Report#', 'Case Number', 'Process Date', 'rev_amt', 'Trans.Agent Name']]
    df2.rename(columns={'Job#':'job', 'LT#':'lt', 'Case Number':'case_number', 'Report#':'recap', 'Process Date':'submittal_dte', 'Trans.Agent Name':'ta'}, inplace=True)
    df2['rev_amt'] = df2['rev_amt'].astype(int)
    return df2
			
def lcs_submittals(d):
    sql = open(r'keastone_submittals.sql').read()
    conn, curs = keane_db.connect_to_keastone()
    curs.execute(sql, (d['keastonestart'], d['keastoneend']))
    res = curs.fetchall()
    df = pd.DataFrame([list(i) for i in res])
    df.rename(columns={0:'case_number', 1:'submittal_dte', 2:'rev_amt', 3:'line_of_bus', 4:'service_provided', 5:'submittal_type'}, inplace=True)
    df['submittal_dte'] = df['submittal_dte'].apply(lambda x: x.strftime('%Y%m%d'))
    df['rev_amt'] = df['rev_amt'].astype(int)
    return df
