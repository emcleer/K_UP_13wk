from __future__ import print_function
import sqlite3
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta, FR, MO

def main():
    db = r'submittals.sqlite'
    conn = sqlite3.connect(db)
    
    dtes = dte_rnge()
    #dtes = {'start': '20160601', 'end': '20160630'}
                                
    morsql = '''SELECT l.line_of_bus, COUNT(m.lt), SUM(m.rev_amt)
                FROM mor_submittals as m
                LEFT OUTER JOIN lcs_submittals_all as l
                ON m.case_number = l.case_number 
                WHERE m.submittal_dte BETWEEN {0} AND {1}
                GROUP BY line_of_bus'''.format(dtes['start'], dtes['end'])
    
    lcssql = '''SELECT line_of_bus, COUNT(line_of_bus), SUM(rev_amt) FROM lcs_submittals_all 
                WHERE submittal_dte BETWEEN {0} AND {1} 
                AND submittal_type = 'LCS Submittal'
                GROUP BY line_of_bus'''.format(dtes['start'], dtes['end'])
    
    pelsql = '''SELECT l.line_of_bus, l.service_provided, COUNT(p.lt), SUM(p.rev_amt)
                FROM pel_submittals as p
                LEFT OUTER JOIN lcs_submittals_all as l
                ON p.case_number = l.case_number 
                WHERE p.submittal_dte BETWEEN {0} AND {1}
                GROUP BY line_of_bus, service_provided'''.format(dtes['start'], dtes['end'])
    
    dupsql = '''SELECT COALESCE(line_of_bus, source) AS lob, COUNT(COALESCE(case_number_keastone, lt)), SUM(rev_amt)
                FROM duplicate_submittals
                WHERE submittal_dte BETWEEN {0} AND {1}
                GROUP BY lob '''.format(dtes['start'], dtes['end'])
    
    
    print('''\n *** TOTAL SUBMITTALS FOR TIME PERIOD {0} TO {1} *** '''.format(dtes['start'], dtes['end']))
    print(''' *** ANY 'BULK' LINE BELOW SHOULD BE ADDED-BACK UNDER THE LCS LOB *** \n''')
    total_company(morsql, lcssql, pelsql, conn, dtes)
    print('''\n 
*** BELOW SECTION IS A RECAP OF DUPLICATE SUBMITTALS FROM ***
***       THIS WEEK NOT INCLUDED IN THE FIRST RECAP       ***
***          -- MAY NEED TO RESEARCH FURTHER --           *** \n''')       
    dup_submittals(dupsql, conn, dtes)
        
def dte_rnge():  
    l_fri = datetime.today() + relativedelta(weekday=FR(-1))
    l_mon = l_fri + relativedelta(weekday=MO(-1))
    l_fri_string = l_fri.strftime('%Y%m%d')   #no dashes for SQLite query
    l_mon_string = l_mon.strftime('%Y%m%d')   #no dashes for SQLite query
    return {'start': l_mon_string, 'end': l_fri_string}

def pel_alloc(row):
    if (row['line_of_bus'] == 'Securities') and ((row['service_provided'] == 'DRO') or (row['service_provided'] == 'Deep Search')):
        return 'Bulk Sec Add-back'
    
    elif (row['line_of_bus'] == 'International') and ((row['service_provided'] == 'DRO') or (row['service_provided'] == 'Deep Search')):
        return 'Bulk Int\'l Add-back'
    
    else:
        return 'PEL'
      
def pel(sql, conn, d):
    df = pd.read_sql(sql, conn)
    df.line_of_bus = df.apply(pel_alloc, axis=1)
    df2 = df[['line_of_bus', 'COUNT(p.lt)', 'SUM(p.rev_amt)']]
    grp = df2.groupby('line_of_bus').sum()
    df3 = pd.DataFrame(grp).reset_index()
    df3.rename(columns={'COUNT(p.lt)':'count', 'SUM(p.rev_amt)':'revenue'}, inplace=True)
    return df3

def mor(sql, conn, d):
    df = pd.read_sql(sql, conn)
    df.line_of_bus = 'MOR'
    grp = df.groupby('line_of_bus').sum()
    df2 = pd.DataFrame(grp).reset_index()
    df2.rename(columns={'COUNT(m.lt)':'count', 'SUM(m.rev_amt)':'revenue'}, inplace=True)
    return df2

def lcs(sql, conn, d):
    df = pd.read_sql(sql, conn)
    df.rename(columns={'COUNT(line_of_bus)':'count', 'SUM(rev_amt)':'revenue'}, inplace=True)
    return df

def total_company(sql1, sql2, sql3, conn, d):
    dfs = []
    dfs.append(mor(sql1, conn, d))
    dfs.append(lcs(sql2, conn, d))
    dfs.append(pel(sql3, conn, d))
    total = pd.concat(dfs, axis=0).reset_index(drop=True)
    total.revenue = total.revenue.apply(lambda x: '{:,}'.format(x))
    print (total)

def dup_submittals(sql, conn, d):
    df = pd.read_sql(sql, conn)
    df.rename(columns={'lob':'line_of_bus', 'COUNT(COALESCE(case_number_keastone, lt))':'count', 'SUM(rev_amt)': 'revenue'}, inplace=True)
    df.revenue = df.revenue.apply(lambda x: '{:,}'.format(x))
    print (df)
         
if __name__ == '__main__':
    main()
