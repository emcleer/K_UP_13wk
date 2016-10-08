from __future__ import print_function
import sqlite3
import pandas as pd
import lcs_cash_fcst as lcs
import pel_mor_cash_fcst as p_m

def main():
    db = 'submittals.sqlite'
    conn = sqlite3.connect(db)
    c = conn.cursor()
    
    dtes = p_m.dte_range()
    sqls = p_m.sql_queries(dtes)
    
    p = p_m.pel(sqls, conn, dtes)
    m = p_m.mor(sqls, conn, dtes)
    l = lcs.lcs_dict(dtes)
    
    load_fcst_db(p, m, l, c, sqls)
    conn.commit()
    conn.close()

def load_fcst_db(peldict, mordict, lcsdict, cursor, sql):
    df1 = pd.DataFrame([peldict, mordict])
    tup = [tuple(i) for i in df1.values]
    for row in tup:
        try:
            cursor.execute(sql['cashfcst1'], row)
        except sqlite3.IntegrityError:
            print (''' **ERROR: {0} FORECAST FOR {1} HAS ALREADY BEEN SUBMITTED TO CASH_FORECAST DATABASE **'''.format(row[0],row[1]))
    
    df2 = pd.DataFrame(lcsdict)
    df2 = df2.T
    tup2 = [tuple(i) for i in df2.values]
    for row in tup2:
        try:
            cursor.execute(sql['cashfcst2'], row)
        except sqlite3.IntegrityError:
            print (''' **ERROR: {0} FORECAST FOR {1} HAS ALREADY BEEN SUBMITTED TO CASH_FORECAST DATABASE **'''.format(row[13],row[14]))


if __name__ == '__main__':
    main()