import numpy as np
import datetime
from collections import defaultdict
import keane_db

def lcs_dict(dtes):
    rates = np.genfromtxt(r'..\rates.generic', skip_header=1, delimiter=',')
    # as400 = set(np.loadtxt('as400submittals.txt', dtype=str))
    
    # qry = ", ".join(["?"] * len(as400))
    sql = open(r'..\keastone_submittals.sql').read()
    
    today = datetime.date.today()
    fri = today + datetime.timedelta((4 - today.weekday()) % 7)
    ref = fri - datetime.timedelta(weeks=21) # our curve has 21 data points
    
    conn, curs = keane_db.connect_to_keastone()
    curs.execute(sql, [ref])
    res = curs.fetchall()
    
    submittals = {}
    for r in res:
        product_type = r[0]
        if product_type not in submittals:
            submittals[product_type] = np.zeros(21)
        week = r[1]
        week_num = (fri - week).days / 7
        week_pos = week_num - 1
        submittals[product_type][week_pos] = r[2]
    
    l = []
    
    for n in range(13):
        tot = 0
        for k in submittals:
            amt = sum(submittals[k] * rates)
            tot += amt
            l.append([n+1, k, amt])
            # assume submittals on a 4 week moving average
            avg = sum(submittals[k][:4]) / 4.0
            #f.writerow([k, n+1, amt, avg])
            submittals[k] = np.roll(submittals[k], 1)
            submittals[k][0] = avg
    
    d = defaultdict(list)
    
    for elem in l:
        #d[elem[0]].append(elem[1:])
        d[elem[1]].append(elem[2])
    
    for i in d:
        d[i].append(i)
        d[i].append(dtes['end'])
    
    return d