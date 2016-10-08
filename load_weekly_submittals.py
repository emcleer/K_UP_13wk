import sqlite3
import build_weekly_submittals as subs

def main():
    db = 'submittals.sqlite'
    #db = 'test.sqlite'
    conn = sqlite3.connect(db)
    c = conn.cursor()
    load_pel_submittals(c)
    load_mor_submittals(c)
    load_lcs_submittals(c)
    conn.commit()
    conn.close()

def load_pel_submittals(cursor):
    df = subs.pel_submittals(subs.dte_rnge())
    tup = [tuple(i) for i in df.values]
    for row in tup:
        try:
            cursor.execute('''INSERT INTO pel_submittals (lt, job, recap, case_number, submittal_dte, rev_amt, ta) VALUES (?, ?, ?, ?, ?, ?, ?)''', row)
        except sqlite3.IntegrityError:
            print ('**ERROR: LT {0} ALREADY EXISTS IN DB -- PREVIOUSLY SUBMITTED**'.format(row[0]))
            cursor.execute('''INSERT INTO duplicate_submittals (source, lt, job, recap, case_number_pend, submittal_dte, rev_amt, ta)
                              VALUES ('PEL', ?, ?, ?, ?, ?, ?, ?)''', row)

def load_mor_submittals(cursor):
    df = subs.mor_submittals(subs.dte_rnge())
    tup = [tuple(i) for i in df.values]
    for row in tup:
        try:
            cursor.execute('''INSERT INTO mor_submittals (lt, job, recap, case_number, submittal_dte, rev_amt, ta) VALUES (?, ?, ?, ?, ?, ?, ?)''', row)
        except sqlite3.IntegrityError:
            print ('**ERROR: LT {0} ALREADY EXISTS IN DB -- PREVIOUSLY SUBMITTED**'.format(row[0]))
            cursor.execute('''INSERT INTO duplicate_submittals (source, lt, job, recap, case_number_pend, submittal_dte, rev_amt, ta)
                              VALUES ('MOR', ?, ?, ?, ?, ?, ?, ?)''', row)
                        
def load_lcs_submittals(cursor):
    df = subs.lcs_submittals(subs.dte_rnge())
    tup = [tuple(i) for i in df.values]
    for row in tup:
        try:
            cursor.execute('''INSERT INTO lcs_submittals_all (case_number, submittal_dte, rev_amt, line_of_bus, service_provided, submittal_type) VALUES (?, ?, ?, ?, ?, ?)''', row)
        except sqlite3.IntegrityError:
            print ('**ERROR: CASE NUMBER {0} ALREADY EXISTS IN DB -- PREVIOUSLY SUBMITTED**'.format(row[0]))
            cursor.execute('''INSERT INTO duplicate_submittals (source, case_number_keastone, submittal_dte, rev_amt, line_of_bus, service_provided, submittal_type)
                              VALUES ('LCS', ?, ?, ?, ?, ?, ?)''', row)
                              
if __name__ == '__main__':
    main()