import sqlite3

def main():
    db = r'..\submittals.sqlite'
    #db = r'..\test.sqlite'
    conn = sqlite3.connect(db)
    c = conn.cursor()
    #build_table1(c)
    #build_table2(c)
    #build_table3(c)
    #build_table4(c)
    #build_table5(c)
    build_table6(c)
    conn.close()

def build_table1(c):
    # No Primary Key used in this table as it is going to be a run-on of LTs mailed multiple times
    c.execute('''CREATE TABLE lcs_submittals_all (
                case_number TEXT PRIMARY KEY,
                submittal_dte TEXT,
                rev_amt INT,
                line_of_bus TEXT,
                service_provided TEXT,
                submittal_type TEXT)''')
                
def build_table2(c):
    c.execute('''CREATE TABLE pel_submittals (
                    lt TEXT PRIMARY KEY,
                    job TEXT,
                    recap TEXT,
                    case_number TEXT,
                    submittal_dte TEXT,
                    rev_amt INT,
                    ta TEXT)''' )

def build_table3(c):
    c.execute('''CREATE TABLE mor_submittals (
                    lt TEXT PRIMARY KEY,
                    job TEXT,
                    recap TEXT,
                    case_number TEXT,
                    submittal_dte TEXT,
                    rev_amt INT,
                    ta TEXT)''' )

def build_table4(c):
    c.execute('''CREATE TABLE duplicate_submittals (
                    source TEXT,
                    lt TEXT,
                    job TEXT,
                    recap TEXT,
                    case_number_pend TEXT,
                    submittal_dte TEXT,
                    rev_amt INT,
                    ta TEXT,
                    case_number_keastone TEXT,
                    line_of_bus TEXT,
                    service_provided TEXT,
                    submittal_type TEXT)''' )

def build_table5(c):
    c.execute('''CREATE TABLE cash_curves (
                    line_of_bus TEXT NOT NULL,
                    date TEXT NOT NULL,
                    w1 REAL,
                    w2 REAL,
                    w3 REAL,
                    w4 REAL,
                    w5 REAL,
                    w6 REAL,
                    w7 REAL,
                    w8 REAL,
                    w9 REAL,
                    w10 REAL,
                    w11 REAL,
                    w12 REAL,
                    w13 REAL,
                    PRIMARY KEY (line_of_bus, date)
                )''')
                    

def build_table6(c):
    c.execute('''CREATE TABLE cash_forecast (
                    line_of_bus TEXT NOT NULL,
                    date TEXT NOT NULL,
                    w1 REAL,
                    w2 REAL,
                    w3 REAL,
                    w4 REAL,
                    w5 REAL,
                    w6 REAL,
                    w7 REAL,
                    w8 REAL,
                    w9 REAL,
                    w10 REAL,
                    w11 REAL,
                    w12 REAL,
                    w13 REAL,
                    PRIMARY KEY (line_of_bus, date)
                )''')
                    

if __name__ == '__main__':
    main()