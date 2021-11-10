# etl_redshift.py

import configparser
import psycopg2
import datetime as dt
import os
import subprocess
from sql_redshift_qry import load_stg_tbl_qry, load_tgt_tbl_qry
from sql_redshift_qry import data_quality_qry

def open_database():
    """
    Open database obtaining connection and cursor objects and return them.
    
    Assumes DB logon information is read from the configuration file: dwh.cfg
    stored in the application directory
    
    Return:
        (DB cursor) cur - cursor of open DB connection
        (DB connection) conn - connection to target database
    """

    print('\n###############################################')
    cur_ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print('{}: Opening immigration database'.format(cur_ts))
    
    # open connection to immigration database
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    cur_ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    db_nm = config['CLUSTER']['DB_NAME']
    print('{}: Opening immigration DB: {}'.format(cur_ts, db_nm))
    
    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".
                            format(*config['CLUSTER'].values()))
                            
        conn.set_session(autocommit=True)
        cur = conn.cursor()
    except psycopg2.Error as error: 
        print('Error: opening immigration DB: ',  db_nm)
        print (error)
        
        cur, conn = None, None
        return cur, conn
    else:
        cur_ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('{}: Opened immigration DB: {} successfully'
                .format(cur_ts, db_nm))
        
    return cur, conn

    
def load_stg_tbl(cur, conn):
    """
    Loads staging tables using the copy commands mentioned in the imported
    dictionary: load_stg_tbl_qry.
    
    Args:
        (DB cursor) cur - cursor of open DB connection where tables exist
        (DB connection) conn - open database where the tables exist

    Returns:
        (int) sts_cd - status code: 1 (error) or 0 (success)
    """
    
    print('\n###############################################')
    cur_ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print('{}: Loading staging tables'.format(cur_ts))
    for tbl_nm in load_stg_tbl_qry:
        sql_copy_cmd = load_stg_tbl_qry[tbl_nm]
        try:
            # load data into table
            cur_ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print('{}: Loading table: {}'.format(cur_ts, tbl_nm))

            cur.execute(sql_copy_cmd)
            conn.commit()

            # count records loaded into table
            cur_ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print('{}: Counting records loaded into table: {}'\
                .format(cur_ts, tbl_nm))
            
            query = 'SELECT COUNT(*) FROM {};'.format(tbl_nm)
            cur.execute(query)
            rowcount = cur.fetchone()[0]

            cur_ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print('{}: Loaded: {} into table: {} successfully'\
                .format(cur_ts, rowcount, tbl_nm))
                
        except psycopg2.Error as error: 
            print('Error: loading staging table:',  tbl_nm)
            print (error)
            return(1)
                  
    cur_ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print('{}: Loaded staging tables successfully'.format(cur_ts))
    return(0)
            
     
def load_tgt_tbl(cur, conn):
    """
    Inserts data into the target tables using the insert queries mentioned in
    the imported dictionary: load_tgt_tbl_qry.
    
    Args:
        (DB cursor) cur - cursor of open DB connection where tables exist
        (DB connection) conn - open database where the tables exist

    Returns:
        (int) sts_cd - status code: 1 (error) or 0 (success)
    """
    
    print('\n###############################################')
    for tbl_nm in load_tgt_tbl_qry:
        query = load_tgt_tbl_qry[tbl_nm]
        try:
            cur_ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print('{}: Loading data into table: {}'.format(cur_ts, tbl_nm))
            cur.execute(query)
            conn.commit()
              
            # count records loaded into table
            cur_ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print('{}: Counting records loaded into table: {}'\
                .format(cur_ts, tbl_nm))
                
            query = 'SELECT COUNT(*) FROM {};'.format(tbl_nm)
            cur.execute(query)
            rowcount = cur.fetchone()[0]
            
            cur_ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print('{}: Loaded: {} records into target table: {} successfully'.
                  format(cur_ts, rowcount, tbl_nm)) 
                
        except psycopg2.Error as error: 
            print('Error: loading data into target table:',  tbl_nm)
            print (error)
            return(1)
        
    return(0)
    
                      
def chk_data_quality(cur, conn):
    """
    Retrieve and prints row count of fact joined to the dimension tables using
    the select queries mentioned in the imported dictionary: data_quality_qry.
    
    Args:
        (DB cursor) cur - cursor of open DB connection where tables exist
        (DB connection) conn - open database where the tables exist

    Returns:
        (int) sts_cd - status code: 1 (error) or 0 (success)
    """
    
    print('\n###############################################')
    
    cur_dt = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{cur_dt}: retrieving fact joined to dimension table totals")
    
    fct_join_dim_total = {}
    for data_qlty_nm in data_quality_qry:
        cur_dt = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"{cur_dt}: retrieving: {data_qlty_nm}")

        try:
            query = data_quality_qry[data_qlty_nm]
            cur.execute(query)
            rowcount = cur.fetchone()[0]
        except psycopg2.Error as error: 
            print(f"Error: retrieving: {tbl_nm}")
            print (error)
            return(1)
            
        if rowcount == None:
            msg=f"No results returned for: {data_qlty_nm}"     
            raise ValueError(msg)
                    
        fct_join_dim_total.setdefault(data_qlty_nm, []) \
            .append(rowcount)
        
        cur_dt = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        msg=f"{data_qlty_nm} is: {fct_join_dim_total[data_qlty_nm][0]}"
            
        print(f"{cur_dt}: {msg}")
    
    cur_dt = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{cur_dt}: confirming all totals match")
    
    prev_rec_cnt = None
    prev_data_qlty_nm = None
    for data_qlty_nm in fct_join_dim_total:
        cur_rec_cnt = fct_join_dim_total[data_qlty_nm][0]
        if prev_rec_cnt == None:
            prev_rec_cnt = cur_rec_cnt
            prev_data_qlty_nm = data_qlty_nm
        else:
            if prev_rec_cnt != cur_rec_cnt:
                msg=f"Data quality check failed. {prev_data_qlty_nm} \
                    and {data_qlty_nm} total are not equal"
                    
                raise ValueError(msg)

            else:
                prev_rec_cnt = cur_rec_cnt
                prev_data_qlty_nm = data_qlty_nm
                
    cur_dt = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{cur_dt}: successfully confirm all totals: {cur_rec_cnt} match")
    return(0)
   
   
def main():
    """
    Establish database connection and initialize a cursor variable. Then
    load the staging and target tables respectively. Finally perform data
    quality check on the target tables.
    
    The DB logon information is read from the configuration file: dwh.cfg stored
    in the application directory
    
    Returns:
        (int) exit_cd - exit status code: 1 (error) or 0 (success)
    """
    
    # open database and obtain connection and cursor objects
    cur, conn = open_database()
    if cur == None:
        exit(1)

    # load data into stage tables except immigration stage
    sts_cd = load_stg_tbl(cur, conn)
    if sts_cd == 1:
        conn.close()
        exit(1)

    # load data into target dimension tables except for time period dimension 
    sts_cd = load_tgt_tbl(cur, conn)
    if sts_cd == 1:
        conn.close()
        exit(1)

    # check data quality
    sts_cd = chk_data_quality(cur, conn)
    conn.close()
    if sts_cd == 1:
        exit(1)
    
    exit(0)

if __name__ == "__main__":
    main()