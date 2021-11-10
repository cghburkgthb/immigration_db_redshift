import configparser
import psycopg2
import datetime as dt
from sql_redshift_qry import create_tbl_qry, drop_tbl_qry

def drop_tables(cur, conn):
    """
    Drops the staging and target tables using the DDL statements mentioned in
    the imported dictionary: drop_tbl_qry.
    
    Args:
        (DB cursor) cur - cursor of open DB connection where tables exist
        (DB connection) conn - open database where the tables exist

    Returns:
        (int) sts_cd - status code: 1 (error) or 0 (success)
    """
    
    print('\n###############################################')
    for tbl_nm  in drop_tbl_qry:
        print('Dropping table:', tbl_nm)

        query = drop_tbl_qry[tbl_nm]
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as error: 
            print('Error: dropping table: ',  tbl_nm)
            print (error)
            return(1)
        else:
            print('Dropped table: {} successfully'.format(tbl_nm))
            
    return(0)


def create_tables(cur, conn):
    """
    Creates the staging and target tables using the DDL statements mentioned in
    the imported dictionary: create_tbl_qry.
    
    Args:
        (DB cursor) cur - cursor of open DB connection where tables exist
        (DB connection) conn - open database where the tables exist

    Returns:
        (int) sts_cd - status code: 1 (error) or 0 (success)
    """
    
    print('\n###############################################')
    for tbl_nm  in create_tbl_qry:
        print('Creating table:', tbl_nm)
        
        query = create_tbl_qry[tbl_nm]
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as error: 
            print('Error: creating table: ',  tbl_nm)
            print (error)
            return(1)
        else:
            print('Created table: {} successfully'.format(tbl_nm))
            
    return(0)



def main():
    """
    Establish database connection and initialize cursor variable. Then it delete
    the DB tables, if they exists, before creating the database tables.  
    
    The DB logon information is read from the configuration file: dwh.cfg stored
    in the application directory
    
    Returns:
        (int) exit_cd - exit status code: 1 (error) or 0 (success)
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print('connecting to DB')
    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".
                            format(*config['CLUSTER'].values()))
                            
        conn.set_session(autocommit=True)
        cur = conn.cursor()
    except psycopg2.Error as error: 
        print('Error: opening immigration DB')
        print (error)
    
    print('connected to DB')
    
    #cur, conn = create_database()
    if cur == 'none':
        exit(1)
    
    sts_cd = drop_tables(cur, conn)
    if sts_cd == 1:
        exit(1)
        
    sts_cd = create_tables(cur, conn)

    conn.close()
    exit(0)

if __name__ == "__main__":
    main()