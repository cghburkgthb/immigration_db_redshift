import configparser
import pandas as pd
import os
import subprocess
import datetime as dt

def clean_imgrtn_data(imgrtn_src_dir, imgrtn_loc_dir):
    """
    Reads and cleans immigration data files and store the clean files in a local
    directory.
    
    Args:
        (str) imgrtn_src_dir - immigration source data directory
        (str) imgrtn_loc_dir - immigration location data directory
    """

    cur_ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print('{}: cleaning immigration data files in directory: {} '. \
            format(cur_ts, imgrtn_src_dir))
            
    # Obtain the list of immigration data files
    imgrtn_data_f_lst=os.listdir(imgrtn_src_dir)

    for src_f_nm  in imgrtn_data_f_lst:
        # assemble source data file path
        src_path = imgrtn_src_dir + '/' + src_f_nm
        
        cur_dt = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('{}: reading file: {} into data frame'.format(cur_dt, src_path))   
        imgrtn_df = pd.read_sas(src_path, 'sas7bdat', encoding="ISO-8859-1")
        
        # drop columns that are not going to be used in database
        cur_dt = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        msg = 'dropping columns from data frame that are not going to be used'
        print('{}: {}'.format(cur_dt, msg))
              
        if src_f_nm == 'i94_jun16_sub.sas7bdat':
            imgrtn_df.drop(['dtadfile', 'entdepa', 'entdepd', 'entdepu'
                            , 'matflag', 'insnum'
                            , 'admnum', 'fltno', 'delete_dup', 'delete_visa'
                            , 'validres', 'delete_recdup', 'delete_days'
                            , 'delete_mexl'], axis = 1, inplace=True)
        else:
            imgrtn_df.drop(['dtadfile', 'entdepa', 'entdepd', 'entdepu'
                            , 'matflag', 'insnum', 'admnum'
                            , 'fltno'], axis = 1, inplace=True) 
        
        # cleaning data frame columns
        cur_dt = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('{}: cleaning data frame columns'.format(cur_dt))
        
        # replace NULL values in i94mode with 9 (Not reported)
        imgrtn_df["i94mode"].fillna(9, inplace = True)
        
        #replace D/S values in dtaddto with 12319999
        imgrtn_df["dtaddto"].replace({"D/S": '12319999'}, inplace=True)
        
        # convert date interger values to date
        cur_dt = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        msg = 'converting data frame date integer values to dates'
        print('{}: {}'.format(cur_dt, msg))
        
        # convert date integer values
        imgrtn_df['arrl_date'] = \
            pd.to_timedelta(imgrtn_df['arrdate'], unit='d', errors='ignore') + \
            pd.datetime(1960, 1, 1)
            
        imgrtn_df['dep_date'] = \
            pd.to_timedelta(imgrtn_df['depdate'], unit='d', errors='ignore') + \
            pd.datetime(1960, 1, 1)
        
        # drop columns that were converted to real date columns above
        imgrtn_df.drop(['arrdate', 'depdate'], axis = 1, inplace=True) 
        
        # convert columns to correct data types
        cur_dt = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        msg = 'converting data frame columns to correct data types'
        print('{}: {}'.format(cur_dt, msg))
        data_type_dict = {'cicid': int,'i94yr': int,'i94mon': int,'i94visa': int
                          ,'count': int
                         } 
        imgrtn_df = imgrtn_df.astype(data_type_dict)

        # determine path of CSV output file
        f_nm_prfx = src_f_nm.split(".")[0]
        dest_path = imgrtn_loc_dir + '/' + f_nm_prfx + '.csv'
        
        cur_dt = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('{}: writing data frame to file: {}'.format(cur_dt, dest_path))  
        imgrtn_df.to_csv(dest_path, encoding='utf-8', index=False, header=True)
        
        # remove bad characters from data file
        cur_ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('{}: removing bad characters from data file: {}'.\
            format(cur_ts, dest_path))    
        cmd=['perl', '-pi', '-e s/[^a-zA-Z0-9,\.\-\n]+//g', dest_path]
        try:
            subprocess.run(cmd, check=True)
            cur_ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print('{}: cleaned data file: {} successfully'.\
                format(cur_ts, dest_path))
        except subprocess.CalledProcessError as err:
            print(err.stderr.decode('utf-8'))
            
        # compress data file
        cur_ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('{}: compress data file: {}'.format(cur_ts, dest_path))  
        cmd=['gzip', '-f', dest_path]
        try:
            subprocess.run(cmd, check=True)
            cur_ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print('{}: cleaned data file: {} successfully'.\
                format(cur_ts, dest_path))
        except subprocess.CalledProcessError as err:
            print(err.stderr.decode('utf-8'))
        
        #clean data frame  
        imgrtn_df = imgrtn_df.iloc[0:0]
        
def main():
    """
    Parse data warehouse configuration file and call function to clean
    immigration data.
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    imgrtn_src_dir = config['SRC_DATA']['imgrtn_data_src_dir']
    imgrtn_loc_dir = config['SRC_DATA']['imgrtn_data_loc_dir']
    clean_imgrtn_data(imgrtn_src_dir, imgrtn_loc_dir)

if __name__ == "__main__":
    main()