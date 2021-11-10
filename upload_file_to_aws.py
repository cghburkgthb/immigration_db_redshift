import configparser
import os
import boto3
from botocore.exceptions import NoCredentialsError
import datetime as dt

def upload_to_aws(aws_access_key, aws_secret_key, src_dir, s3_bucket_nm):
    """
    Upload data files with a directory to AWS S3.
    
    Args:
        (str) aws_access_key - AWS access key
        (str) aws_secret_key - AWS secret key
        (str) src_dir - source file directory
        (str) s3_bucket_nm - AWS S3 bucket name
    """

    s3 = boto3.client('s3', aws_access_key_id=aws_access_key,
                      aws_secret_access_key=aws_secret_key)

    # Obtain the list of immigration data files
    src_f_lst=os.listdir(src_dir)
    for src_f_nm in src_f_lst:
        src_f_path = src_dir + '/' + src_f_nm
        
        cur_ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('{}: uploading: {} to AWS S3'.format(cur_ts, src_f_path))
        try:
            s3.upload_file(src_f_path, s3_bucket_nm, src_f_nm)
            cur_ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print('{}: uploaded: {} to AWS S3'.format(cur_ts, src_f_path))
        except FileNotFoundError:
            cur_ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print('{}: failed while uploading file: {} to AWS S3'.\
                format(cur_ts, src_f_path))
            return(1)
        except NoCredentialsError:
            print("Credentials not available")
            return(1)
        
    return(0)


def main():
    """
    Parse data warehouse configuration file and call functions to upload local
    data files AWS S3 bucket.
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')  # windows configuration file
    
    aws_access_key = config['AWS_KEY']['ACCESS_KEY']
    aws_secret_key = config['AWS_KEY']['SECRET_KEY']

    # upload misc data files to AWS S3 bucket
    misc_data_loc_dir = config['SRC_DATA']['misc_data_loc_dir']
    s3_bucket_nm = config['S3']['s3_misc_data_bucket']
    sts_cd = upload_to_aws(aws_access_key, aws_secret_key, misc_data_loc_dir
                            , s3_bucket_nm)
    if sts_cd == 1:
        exit(1)
    
    # upload immigration data files to AWS S3 bucket
    imgrtn_data_loc_dir = config['SRC_DATA']['imgrtn_data_loc_dir']
    s3_bucket_nm = config['S3']['s3_imgrtn_data_bucket']
    uploaded = upload_to_aws(aws_access_key, aws_secret_key, imgrtn_data_loc_dir
                                , s3_bucket_nm)
    if sts_cd == 1:
        exit(1)
    
    exit(0)
if __name__ == "__main__":
    main()