import os
import random
import pandas as pd
import sqlite3
from queries import *
import time
from selection import *
from file_handling_docker import *
from functions import *
from datetime import datetime
from queries import *
import hashlib


def ext_scan_file(file_data_df):
    # Absolute File Path
    file_path_abs = file_data_df['file_path']

    # File Name
    file_nm = file_data_df['filenames']

    try:
        # Get file stats
        file_stat = os.stat(file_path_abs)

        # Get File Size
        file_sz = file_stat.st_size

        # Get Last Access Time
        file_lst_accss_tm = file_stat.st_atime

        # Get Last Modify Time
        file_lst_mdfy_tm = file_stat.st_mtime

        # Get Create Time
        file_lst_crt_tm = file_stat.st_ctime

        # Make Dictionary
        file_info_dict = {'file_path': [file_path_abs],
                          'file_name_ext': [file_nm],
                          'file_size_bytes': [file_sz],
                          'last_access_time_unix': [file_lst_accss_tm],
                          'last_modify_time_unix': [file_lst_mdfy_tm],
                          'create_time_unix': [file_lst_crt_tm],
                          'hash': hashlib.sha224(file_path_abs.encode()).hexdigest()}

        # Make into DataFrame
        file_info_df = pd.DataFrame.from_dict(file_info_dict)

        # Return Dictionary
        return file_info_df
    except FileNotFoundError:
        print("File: '{file_path}' is missing. Skipping...".format(file_path=file_path_abs))
        return None

    except OSError:
        print("File: '{file_path}' cannot be accessed. Skipping...".format(file_path=file_path_abs))
        return None


def meta_data_format(data):
    # Put file list into DataFrame
    meta_data = data

    # Get File Name and Extension
    meta_data['file_name'] = meta_data['file_name_ext'].apply(
        lambda x: os.path.splitext(x)[0])
    meta_data['ext'] = meta_data['file_name_ext'].apply(
        lambda x: os.path.splitext(x)[1])

    # Get File Directory
    meta_data['dir_path'] = meta_data.apply(
        lambda x: x['file_path'][:-len(x['file_name_ext'])], axis=1)

    # Get File Size
    meta_data['file_size'] = meta_data['file_size_bytes'].apply(sizeof_fmt)

    # Get Last Access Time
    meta_data['last_access_time'] = meta_data['last_access_time_unix'].apply(
        lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'))

    # Get Last Modify Time
    meta_data['last_modify_time'] = meta_data['last_access_time_unix'].apply(
        lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'))

    # Get Last Create/Metadata Change Time (Windows/Unix)
    meta_data['create_time'] = meta_data['create_time_unix'].apply(
        lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'))

    # Create Report Time
    meta_data['report_time'] = time.time()
    meta_data['report_time_str'] = meta_data['report_time'].apply(
        lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'))

    # Create File Age
    meta_data['file_age'] = meta_data['report_time'] - meta_data['create_time_unix']
    meta_data['file_age_str'] = meta_data['file_age'].apply(
        lambda x: elapsed_time_stringify(x, incld_all=False, incld_0=False))

    # Create time since last accessed
    meta_data['time_since_last_access'] = meta_data['report_time'] - meta_data['last_access_time_unix']
    meta_data['time_since_last_access_str'] = meta_data['time_since_last_access'].apply(
        lambda x: elapsed_time_stringify(x, incld_all=False, incld_0=False))

    # Return Data
    return meta_data


def scan_chunk(chunk, index_func, index_ln_func, conn_func, tbl_nm_func, incmplt_tbl_nm_func):
    # Perform extended scan
    print("Scanning index {index_str} out of {index_ln_str}".format(index_str=str(index_func + 1),
                                                                    index_ln_str=str(index_ln_func)))

    # Scan Files and create DataFrame
    chnk_pull_df = pd.concat(chunk.apply(ext_scan_file, axis=1).to_list())

    # Create full meta-data
    chnk_meta_df_func = meta_data_format(chnk_pull_df)

    # Upload data to sqlite database
    chnk_meta_df_func.to_sql(tbl_nm_func, con=conn_func, if_exists='append', index=False)

    # Upload file_paths to temp table
    tmp_tbl_nm = "temp_table_" + str(random.randrange(0, 1000000000))
    chnk_meta_df_func.to_sql(tmp_tbl_nm, con=conn_func, if_exists='replace', index=False)

    # Delete rows from incomplete table using temp table
    delete_row_query_run_2(conn_func, incmplt_tbl_nm_func, tmp_tbl_nm, 'file_path', 'file_path')

    # Delete temp table
    drop_query_run_1(conn_func=conn_func, tbl_nm_delete_func=tmp_tbl_nm)

    # Commit Changes to Database
    conn_func.commit()

    # Return data
    return chnk_meta_df_func


def folder_file_analysis(file_scn_pth_lst, csv_out=False, db_path='db.db', tm_bfr_rscn=86400, multithread=False):
    # Create db connection
    conn = sqlite3.connect(db_path)

    # Table list
    incmplt_tbl_nm = 'Incomplete Table'
    cmplt_tbl_nm = 'Completed Scan Table'

    # View List
    output_view_nm = "Output View"

    # File List
    file_lst = list()

    # Start Timer
    start_tm = time.time()

    # Print Start
    print("Starting Scanning...")

    # Walk through scan path and get list of files
    for file_scn_pth in file_scn_pth_lst:
        for (dirpath, dirnames, filenames) in os.walk(file_scn_pth):
            if len(filenames) != 0:
                for i in filenames:
                    file_lst.append({'dirpath': dirpath,
                                     'filenames': i,
                                     'file_path': os.path.join(dirpath, i),
                                     'hash': hashlib.sha224(os.path.join(dirpath, i).encode()).hexdigest(),
                                     'scantime': time.time()})

    # End Timer
    end_tm = time.time()

    # Calculate Elapsed Time
    elpsd_tm = end_tm - start_tm

    # Create Elapsed Time String
    time_str = elapsed_time_stringify(elpsd_tm, False, False)

    # Print End of Scan
    print("Initial Scan Complete! Time to complete: {elpsd_tm_str}".format(elpsd_tm_str=time_str))

    # Convert file list to dataframe
    file_list_df = pd.DataFrame(file_lst)

    # Uploading file list to database
    print("Uploading file list to database...")

    # Write file list to incomplete file list
    file_list_df.to_sql(incmplt_tbl_nm, con=conn, if_exists='append', index=False)

    # Deduping database
    print("Deduping database...")

    # Dedupe Incomplete Table
    dedupe_query_run_1(conn_func=conn, tbl_nm=incmplt_tbl_nm)

    # Compare completed run to incomplete table to see if last run was within refresh time
    try:
        limit_to_scan_difference_query_run_1(conn_func=conn,
                                             incmplt_tbl_func=incmplt_tbl_nm,
                                             cmplt_tbl_func=cmplt_tbl_nm,
                                             scn_tm_lmt=tm_bfr_rscn)
    except sqlite3.OperationalError:
        print('No complete table found. Running Program...')

    # Print Start of extended test with wait
    print('Starting Extended Scan...')
    time.sleep(3)
    for i in range(0, 5):
        print()

    # Get list of files to scan from db
    file_lst_df = query_tbl_run_1(tbl_nm=incmplt_tbl_nm,
                                  conn_func=conn)

    # Chunk df into list of df
    file_lst_df_chnkd = chunk_dataframe(file_lst_df, 100)

    # If no data in dataset, print
    if len(file_lst_df_chnkd) == 0:
        print("No files to scan. Either files have be scanned to recently or no files present in path!")

    # Get total number of indices
    index_ln = len(file_lst_df_chnkd)

    # Start Timer
    start_tm = time.time()

    for index, file_df_chnk in enumerate(file_lst_df_chnkd):
        # Perform Scan and upload to database.
        # This step scans file, uploads data to db and removes data from incomplete table
        scan_chunk(file_df_chnk, index, index_ln, conn, cmplt_tbl_nm, incmplt_tbl_nm)

    # End Timer
    end_tm = time.time()

    # Calculate Elapsed Time
    elpsd_tm = end_tm - start_tm

    # Create Elapsed Time String
    time_str = elapsed_time_stringify(elpsd_tm, False, False)

    # Print End of Scan
    print("Full Scan Complete! Time to complete: {elpsd_tm_str}".format(elpsd_tm_str=time_str))

    # Create view of data
    create_current_view_run_1(conn_func=conn,
                              view_tbl_nm=output_view_nm,
                              cmplt_tbl_func=cmplt_tbl_nm,
                              scn_tm_lmt=tm_bfr_rscn,
                              crnt_tm=time.time())

    # Get output of latest scan
    data_out = query_tbl_run_1(tbl_nm=output_view_nm,
                               conn_func=conn)

    # Print number of files in output dataset
    print("{num_files_str} files analyzed!".format(num_files_str=str(data_out.shape[0])))

    # If csv out, output file
    if not csv_out:
        pass
    else:
        # Export DataFrame for testing
        data_out.to_csv(csv_out, index=False)

    # *****Cleanup old temp tables*****
    # Get list of tables
    db_tbl_lst = list_of_tables(conn)

    # Make holding list for temp table names
    temp_tbl_lst = list()

    # Append temp tables to list
    for i in db_tbl_lst:
        if i[:11] == 'temp_table_':
            temp_tbl_lst.append(i)

    # Drop tables
    for i in temp_tbl_lst:
        drop_query_run_1(conn_func=conn,
                         tbl_nm_delete_func=i)

    # Return True for Success
    return True
