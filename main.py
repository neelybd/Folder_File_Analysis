# Import Statement
import os
import pandas as pd
import sqlite3
from selection import *
from file_handling_docker import *
from functions import *
from datetime import datetime
from queries import *
import time
# from joblib import Parallel, delayed


print("Program: Folder_File_Analysis")
print("Release: 0.2.0")
print("Date: 2021-11-18")
print("Author: Brian Neely")
print()
print()
print("A program that that analyzes a directory of files and folders its metadata.")
print()
print()

# File Scan location
file_scn_pth_lst = ['H:\\TV Shows']

# Database location
db_path = 'db.db'

# Create db connection
conn = sqlite3.connect(db_path)

# Create Cursor
cur = conn.cursor()

# Table list
incmplt_tbl_nm = 'Incomplete Table'
cmplt_tbl_nm = 'Completed Scan Table'

# Look for incomplete scan in sqlite db by looking for incomplete scan table
try:
    incmplt_fls = conn.execute("""Select * from "{incmplt_tbl_str}";""".format(incmplt_tbl_str=incmplt_tbl_nm)).fetchall()
except sqlite3.OperationalError:
    print('No incomplete table found. Running Program.')
    incmplt_fls = False

# If incomplete files is present, resume scan there
# -----------------

# File List
file_lst = list()
file_dict_lst = list()

# Get Start DateTime
strt_dttm = datetime.today()

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

# Write file list to incomplete file list
file_list_df.to_sql(incmplt_tbl_nm, con=conn, if_exists='append', index=False)

# Dedupe Incomplete Table
dedupe_query_run_1(conn=conn, tbl_nm=incmplt_tbl_nm)

# Print Start of extended test with wait
time.sleep(3)
for i in range(0, 5): print()

##### Get Files
##### Chunk into groups of 100
##### Write results to sqlite db and remove from incomplete list

# Get list of files to scan from db
file_lst_df = incomplete_table_query_run_1(conn, incmplt_tbl_nm)

print('Starting Extended Scan...')

# Chunk df into list of df
file_lst_df_chnkd = chunk_dataframe(file_lst_df, 100)

# Get first index
index = 0

# Get total number of indices
index_ln = len(file_lst_df_chnkd)


def ext_scan_file(file_data_df):
    try:
        # Absolute File Path
        file_path_abs = file_data_df['file_path']

        # File Name
        file_nm = file_data_df['filenames']

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
                          'create_time_unix': [file_lst_crt_tm]}

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


def delete_row_query_run_1(conn, tbl, where_col, value_lst):
    # Convert value_lst to a string for sql
    # Initialize string
    value_lst_sql = str()
    for index, i in enumerate(value_lst):
        i = i.strip()
        if index + 1 != len(value_lst):
            # If a ' exists in a string, replace it with '' and append to result
            value_lst_sql = value_lst_sql + "'" + i.replace("'", "''") + "',"
        else:
            # If last index, don't include trailing ,
            value_lst_sql = value_lst_sql + "'" + i.replace("'", "''") + "'"

    # Replace ' with ''
    value_lst = [v.replace("'", "''") for v in value_lst]

    value_lst_sql = "','".join(value_lst)

    # Delete data in table
    query = """
    delete from "{tbl_str}"
    where
        "{where_col_str}" in ('{value_lst_sql_str}');
    """.format(tbl_str=tbl, where_col_str=where_col, value_lst_sql_str=value_lst_sql)
    conn.execute(query)



    # Convert value_lst to a string for sql

    value_lst = meta_df_lst['file_path'].to_list()

    # Replace ' with ''
    value_lst = [v.replace("'", "''") for v in value_lst]

    value_tuple = tuple(value_lst[:2])

    # Delete data in table
    query = """
    delete from "%s"
    where
        '%s' in {};
    """ % (tbl, where_col)
    conn.execute(query.format(value_tuple))


    value_tuple = tuple(value_lst)

    base_str = """
    delete from "%s"
    where
        '%s' in (
    """ % (tbl, where_col)

    placeholders = ', '.join(['%'] * len(value_lst))  # "%s, %s, %s, ... %s"

    query = 'delete from "{}" WHERE {} IN {}'.format(tbl, where_col, value_tuple)

    cur = conn.cursor()

    cur.execute(query)


def scan_chunk(chunk, index_func, index_ln_func, conn_func, tbl_nm_func, incmplt_tbl_nm_func):
    # Perform extended scan
    print("Scanning index {index_str} out of {index_ln_str}".format(index_str=str(index_func + 1),
                                                                    index_ln_str=str(index_ln_func)))

    # Scan Files and create DataFrame
    chnk_pull_df = pd.concat(chunk.apply(ext_scan_file, axis=1).to_list())

    # Create full meta-data
    chnk_meta_df_func = meta_data_format(chnk_pull_df)

    # Upload data to sqlite database
    chnk_meta_df_func.to_sql(tbl_nm_func, con=conn_func, if_exists='append')

    # Delete rows from incomplete table
    delete_row_query_run_1(conn_func, incmplt_tbl_nm_func, 'file_path', chnk_meta_df_func['file_path'].to_list())

    # Return data
    return chnk_meta_df_func


# Get chunk
file_df_chnk = file_lst_df_chnkd[index]

meta_df_lst = scan_chunk(file_df_chnk, index, index_ln, conn, cmplt_tbl_nm, incmplt_tbl_nm)

delete_row_query_run_1(conn, incmplt_tbl_nm, 'file_path', meta_df_lst['file_path'].to_list())









# Loop through file list and get metadata
for i in file_lst:
    # If the file is missing, temporary, or not present; skip file.
    try:
        # Absolute File Path
        file_path_abs = os.path.join(i['dirpath'], i['filenames'])

        # File Name
        file_nm = i['filenames']

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
        file_info_dict = {'file_path': file_path_abs,
                          'file_name_ext': file_nm,
                          'file_size_bytes': file_sz,
                          'last_access_time_unix': file_lst_accss_tm,
                          'last_modify_time_unix': file_lst_mdfy_tm,
                          'create_time_unix': file_lst_crt_tm}

        # Append Dictionary to file list
        file_dict_lst.append(file_info_dict)
    except FileNotFoundError:
        print("File: '{file_path}' is missing. Skipping...".format(file_path=file_path_abs))

    except OSError:
        print("File: '{file_path}' cannot be accessed. Skipping...".format(file_path=file_path_abs))

# Put file list into DataFrame
meta_data = pd.DataFrame(file_dict_lst)

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

# End Timer
end_tm = time.time()

# Calculate Elapsed Time
elpsd_tm = end_tm - start_tm

# Create Elapsed Time String
time_str = elapsed_time_stringify(elpsd_tm, False, False)

# Print End of Scan
print("Full Scan Complete! Time to complete: {elpsd_tm_str}".format(elpsd_tm_str=time_str))
print("{num_files_str} files analyzed!".format(num_files_str=str(meta_data.shape[0])))

# Export DataFrame for testing
meta_data.to_csv('out.csv', index=False)
