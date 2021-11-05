# Import Statement
import os
import pandas as pd
import sqlite3
from selection import *
from file_handling_docker import *
from functions import *
from datetime import datetime
import time


print("Program: Folder_File_Analysis")
print("Release: 0.1.1")
print("Date: 2021-11-05")
print("Author: Brian Neely")
print()
print()
print("A program that that analyzes a directory of files and folders its metadata.")
print()
print()


# File Scan location
file_scn_pth_lst = ['H:\\', '\\\\Neelybd_server\\u']

# Database location
db_path = 'db.db'

# Delete old database
delete_file(db_path)

# Create db connection
conn = sqlite3.connect(db_path)

# Create Cursor
cur = conn.cursor()

# File List
file_lst = list()
file_dict_lst = list()

# Start Timer
start_tm = time.time()

# Print Start
print("Starting Scanning...")

# Walk through scan path and get list of files
for file_scn_pth in file_scn_pth_lst:
    for (dirpath, dirnames, filenames) in os.walk(file_scn_pth):
        if len(filenames) != 0:
            for i in filenames:
                file_lst.append({'dirpath': dirpath, 'dirnames': dirnames, 'filenames': i})

# End Timer
end_tm = time.time()

# Calculate Elapsed Time
elpsd_tm = end_tm - start_tm

# Create Elapsed Time String
time_str = elapsed_time_stringify(elpsd_tm, False, False)

# Print End of Scan
print("Initial Scan Complete! Time to complete: {elpsd_tm_str}".format(elpsd_tm_str=time_str))

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
