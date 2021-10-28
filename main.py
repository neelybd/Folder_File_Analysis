# Import Statement
import os
import pandas as pd
import sqlite3
from selection import *
from file_handling_docker import *
from functions import *
from datetime import datetime
import time


def sizeof_fmt(num, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


# File Scan location
file_scn_pth = 'H:\\'

# Database location
db_path = 'db.db'

# Delete old database
delete_file(db_path)

# Create db connection
conn = sqlite3.connect(db_path)

# Create Cursor
cur = conn.cursor()

# File List
file_pth_abs_lst = list()
file_nm_lst = list()

# Start Timer
start_tm = time.time()

# Print Start
print("Starting Scanning...")

# Walk through scan path
for (dirpath, dirnames, filenames) in os.walk(file_scn_pth):
    if len(filenames) != 0:
        for i in filenames:
            # Absolute File Path
            file_path_abs = os.path.join(dirpath, i)
            file_pth_abs_lst.append(file_path_abs)

            # File Name
            file_nm = i
            file_nm_lst.append(file_nm)

# End Timer
end_tm = time.time()

# Calculate Elapsed Time
elpsd_tm = end_tm - start_tm

# Create Elapsed Time String
time_str = elapsed_time_stringify(elpsd_tm, False, False)

# Print End of Scan
print("Inital Scan Complete! Time to complete: {elpsd_tm_str}".format(elpsd_tm_str=time_str))

# Put file list into DataFrame
meta_data = pd.DataFrame()

# Append Lists
meta_data['file_path'] = file_pth_abs_lst
meta_data['file_name_ext'] = file_nm_lst

# Get File Name and Extension
meta_data['file_name'] = meta_data['file_name_ext'].apply(lambda x: os.path.splitext(x)[0])
meta_data['ext'] = meta_data['file_name_ext'].apply(lambda x: os.path.splitext(x)[1])

# Get File Directory
meta_data['dir_path'] = meta_data.apply(lambda x: x['file_path'][:-len(x['file_name_ext'])], axis=1)

# Get File Size
meta_data['file_size_bytes'] = meta_data['file_path'].apply(os.path.getsize)
meta_data['file_size'] = meta_data['file_size_bytes'].apply(sizeof_fmt)

# Get Last Access Time
meta_data['last_access_time_unix'] = meta_data['file_path'].apply(lambda x: os.stat(x).st_atime)
meta_data['last_access_time'] = meta_data['last_access_time_unix'].apply(lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'))

# Get Last Modify Time
meta_data['last_modify_time_unix'] = meta_data['file_path'].apply(lambda x: os.stat(x).st_mtime)
meta_data['last_modify_time'] = meta_data['last_access_time_unix'].apply(lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'))

# Get Last Create/Metadata Change Time (Windows/Unix)
meta_data['last_create_time_unix'] = meta_data['file_path'].apply(lambda x: os.stat(x).st_ctime)
meta_data['last_create_time'] = meta_data['last_create_time_unix'].apply(lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'))

# End Timer
end_tm = time.time()

# Calculate Elapsed Time
elpsd_tm = end_tm - start_tm

# Create Elapsed Time String
time_str = elapsed_time_stringify(elpsd_tm, False, False)

# Print End of Scan
print("Full Scan Complete! Time to complete: {elpsd_tm_str}".format(elpsd_tm_str=time_str))

# Export DataFrame for testing
meta_data.to_csv('out.csv', index=False)
