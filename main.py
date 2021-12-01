# Import Statement
from folder_file_analysis_functions import *


print("Program: Folder_File_Analysis")
print("Release: 1.1.0")
print("Date: 2021-11-31")
print("Author: Brian Neely")
print()
print()
print("A program that that analyzes a directory of files and folders its metadata.")
print()
print()


# Time Before rescan
tm_bfr_rscn = 60 * 60 * 24

# File Scan location
file_scn_pth_lst = ['C:\\', 'H:\\']

# Database location
db_path = 'db.db'

# Set file output
csv_out_pth = 'data_out.csv'

# Set multithread
multithread_tf = True

folder_file_analysis(file_scn_pth_lst=file_scn_pth_lst,
                     csv_out=csv_out_pth,
                     multithread=multithread_tf,
                     db_path=db_path)
