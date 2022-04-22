import os
import pandas as pd
import sqlite3
import pandas.io.sql
from file_handling import *
from queries import *
import kivy
from kivy.app import App
from kivy.uix.label import Label
from kivy.uix.gridlayout import GridLayout
from kivy.uix.textinput import TextInput
from kivy.uix.button import Button
from kivy.uix.dropdown import DropDown
from kivy.uix.widget import Widget
from kivy.base import runTouchApp
from kivy.properties import ObjectProperty
import pathlib
from tkinter import filedialog as tkFileDialog
from folder_file_analysis_functions import *


# Column List
# time_since_last_access
# file_age
# create_time_unix
# last_modify_time_unix
# last_access_time_unix

print("Program: Folder_File_Analysis - Slicer")
print("Release: 1.5.1")
print("Date: 2022-04-22")
print("Author: Brian Neely")
print()
print()
print("A program that that analyzes a directory of files and folders its metadata.")
print()
print()


def create_sql_query(filter_tpl_lst, tbl_in):
    # Make Sql Query Base
    query = """select * from {}""".format(str(tbl_in))

    # If filter_tpl_lst is None, Pass full Data Query
    if not filter_tpl_lst:
        return query

    # Filter String List
    fltr_str_lst = list()

    # Make Filter String List
    for i in filter_tpl_lst:
        if not i[1]:
            pass
        else:
            if i[0][-8:] == "lower_ts":
                # Remove all non-numbers in filter_string
                fltr_str = filter(str.isdigit, i[1])
                fltr_str = "".join(fltr_str)

                # Convert to float then divide by timestamp selector
                fltr_num = float(fltr_str) * i[3]

                # Convert back to string
                fltr_str = str(fltr_num)

                # If the filter string is empty, pass
                if fltr_str == '':
                    pass
                # Else append filter string list
                else:
                    fltr_str_lst.append("{} >= {}".format(str(i[2]), fltr_str))
            elif i[0][-8:] == "upper_ts":
                # Remove all non-numbers in filter_string
                fltr_str = filter(str.isdigit, i[1])
                fltr_str = "".join(fltr_str)

                # Convert to float then divide by timestamp selector
                fltr_num = float(fltr_str) * i[3]

                # Convert back to string
                fltr_str = str(fltr_num)

                # If the filter string is empty, pass
                if fltr_str == '':
                    pass
                # Else append filter string list
                else:
                    fltr_str_lst.append("{} <= {}".format(str(i[2]), fltr_str))
            elif i[0][-6:] == "string":
                fltr_str_lst.append("{} <= '{}'".format(str(i[2]), str(i[1])))
            else:
                # Remove all non-numbers in filter_string
                fltr_str = filter(str.isdigit, i[1])
                fltr_str = "".join(fltr_str)

                # If the filter string is empty, pass
                if fltr_str == '':
                    pass
                # Else append filter string list
                else:
                    fltr_str_lst.append("{} = {}".format(str(i[2]), fltr_str))

    # Make Filter String
    fltr_str = " and ".join(fltr_str_lst)

    # If Filter Exists, add where to query
    if fltr_str == '':
        query = query + ';'
    else:
        query = query + " where " + fltr_str + ';'

    return query


class CustomDropDown(DropDown):
    for i in range(5):
        print(i)


class MyGridLayout(Widget):

    # Initialize
    def __init__(self, *args, **kwargs):
        super(MyGridLayout, self).__init__(*args, **kwargs)

        # Select database path
        self.db_path = 'db.db'

        # Filter Table
        self.tbl_in = '"Output View"'

        # Create db connection
        self.conn = sqlite3.connect(self.db_path)

        # Set multithread
        self.multithread_tf = True

        # Set file output
        self.csv_out_pth = 'data_out.csv'

        # Create Empty Variable for Data from Query
        self.data = None

        # Set Column Height
        self.col_height = 50

        # Create list of date selectors
        self.date_selector_lst = [("Seconds", 1),
                                  ("Minutes", 60),
                                  ("Hours", 60*60),
                                  ("Days", 60*60*24),
                                  ("Months", 60*60*24*365.25/12),
                                  ("Years", 60*60*24*365.25)]

        # Create initial date selector
        self.date_selector_default = self.date_selector_lst[5][0]

        # If no database table exists, open rescan
        try:
            query = 'select * from {} limit 1;'.format(self.tbl_in)
            pd.read_sql_query(query, con=self.conn)
        except pandas.io.sql.DatabaseError:
            print("No Database data exists... Opening Rescan...")
            self.rescan_press()

        # Initialize ts selector spinner variables
        self.time_since_last_access_fltr_lower_selector_str = self.date_selector_default
        self.time_since_last_access_fltr_upper_selector_str = self.date_selector_default
        self.file_age_fltr_lower_selector_str = self.date_selector_default
        self.file_age_fltr_upper_selector_str = self.date_selector_default
        self.last_modify_time_unix_fltr_lower_selector_str = self.date_selector_default
        self.last_modify_time_unix_fltr_upper_selector_str = self.date_selector_default

    def spinner_time_type_clicked(self, ts_type, fltr_varible):
        print(ts_type)
        print(fltr_varible)
        # *****Save Data from spinner to appropriate variable*****
        if fltr_varible == "time_since_last_access_fltr_lower_selector":
            self.time_since_last_access_fltr_lower_selector_str = ts_type
        if fltr_varible == "time_since_last_access_fltr_upper_selector":
            self.time_since_last_access_fltr_upper_selector_str = ts_type
        if fltr_varible == "file_age_fltr_lower_selector":
            self.file_age_fltr_lower_selector_str = ts_type
        if fltr_varible == "file_age_fltr_upper_selector":
            self.file_age_fltr_upper_selector_str = ts_type
        if fltr_varible == "last_modify_time_unix_fltr_lower_selector":
            self.last_modify_time_unix_fltr_lower_selector_str = ts_type
        if fltr_varible == "last_modify_time_unix_fltr_upper_selector":
            self.last_modify_time_unix_fltr_upper_selector_str = ts_type
        # /*****Save Data from spinner to appropriate variable*****

    # Make rescan function
    def rescan_press(self):
        root = Tk()
        root.withdraw()

        # Select Folder Location
        rescan_loc = tkFileDialog.askdirectory()
        print("Rescan Location: " + rescan_loc)

        # If rescan location is blank, end
        if not rescan_loc:
            print("No rescan location...")
            return False

        file_scn_pth_lst = [rescan_loc]

        # Run folder file analysis for rescan
        folder_file_analysis(file_scn_pth_lst=file_scn_pth_lst,
                             csv_out=False,
                             multithread=self.multithread_tf,
                             db_path=self.db_path)

    def lookup_time_selector_multiplier(self, selector_text_in):
        # Lookup timestamp multiplier
        for i in self.date_selector_lst:
            if i[0] == selector_text_in:
                return i[1]

        # If no match, return 1
        return 1

    # Define Submit Slicer Button
    def slicer_press(self):
        # Text Box Inputs
        time_since_last_access_fltr_lower = self.time_since_last_access_fltr_lower.text
        time_since_last_access_fltr_upper = self.time_since_last_access_fltr_upper.text
        file_age_fltr_lower = self.file_age_fltr_lower.text
        file_age_fltr_upper = self.file_age_fltr_upper.text
        last_modify_time_unix_fltr_lower = self.last_modify_time_unix_fltr_lower.text
        last_modify_time_unix_fltr_upper = self.last_modify_time_unix_fltr_upper.text

        # Make list of tuples that has all of the filter information for each filtered item.
        # Variable name, Value, Column Name, Timestamp modifier
        filter_tpl_lst = [
            ("time_since_last_access_fltr_lower_ts", time_since_last_access_fltr_lower, '"time_since_last_access"', self.lookup_time_selector_multiplier(selector_text_in=self.time_since_last_access_fltr_lower_selector_str)),
            ("time_since_last_access_fltr_upper_ts", time_since_last_access_fltr_upper, '"time_since_last_access"', self.lookup_time_selector_multiplier(selector_text_in=self.time_since_last_access_fltr_upper_selector_str)),
            ("file_age_fltr_lower_ts", file_age_fltr_lower, '"file_age"', self.lookup_time_selector_multiplier(selector_text_in=self.file_age_fltr_lower_selector_str)),
            ("file_age_fltr_upper_ts", file_age_fltr_upper, '"file_age"', self.lookup_time_selector_multiplier(selector_text_in=self.file_age_fltr_upper_selector_str)),
            ("time_since_last_modify_time_unix_fltr_lower_ts", last_modify_time_unix_fltr_lower, '"time_since_last_modify"', self.lookup_time_selector_multiplier(selector_text_in=self.last_modify_time_unix_fltr_lower_selector_str)),
            ("time_since_last_modify_time_unix_fltr_upper_ts", last_modify_time_unix_fltr_upper, '"time_since_last_modify"', self.lookup_time_selector_multiplier(selector_text_in=self.last_modify_time_unix_fltr_upper_selector_str))
        ]

        # Create Sql Query
        query = create_sql_query(filter_tpl_lst=filter_tpl_lst, tbl_in=self.tbl_in)

        # Print Sql Statement
        print(query)
        # self.add_widget(Label(text=f'{query}'))

        # Query Data
        self.data = pd.read_sql_query(query, con=self.conn)

        # Print Head
        print(self.data.head(100))

    # Define Download Button
    def download_press(self):
        # Get Download location
        file_out_loc = select_file_out_csv(file_in=pathlib.Path().resolve())

        # If no data exists within the DataFrame, Get full dataset
        if self.data is None:
            query = create_sql_query(filter_tpl_lst=None, tbl_in=self.tbl_in)
            self.data = pd.read_sql_query(query, con=self.conn)

        # Download Data
        self.data.to_csv(file_out_loc, index=False)


class MyApp(App):
    def build(self):
        return MyGridLayout()


if __name__ == '__main__':
    MyApp().run()
