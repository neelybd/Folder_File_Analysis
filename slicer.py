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
from kivy.base import runTouchApp
import pathlib
from tkinter import filedialog as tkFileDialog
from folder_file_analysis_functions import *


# Column List
# time_since_last_access
# file_age
# create_time_unix
# last_modify_time_unix
# last_access_time_unix


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


class MyGridLayout(GridLayout):
    # Initialize
    def __init__(self, **kwargs):
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
        self.date_selector_lst = [("Second", 1),
                                  ("Minute", 60),
                                  ("Hour", 60*60),
                                  ("Day", 60*60*24),
                                  ("Month", 60*60*24*365.25/12),
                                  ("Year", 60*60*24*365.25)]

        # Create initial date selector
        self.date_selector_default = self.date_selector_lst[5][0]

        # If no database table exists, open rescan
        try:
            query = 'select * from {} limit 1;'.format(self.tbl_in)
            pd.read_sql_query(query, con=self.conn)
        except pandas.io.sql.DatabaseError:
            print("No Database data exists... Opening Rescan...")
            self.rescan_press(self)

        # Call grid layout constructor
        super(MyGridLayout, self).__init__(**kwargs)

        # Set Columns
        self.cols = 3

        # *****Add Widget: time_since_last_access_fltr_lower*****
        # Add Label
        self.add_widget(Label(text="Minimum Time Since Last Access: ", size_hint=(None, None), height=self.col_height))

        # Add Input Box
        self.time_since_last_access_fltr_lower = TextInput(multiline=False, size_hint=(None, None), height=self.col_height)
        self.add_widget(self.time_since_last_access_fltr_lower)

        # *****Time Selector*****
        # Create DropDown Type
        self.time_since_last_access_fltr_lower_selector_dd = DropDown()

        # Loop through the date selector list
        for i in self.date_selector_lst:
            # Adding button in drop down list
            btn = Button(text=i[0], size_hint_y=None, height=self.col_height)

            # binding the button to show the text when selected
            btn.bind(on_release=lambda btn: self.time_since_last_access_fltr_lower_selector_dd.select(btn.text))

            # then add the button inside the dropdown
            self.time_since_last_access_fltr_lower_selector_dd.add_widget(btn)
        self.time_since_last_access_fltr_lower_selector_main = Button(text=self.date_selector_default, size_hint=(None, None), height=50, pos_hint=(.33, .25))
        self.time_since_last_access_fltr_lower_selector_main.bind(on_release=self.time_since_last_access_fltr_lower_selector_dd.open)
        self.time_since_last_access_fltr_lower_selector_dd.bind(on_select=lambda instance, x: setattr(self.time_since_last_access_fltr_lower_selector_main, 'text', x))
        self.add_widget(self.time_since_last_access_fltr_lower_selector_main)
        # /*****Time Selector*****
        # /*****Add Widget: time_since_last_access_fltr_lower*****

        # *****Add Widget: time_since_last_access_fltr_upper*****
        # Add Label
        self.add_widget(Label(text="Maximum Time Since Last Access: ", size_hint=(None, None), height=self.col_height))

        # Add Input Box
        self.time_since_last_access_fltr_upper = TextInput(multiline=False, size_hint=(None, None), height=self.col_height)
        self.add_widget(self.time_since_last_access_fltr_upper)

        # *****Time Selector*****
        # Create DropDown Type
        self.time_since_last_access_fltr_upper_dd = DropDown()

        # Loop through the date selector list
        for i in self.date_selector_lst:
            # Adding button in drop down list
            btn = Button(text=i[0], size_hint_y=None, height=self.col_height)

            # binding the button to show the text when selected
            btn.bind(on_release=lambda btn: self.time_since_last_access_fltr_upper_dd.select(btn.text))

            # then add the button inside the dropdown
            self.time_since_last_access_fltr_upper_dd.add_widget(btn)
        self.time_since_last_access_fltr_upper_selector_main = Button(text=self.date_selector_default, size_hint=(None, None), height=50, pos_hint=(.33, .25))
        self.time_since_last_access_fltr_upper_selector_main.bind(on_release=self.time_since_last_access_fltr_upper_dd.open)
        self.time_since_last_access_fltr_upper_dd.bind(on_select=lambda instance, x: setattr(self.time_since_last_access_fltr_upper_selector_main, 'text', x))
        self.add_widget(self.time_since_last_access_fltr_upper_selector_main)
        # /*****Time Selector*****
        # /*****Add Widget: time_since_last_access_fltr_upper*****

        # *****Add Widget: file_age_fltr_lower*****
        # Add Label
        self.add_widget(Label(text="Minimum File Age: ", size_hint=(None, None), height=self.col_height))

        # Add Input Box
        self.file_age_fltr_lower = TextInput(multiline=False, size_hint=(None, None), height=self.col_height)
        self.add_widget(self.file_age_fltr_lower)

        # *****Time Selector*****
        # Create DropDown Type
        self.file_age_fltr_lower_dd = DropDown()

        # Loop through the date selector list
        for i in self.date_selector_lst:
            # Adding button in drop down list
            btn = Button(text=i[0], size_hint_y=None, height=self.col_height)

            # binding the button to show the text when selected
            btn.bind(on_release=lambda btn: self.file_age_fltr_lower_dd.select(btn.text))

            # then add the button inside the dropdown
            self.file_age_fltr_lower_dd.add_widget(btn)
        self.file_age_fltr_lower_selector_main = Button(text=self.date_selector_default, size_hint=(None, None), height=50, pos_hint=(.33, .25))
        self.file_age_fltr_lower_selector_main.bind(on_release=self.file_age_fltr_lower_dd.open)
        self.file_age_fltr_lower_dd.bind(on_select=lambda instance, x: setattr(self.file_age_fltr_lower_selector_main, 'text', x))
        self.add_widget(self.file_age_fltr_lower_selector_main)
        # /*****Time Selector*****
        # /*****Add Widget: file_age_fltr_lower*****

        # *****Add Widget: file_age_fltr_upper*****
        # Add Label
        self.add_widget(Label(text="Maximum File Age: ", size_hint=(None, None), height=self.col_height))

        # Add Input Box
        self.file_age_fltr_upper = TextInput(multiline=False, size_hint=(None, None), height=self.col_height)
        self.add_widget(self.file_age_fltr_upper)

        # *****Time Selector*****
        # Create DropDown Type
        self.file_age_fltr_upper_dd = DropDown()

        # Loop through the date selector list
        for i in self.date_selector_lst:
            # Adding button in drop down list
            btn = Button(text=i[0], size_hint_y=None, height=self.col_height)

            # binding the button to show the text when selected
            btn.bind(on_release=lambda btn: self.file_age_fltr_upper_dd.select(btn.text))

            # then add the button inside the dropdown
            self.file_age_fltr_upper_dd.add_widget(btn)
        self.file_age_fltr_upper_selector_main = Button(text=self.date_selector_default, size_hint=(None, None), height=50, pos_hint=(.33, .25))
        self.file_age_fltr_upper_selector_main.bind(on_release=self.file_age_fltr_upper_dd.open)
        self.file_age_fltr_upper_dd.bind(on_select=lambda instance, x: setattr(self.file_age_fltr_upper_selector_main, 'text', x))
        self.add_widget(self.file_age_fltr_upper_selector_main)
        # /*****Time Selector*****
        # /*****Add Widget: file_age_fltr_upper*****

        # *****Add Widget: last_modify_time_unix_fltr_lower*****
        # Add Label
        self.add_widget(Label(text="Minimum Time Since Last Modify: ", size_hint=(None, None), height=self.col_height))

        # Add Input Box
        self.last_modify_time_unix_fltr_lower = TextInput(multiline=False, size_hint=(None, None), height=self.col_height)
        self.add_widget(self.last_modify_time_unix_fltr_lower)

        # *****Time Selector*****
        # Create DropDown Type
        self.last_modify_time_unix_fltr_lower_dd = DropDown()

        # Loop through the date selector list
        for i in self.date_selector_lst:
            # Adding button in drop down list
            btn = Button(text=i[0], size_hint_y=None, height=self.col_height)

            # binding the button to show the text when selected
            btn.bind(on_release=lambda btn: self.last_modify_time_unix_fltr_lower_dd.select(btn.text))

            # then add the button inside the dropdown
            self.last_modify_time_unix_fltr_lower_dd.add_widget(btn)
        self.last_modify_time_unix_fltr_lower_selector_main = Button(text=self.date_selector_default, size_hint=(None, None), height=50, pos_hint=(.33, .25))
        self.last_modify_time_unix_fltr_lower_selector_main.bind(on_release=self.last_modify_time_unix_fltr_lower_dd.open)
        self.last_modify_time_unix_fltr_lower_dd.bind(on_select=lambda instance, x: setattr(self.last_modify_time_unix_fltr_lower_selector_main, 'text', x))
        self.add_widget(self.last_modify_time_unix_fltr_lower_selector_main)
        # /*****Time Selector*****
        # /*****Add Widget: last_modify_time_unix_fltr_lower*****

        # *****Add Widget: last_modify_time_unix_fltr_upper*****
        # Add Label
        self.add_widget(Label(text="Maximum Time Since Last Modify: ", size_hint=(None, None), height=self.col_height))

        # Add Input Box
        self.last_modify_time_unix_fltr_upper = TextInput(multiline=False, size_hint=(None, None), height=self.col_height)
        self.add_widget(self.last_modify_time_unix_fltr_upper)

        # *****Time Selector*****
        # Create DropDown Type
        self.last_modify_time_unix_fltr_upper_dd = DropDown()

        # Loop through the date selector list
        for i in self.date_selector_lst:
            # Adding button in drop down list
            btn = Button(text=i[0], size_hint_y=None, height=self.col_height)

            # binding the button to show the text when selected
            btn.bind(on_release=lambda btn: self.last_modify_time_unix_fltr_upper_dd.select(btn.text))

            # then add the button inside the dropdown
            self.last_modify_time_unix_fltr_upper_dd.add_widget(btn)
        self.last_modify_time_unix_fltr_upper_selector_main = Button(text=self.date_selector_default, size_hint=(None, None), height=50, pos_hint=(.33, .25))
        self.last_modify_time_unix_fltr_upper_selector_main.bind(on_release=self.last_modify_time_unix_fltr_upper_dd.open)
        self.last_modify_time_unix_fltr_upper_dd.bind(on_select=lambda instance, x: setattr(self.last_modify_time_unix_fltr_upper_selector_main, 'text', x))
        self.add_widget(self.last_modify_time_unix_fltr_upper_selector_main)
        # /*****Time Selector*****
        # /*****Add Widget: last_modify_time_unix_fltr_upper*****

        # *****Create Rescan Button*****
        # Create Submit Button
        self.submit = Button(text="Rescan", font_size=32, size_hint=(None, None), height=self.col_height)

        # Bind Button
        self.submit.bind(on_release=self.rescan_press)
        self.add_widget(self.submit)
        # /*****Create Rescan Button*****

        # *****Create Submit Button*****
        # Create Submit Button
        self.submit = Button(text="Submit", font_size=32, size_hint=(None, None), height=self.col_height)

        # Bind Button
        self.submit.bind(on_release=self.slicer_press)
        self.add_widget(self.submit)
        # /*****Create Submit Button*****

        # *****Create Download Button*****
        # Create Download Button
        self.submit = Button(text="Download CSV", font_size=32, size_hint=(None, None), height=self.col_height)

        # Bind Button
        self.submit.bind(on_press=self.download_press)
        self.add_widget(self.submit)
        # /*****Create Download Button*****

    # Make rescan function
    def rescan_press(self, instance):
        root = Tk()
        root.withdraw()

        # Select Folder Location
        rescan_loc = tkFileDialog.askdirectory()
        file_scn_pth_lst = [rescan_loc]
        print(rescan_loc)

        # Run folder file analysis for rescan
        folder_file_analysis(file_scn_pth_lst=file_scn_pth_lst,
                             csv_out=False,
                             multithread=self.multithread_tf,
                             db_path=self.db_path)

    # Make Dropdown Selector for Dates
    def drop_down_time_selector(self):
        # Create DropDown Type
        dropdown = DropDown()

        # Loop through the date selector list
        for i in self.date_selector_lst:
            # Adding button in drop down list
            btn = Button(text=i[0], size_hint_y=None, height=40)

            # binding the button to show the text when selected
            btn.bind(on_release=lambda btn: dropdown.select(btn.text))

            # then add the button inside the dropdown
            dropdown.add_widget(btn)
        mainbutton = Button(text='Select Type', size_hint=(None, None), pos=(350, 300))
        mainbutton.bind(on_release=dropdown.open)
        dropdown.bind(on_select=lambda instance, x: setattr(mainbutton, 'text', x))

        return mainbutton

    def lookup_time_selector_multiplier(self, selector_text_in):
        # Lookup timestamp multiplier
        for i in self.date_selector_lst:
            if i[0] == selector_text_in:
                return i[1]

        # If no match, return 1
        return 1

    # Define Submit Slicer Button
    def slicer_press(self, instance):
        # Text Box Inputs
        time_since_last_access_fltr_lower = self.time_since_last_access_fltr_lower.text
        time_since_last_access_fltr_upper = self.time_since_last_access_fltr_upper.text
        file_age_fltr_lower = self.file_age_fltr_lower.text
        file_age_fltr_upper = self.file_age_fltr_upper.text
        last_modify_time_unix_fltr_lower = self.last_modify_time_unix_fltr_lower.text
        last_modify_time_unix_fltr_upper = self.last_modify_time_unix_fltr_upper.text

        # Drop Down Selector
        time_since_last_access_fltr_lower_selector_str = self.time_since_last_access_fltr_lower_selector_main.text
        time_since_last_access_fltr_upper_selector_str = self.time_since_last_access_fltr_upper_selector_main.text
        file_age_fltr_lower_selector_str = self.file_age_fltr_lower_selector_main.text
        file_age_fltr_upper_selector_str = self.file_age_fltr_upper_selector_main.text
        last_modify_time_unix_fltr_lower_selector_str = self.last_modify_time_unix_fltr_lower_selector_main.text
        last_modify_time_unix_fltr_upper_selector_str = self.last_modify_time_unix_fltr_upper_selector_main.text

        filter_tpl_lst = [
            ("time_since_last_access_fltr_lower_ts", time_since_last_access_fltr_lower, '"time_since_last_access"', self.lookup_time_selector_multiplier(selector_text_in=time_since_last_access_fltr_lower_selector_str)),
            ("time_since_last_access_fltr_upper_ts", time_since_last_access_fltr_upper, '"time_since_last_access"', self.lookup_time_selector_multiplier(selector_text_in=time_since_last_access_fltr_upper_selector_str)),
            ("file_age_fltr_lower_ts", file_age_fltr_lower, '"file_age"', self.lookup_time_selector_multiplier(selector_text_in=file_age_fltr_lower_selector_str)),
            ("file_age_fltr_upper_ts", file_age_fltr_upper, '"file_age"', self.lookup_time_selector_multiplier(selector_text_in=file_age_fltr_upper_selector_str)),
            ("time_since_last_modify_time_unix_fltr_lower_ts", last_modify_time_unix_fltr_lower, '"time_since_last_modify"', self.lookup_time_selector_multiplier(selector_text_in=last_modify_time_unix_fltr_lower_selector_str)),
            ("time_since_last_modify_time_unix_fltr_upper_ts", last_modify_time_unix_fltr_upper, '"time_since_last_modify"', self.lookup_time_selector_multiplier(selector_text_in=last_modify_time_unix_fltr_upper_selector_str))
        ]

        # print(self.lookup_time_selector_multiplier(selector_text_in=time_since_last_access_fltr_lower_selector_str))

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
    def download_press(self, instance):
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
