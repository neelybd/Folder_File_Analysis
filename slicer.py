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
import pathlib


# Database location
db_path = 'db.db'

# Filter Table
tbl_in = '"Output View"'

# Create db connection
conn = sqlite3.connect(db_path)

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
            if i[0][-5:] == "lower":
                # Remove all non-numbers in filter_string
                fltr_str = filter(str.isdigit, i[1])
                fltr_str = "".join(fltr_str)

                # If the filter string is empty, pass
                if fltr_str == '':
                    pass
                # Else append filter string list
                else:
                    fltr_str_lst.append("{} >= {}".format(str(i[2]), fltr_str))
            elif i[0][-5:] == "upper":
                # Remove all non-numbers in filter_string
                fltr_str = filter(str.isdigit, i[1])
                fltr_str = "".join(fltr_str)

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
        # Create Empty Variable for Data from Query
        self.data = None

        # Call grid layout constructor
        super(MyGridLayout, self).__init__(**kwargs)

        # Set Columns
        self.cols = 2

        # *****Add Widget: time_since_last_access_fltr_lower*****
        # Add Label
        self.add_widget(Label(text="Minimum Time Since Last Access: "))

        # Add Input Box
        self.time_since_last_access_fltr_lower = TextInput(multiline=False)
        self.add_widget(self.time_since_last_access_fltr_lower)
        # /*****Add Widget: time_since_last_access_fltr_lower*****

        # *****Add Widget: time_since_last_access_fltr_upper*****
        # Add Label
        self.add_widget(Label(text="Maximum Time Since Last Access: "))

        # Add Input Box
        self.time_since_last_access_fltr_upper = TextInput(multiline=False)
        self.add_widget(self.time_since_last_access_fltr_upper)
        # /*****Add Widget: time_since_last_access_fltr_upper*****

        # *****Add Widget: file_age_fltr_lower*****
        # Add Label
        self.add_widget(Label(text="Minimum File Age: "))

        # Add Input Box
        self.file_age_fltr_lower = TextInput(multiline=False)
        self.add_widget(self.file_age_fltr_lower)
        # /*****Add Widget: file_age_fltr_lower*****

        # *****Add Widget: file_age_fltr_upper*****
        # Add Label
        self.add_widget(Label(text="Maximum File Age: "))

        # Add Input Box
        self.file_age_fltr_upper = TextInput(multiline=False)
        self.add_widget(self.file_age_fltr_upper)
        # /*****Add Widget: file_age_fltr_upper*****

        # *****Add Widget: create_time_unix_fltr_lower*****
        # Add Label
        self.add_widget(Label(text="Minimum Create Time: "))

        # Add Input Box
        self.create_time_unix_fltr_lower = TextInput(multiline=False)
        self.add_widget(self.create_time_unix_fltr_lower)
        # /*****Add Widget: create_time_unix_fltr_lower*****

        # *****Add Widget: create_time_unix_fltr_upper*****
        # Add Label
        self.add_widget(Label(text="Maximum Create Time: "))

        # Add Input Box
        self.create_time_unix_fltr_upper = TextInput(multiline=False)
        self.add_widget(self.create_time_unix_fltr_upper)
        # /*****Add Widget: create_time_unix_fltr_upper*****

        # *****Add Widget: last_modify_time_unix_fltr_lower*****
        # Add Label
        self.add_widget(Label(text="Minimum Last Modify Time: "))

        # Add Input Box
        self.last_modify_time_unix_fltr_lower = TextInput(multiline=False)
        self.add_widget(self.last_modify_time_unix_fltr_lower)
        # /*****Add Widget: last_modify_time_unix_fltr_lower*****

        # *****Add Widget: last_modify_time_unix_fltr_upper*****
        # Add Label
        self.add_widget(Label(text="Maximum Last Modify Time: "))

        # Add Input Box
        self.last_modify_time_unix_fltr_upper = TextInput(multiline=False)
        self.add_widget(self.last_modify_time_unix_fltr_upper)
        # /*****Add Widget: last_modify_time_unix_fltr_upper*****

        # *****Add Widget: time_since_last_access_fltr_lower*****
        # Add Label
        self.add_widget(Label(text="Minimum Last Access Time: "))

        # Add Input Box
        self.last_access_time_unix_fltr_lower = TextInput(multiline=False)
        self.add_widget(self.last_access_time_unix_fltr_lower)
        # /*****Add Widget: last_access_time_unix_fltr_lower*****

        # *****Add Widget: last_access_time_unix_fltr_upper*****
        # Add Label
        self.add_widget(Label(text="Maximum Last Access Time: "))

        # Add Input Box
        self.last_access_time_unix_fltr_upper = TextInput(multiline=False)
        self.add_widget(self.last_access_time_unix_fltr_upper)
        # /*****Add Widget: last_access_time_unix_fltr_upper*****

        # *****Create Submit Button*****
        # Create Submit Button
        self.submit = Button(text="Submit", font_size=32)

        # Bind Button
        self.submit.bind(on_press=self.slicer_press)
        self.add_widget(self.submit)
        # /*****Create Submit Button*****

        # *****Create Download Button*****
        # Create Download Button
        self.submit = Button(text="Download CSV", font_size=32)

        # Bind Button
        self.submit.bind(on_press=self.download_press)
        self.add_widget(self.submit)
        # /*****Create Download Button*****

    # Define Submit Slicer Button
    def slicer_press(self, instance):
        time_since_last_access_fltr_lower = self.time_since_last_access_fltr_lower.text
        time_since_last_access_fltr_upper = self.time_since_last_access_fltr_upper.text
        file_age_fltr_lower = self.file_age_fltr_lower.text
        file_age_fltr_upper = self.file_age_fltr_upper.text
        create_time_unix_fltr_lower = self.create_time_unix_fltr_lower.text
        create_time_unix_fltr_upper = self.create_time_unix_fltr_upper.text
        last_modify_time_unix_fltr_lower = self.last_modify_time_unix_fltr_lower.text
        last_modify_time_unix_fltr_upper = self.last_modify_time_unix_fltr_upper.text
        last_access_time_unix_fltr_lower = self.last_access_time_unix_fltr_lower.text
        last_access_time_unix_fltr_upper = self.last_access_time_unix_fltr_upper.text

        filter_tpl_lst = [
            ("time_since_last_access_fltr_lower", time_since_last_access_fltr_lower, "time_since_last_access"),
            ("time_since_last_access_fltr_upper", time_since_last_access_fltr_upper, "time_since_last_access"),
            ("file_age_fltr_lower", file_age_fltr_lower, "file_age"),
            ("file_age_fltr_upper", file_age_fltr_upper, "file_age"),
            ("create_time_unix_fltr_lower", create_time_unix_fltr_lower, "create_time_unix"),
            ("create_time_unix_fltr_upper", create_time_unix_fltr_upper, "create_time_unix"),
            ("last_modify_time_unix_fltr_lower", last_modify_time_unix_fltr_lower, "last_modify_time_unix"),
            ("last_modify_time_unix_fltr_upper", last_modify_time_unix_fltr_upper, "last_modify_time_unix"),
            ("last_access_time_unix_fltr_lower", last_access_time_unix_fltr_lower, "last_access_time_unix"),
            ("last_access_time_unix_fltr_upper", last_access_time_unix_fltr_upper, "last_access_time_unix")
        ]

        # Create Sql Query
        query = create_sql_query(filter_tpl_lst=filter_tpl_lst, tbl_in=tbl_in)

        # Print Sql Statement
        # self.add_widget(Label(text=f'{query}'))

        # Query Data
        self.data = pd.read_sql_query(query, con=conn)

    # Define Download Button
    def download_press(self, instance):
        # Get Download location
        file_out_loc = select_file_out_csv(file_in=pathlib.Path().resolve())

        # If no data exists within the DataFrame, Get full dataset
        if self.data is None:
            query = create_sql_query(filter_tpl_lst=None, tbl_in=tbl_in)
            self.data = pd.read_sql_query(query, con=conn)

        # Download Data
        self.data.to_csv(file_out_loc, index=False)


class MyApp(App):
    def build(self):
        return MyGridLayout()


if __name__ == '__main__':
    MyApp().run()
