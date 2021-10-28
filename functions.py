from collections import OrderedDict
import numpy as np
from dateutil.parser import parse
import math


# print("Function: Functions")
# print("Release: 1.2.0")
# print("Date: 2021-10-28")
# print("Author: Brian Neely")
# print()
# print()
# print("General Functions")
# print()
# print()


def is_date(string, fuzzy=False):
    """
    Created by Alex Riley
    Return whether the string can be interpreted as a date.

    :param string: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    try:
        parse(string, fuzzy=fuzzy)
        return True

    except ValueError:
        return False


def dedupe_list(duplicate_list):
    # *****Dedup list*****
    return list(OrderedDict.fromkeys(duplicate_list))


def split_data(data, num_splits):
    # *****Split data for parallel processing*****
    # Calculate the split locations
    split_locations = np.linspace(0, len(data), num_splits)
    # Rounds up the  split_locations
    split_locations = np.ceil(split_locations)
    # Convert split_locations to int for splitting data
    split_locations = split_locations.astype(int)
    # Split data for parallel processing
    data_split = np.split(data, split_locations)

    return data_split


def list_diff(list_1, list_2):
    # Return different items between lists
    return [i for i in list_1 + list_2 if i not in list_1 or i not in list_2]


def list_common(list_1, list_2):
    # Return common items between lists
    return list(set(list_1).intersection(list_2))


def elapsed_time_stringify(elapsed_time, incld_all=True, incld_0=False):
    # Split off years
    num_years = math.floor(elapsed_time / (365.2425 * 24 * 60 * 60))
    elapsed_time_remainder = elapsed_time % (365.2425 * 24 * 60 * 60)

    # Split off Days
    num_days = math.floor(elapsed_time_remainder / (24 * 60 * 60))
    elapsed_time_remainder = elapsed_time_remainder % (24 * 60 * 60)

    # Split off hours
    num_hours = math.floor(elapsed_time_remainder / (60 * 60))
    elapsed_time_remainder = elapsed_time_remainder % (60 * 60)

    # Split off minutes from seconds
    num_mins = math.floor(elapsed_time_remainder / 60)
    num_sec = round(elapsed_time_remainder % 60, 2)

    # Make time tuple
    time_tuple = (num_years, num_days, num_hours, num_mins, num_sec)

    # Make string tuple
    string_tuple = (' year', ' day', ' hour', ' minute', ' second')
    string_tuple_s = (' years', ' days', ' hours', ' minutes', ' seconds')

    # Initialize variables
    highest_value = False
    string_out = str()

    # Make String
    for index, i in enumerate(time_tuple):
        # If include all
        if incld_all:
            # Add space at end of string
            if len(string_out) > 0:
                string_out = string_out + ' '

            # Append string
            if i == 1:
                string_out = string_out + str(i) + string_tuple[index]
            else:
                string_out = string_out + str(i) + string_tuple_s[index]

            # Continue in loop
            continue

        # Test if highest value is not 0
        if i != 0:
            if not highest_value:
                highest_value = True

        # If highest value
        if highest_value:
            # Include 0
            if incld_0:
                # Add space at end of string
                if len(string_out) > 0:
                    string_out = string_out + ' '

                # Append string
                if i == 1:
                    string_out = string_out + str(i) + string_tuple[index]
                else:
                    string_out = string_out + str(i) + string_tuple_s[index]
            else:
                # Skip loop if 0
                if i == 0:
                    continue
                # Add space at end of string
                if len(string_out) > 0:
                    string_out = string_out + ' '

                # Append string
                if i == 1:
                    string_out = string_out + str(i) + string_tuple[index]
                else:
                    string_out = string_out + str(i) + string_tuple_s[index]

    # Return String
    return string_out
