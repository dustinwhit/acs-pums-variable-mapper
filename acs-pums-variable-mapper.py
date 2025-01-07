## American Community Survey (ACS) Public Use Microdata Sample (PUMS) Variable Mapper
# Last update: 2025-01-03


"""About: Import U.S. Census Bureau American Community Survey (ACS) Public Use Microdata Sample (PUMS) data and map variables/columns values using the official ACS PUMS Data Dictionary. Tested for ACS 2023 1-Year PUMS."""


###############
# Initial Setup
###############

# Erase all declared global variables
globals().clear()


# Import packages
from io import BytesIO, StringIO
import re
from zipfile import ZipFile, ZIP_DEFLATED

import pandas as pd
import requests


# Settings

## Copy-on-Write (will be enabled by default in version 3.0)
if pd.__version__ >= '1.5.0' and pd.__version__ < '3.0.0':
    pd.options.mode.copy_on_write = True


###########
# Functions
###########


def zipfile_download(*, url, directory):
    with ZipFile(file=BytesIO(initial_bytes=requests.get(url=url, headers=None, timeout=5, verify=True).content), mode='r', compression=ZIP_DEFLATED) as zip_file:
        zip_file.extractall(path=directory)


def acs_pums_variable_mapper(*, df, acs_pums_data_dictionary_path=None, acs_pums_data_dictionary_url=None, survey_level='Person-Level', skip_variables=[]):
    if acs_pums_data_dictionary_path is None and acs_pums_data_dictionary_url is None:
        raise ValueError('Either "acs_pums_data_dictionary_path" or "acs_pums_data_dictionary_url" needs to be defined.')

    # Create a copy of the original DataFrame to avoid modifying it
    df = df.copy()

    if acs_pums_data_dictionary_path is not None:
        with open(file=acs_pums_data_dictionary_path, encoding='utf-8') as file:
            file_content = file.readlines()

    if acs_pums_data_dictionary_url is not None:
        file_content = StringIO(requests.get(url=acs_pums_data_dictionary_url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5, verify=True).content.decode('utf-8')).readlines()

    # Initialize lines to select based on the survey_level
    lines = []

    # Use a single pass to find the relevant section of the file
    if survey_level == 'Housing-Level':
        start_index = None
        end_index = None
        for i, line in enumerate(file_content):
            if 'HOUSING RECORD-BASIC VARIABLES' in line:
                start_index = i
            elif 'PERSON RECORD-BASIC VARIABLES' in line and start_index is not None:
                end_index = i
                break
        if start_index is not None and end_index is not None:
            lines = file_content[start_index:end_index]

    elif survey_level == 'Person-Level':
        for i, line in enumerate(file_content):
            if 'PERSON RECORD-BASIC VARIABLES' in line:
                lines = file_content[i:]
                break

    # Function to extract mappings for a specific column
    def mappings_extract(*, column_name, lines):
        mappings = {}
        column_found = False
        for line in lines:
            # Check if the column name is found
            if line.startswith(column_name):
                column_found = True
                continue
            # If we are in the relevant section, look for mappings
            if column_found:
                # Update the regular expression to capture the slash in state mappings
                match = re.match(r'(\d+)\s+\.(.+)', line.strip())
                if match:
                    key, value = match.groups()
                    mappings[int(key)] = value.strip()
                # Stop if we reach an empty line or a new column section
                elif line.strip() == '' or re.match(r'[A-Z]+\s+', line.strip()):
                    break
        return mappings

    # Automatically apply mappings to all uppercase columns
    for column in df.columns:
        if column.isupper() and column not in skip_variables:  # Skip columns in skip_variables
            if column.isupper():  # Check if the column name is uppercase
                mappings = mappings_extract(column_name=column, lines=lines)
                if mappings:  # Only map if mappings are found
                    df[column] = df[column].map(mappings)

    # Return objects
    return df
