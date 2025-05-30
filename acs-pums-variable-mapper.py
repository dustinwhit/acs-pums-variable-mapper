## American Community Survey (ACS) Public Use Microdata Sample (PUMS) Variable Mapper
# Last update: 2025-01-03


"""About: Import U.S. Census Bureau American Community Survey (ACS) Public Use Microdata Sample (PUMS) data and map variables/columns values using the official ACS PUMS data dictionary."""


###############
# Initial Setup
###############


# Import packages
from io import BytesIO, StringIO
import re
from zipfile import ZipFile, ZIP_DEFLATED
import pandas as pd
import requests

# Erase all declared global variables
globals().clear()


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


def get_acs_pums_data_dictionary(*, year, table_group='1-Year'):
    """Fetch the ACS PUMS data dictionary for a specific year and table group."""

    # Construct URL based on year and table group
    url = (
        'https://www2.census.gov/programs-surveys/acs/tech_docs/pums/data_dict/'
        f'PUMS_Data_Dictionary_{year}.txt'
    )
    response = requests.get(url=url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5, verify=True)
    response.raise_for_status()
    return StringIO(response.content.decode('utf-8')).readlines()


def acs_pums_variable_mapper(
    *,
    df,
    acs_pums_data_dictionary_path=None,
    acs_pums_data_dictionary_url=None,
    acs_year=None,
    table_group='1-Year',
    survey_level='Person-Level',
    skip_variables=[],
):
    if (
        acs_pums_data_dictionary_path is None
        and acs_pums_data_dictionary_url is None
        and acs_year is None
    ):
        raise ValueError(
            'Either "acs_pums_data_dictionary_path", "acs_pums_data_dictionary_url", '
            'or "acs_year" needs to be defined.'
        )

    # Create a copy of the original DataFrame to avoid modifying it
    df = df.copy()

    if acs_pums_data_dictionary_path is not None:
        with open(file=acs_pums_data_dictionary_path, encoding='utf-8') as file:
            file_content = file.readlines()
    elif acs_pums_data_dictionary_url is not None:
        file_content = StringIO(
            requests.get(
                url=acs_pums_data_dictionary_url,
                headers={'User-Agent': 'Mozilla/5.0'},
                timeout=5,
                verify=True,
            ).content.decode('utf-8')
        ).readlines()
    elif acs_year is not None:
        file_content = get_acs_pums_data_dictionary(year=acs_year, table_group=table_group)

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


def bulk_process_pums_datasets(
    *,
    datasets,
    acs_pums_data_dictionary_path=None,
    acs_pums_data_dictionary_url=None,
    acs_year=None,
    table_group='1-Year',
    survey_level='Person-Level',
    skip_variables=None,
):
    """Apply ``acs_pums_variable_mapper`` to multiple datasets.

    Parameters
    ----------
    datasets : dict
        Dictionary where keys are table names and values are DataFrames.
    acs_pums_data_dictionary_path : str, optional
        Path to the local ACS PUMS Data Dictionary file.
    acs_pums_data_dictionary_url : str, optional
        URL to the online ACS PUMS Data Dictionary file.
    acs_year : str, optional
        ACS survey year. Used when no path or URL is provided.
    table_group : str, default ``'1-Year'``
        Table group within the ACS year.
    survey_level : str, default ``'Person-Level'``
        Survey level for mapping (``'Person-Level'`` or ``'Housing-Level'``).
    skip_variables : list[str], optional
        Variables to skip during mapping. Defaults to an empty list.

    Returns
    -------
    dict
        Dictionary of processed DataFrames keyed by the original table name.
    """

    if skip_variables is None:
        skip_variables = []

    processed = {}
    for name, df in datasets.items():
        processed[name] = acs_pums_variable_mapper(
            df=df,
            acs_pums_data_dictionary_path=acs_pums_data_dictionary_path,
            acs_pums_data_dictionary_url=acs_pums_data_dictionary_url,
            acs_year=acs_year,
            table_group=table_group,
            survey_level=survey_level,
            skip_variables=skip_variables,
        )
    return processed
