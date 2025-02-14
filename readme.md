# American Community Survey (ACS) Public Use Microdata Sample (PUMS) Variable Mapper

## Description

This tool imports U.S. Census Bureau American Community Survey (ACS) Public Use Microdata Sample (PUMS) data and maps variables/columns values using the official ACS PUMS Data Dictionary. It was tested for ACS 2023 1-Year PUMS data and provides the following features:

- **Variable Mapping**: Automatically maps coded values in PUMS data to their corresponding labels using the official ACS PUMS Data Dictionary.
- **Local or Online Data Dictionary**: Allows mapping using either a local file or a URL to the official data dictionary.
- **Survey Level Selection**: Supports both Person-Level and Housing-Level records.
- **Custom Column Transformation**: Includes predefined mappings for occupational codes to Standard Occupational Classification (SOC) categories.

## Output

The processed dataset includes human-readable values for coded variables, making it easier to analyze and interpret the data.

## Usage

### Python dependencies

```.ps1
python -m pip install numpy pandas requests
```

### Functions

#### `zipfile_download`

```.py
zipfile_download(url, directory)
```

##### Description

- Downloads and extracts a .zip file from a given URL into a specified directory.

##### Parameters

- `url`: _str_. The URL of the .zip file.
- `directory`: _str_. The directory where the .zip file contents will be extracted.

#### `acs_pums_variable_mapper`

```.py
acs_pums_variable_mapper(df, acs_pums_data_dictionary_path=None, acs_pums_data_dictionary_url=None, survey_level='Person-Level')
```

##### Description

- Maps coded variable values in a PUMS dataset to their corresponding labels based on the ACS PUMS Data Dictionary.

##### Parameters

- `df`: _DataFrame_. The input DataFrame containing PUMS data.
- `acs_pums_data_dictionary_path`: _str_. Path to the local ACS PUMS Data Dictionary file.
- `acs_pums_data_dictionary_url`: _str_. URL to the online ACS PUMS Data Dictionary file.
- `survey_level`: _str_. The survey level for mapping ('Person-Level' or 'Housing-Level').
- `skip_variables`: _str list_. A list of variables/columns to skip during the mapping process. Defaults to an empty list.

### Code Workflow Example

```.py
# Import packages
import os
import numpy as np
import pandas as pd

# Download American Community Survey (ACS) Public Use Microdata Sample (PUMS) Person-Level Records for Wyoming (WY)
zipfile_download(url='https://www2.census.gov/programs-surveys/acs/data/pums/2023/1-Year/csv_pny.zip', directory=os.path.join(os.path.expanduser('~'), 'Downloads'))

# Import American Community Survey (ACS) dataset (Source: https://www2.census.gov/programs-surveys/acs/data/pums/2023/1-Year/)
american_community_survey_df = pd.read_csv(
    filepath_or_buffer=os.path.join(os.path.expanduser('~'), 'Downloads', 'psam_p36.csv'),
    sep=',',
    header=0,
    index_col=None,
    skiprows=0,
    skipfooter=0,
    dtype=None,
    engine='python',
    encoding='utf-8',
    keep_default_na=True,
)

# Select columns
american_community_survey_df = american_community_survey_df.filter(
    items=['REGION', 'STATE', 'SERIALNO', 'SPORDER', 'ESR', 'COW', 'MAR', 'OCCP', 'INDP', 'RAC1P', 'SEX', 'AGEP', 'SCHL', 'HICOV', 'ENG', 'CIT', 'POBP', 'WKHP', 'WAGP'],
)

# Map variables/columns values using the official ACS PUMS Data Dictionary
american_community_survey_df = acs_pums_variable_mapper(
    df=american_community_survey_df,
    acs_pums_data_dictionary_url='https://www2.census.gov/programs-surveys/acs/tech_docs/pums/data_dict/PUMS_Data_Dictionary_2023.txt',
    survey_level='Person-Level',
 skip_variables=['AGEP', 'WKHP', 'WAGP'],
)

# Alternative using local file
# american_community_survey_df = acs_pums_variable_mapper(df=american_community_survey_df, acs_pums_data_dictionary_path=os.path.join(os.path.expanduser('~'), 'Downloads', 'PUMS_Data_Dictionary_2023.txt'), survey_level = 'Person-Level', skip_variables=['AGEP', 'WKHP', 'WAGP'])


# Create "OCCP_SOC_Code" column
american_community_survey_df = american_community_survey_df.assign(
    OCCP_SOC_Code=np.select(
        [american_community_survey_df['OCCP'] == 'Unemployed, With No Work Experience In The Last 5 Years Or Earlier Or Never Worked'],
        [american_community_survey_df['OCCP']],  # Keep the full occupation if condition is true
        default=american_community_survey_df['OCCP'].str[:3],  # Take the first 3 characters if condition is false
    ),
)

# Create "OCCP_SOC" column from 3-digit Standard Occupational Classification (SOC) mapping (Source: https://hrs.isr.umich.edu/sites/default/files/biblio/dr-021_0.pdf)
american_community_survey_df = american_community_survey_df.assign(
    OCCP_SOC=lambda row: row['OCCP_SOC_Code'].replace(
        to_replace={
            'BUS': 'Business Operations Specialists',
            'CLN': 'Building and Grounds Cleaning and Maintenance Occupations',
            'CMM': 'Computer and Mathematical Occupations',
            'CMS': 'Community and Social Services Occupations',
            'CON': 'Construction Trades',
            'EAT': 'Food Preparation and Serving Occupations',
            'EDU': 'Education, Training, and Library Occupations',
            'ENG': 'Architecture and Engineering Occupations',
            'ENT': 'Arts, Design, Entertainment, Sports, and Media Occupations',
            'EXT': 'Extraction Workers',
            'FFF': 'Farming, Fishing, and Forestry Occupations',
            'FIN': 'Financial Specialists',
            'HLS': 'Healthcare Support Occupations',
            'LGL': 'Legal Occupations',
            'MED': 'Healthcare Practitioners and Technical Occupations',
            'MGR': 'Management Occupations',
            'MIL': 'Military Specific Occupation',
            'OFF': 'Office and Administrative Support Occupations',
            'PRD': 'Production Occupations',
            'PRS': 'Personal Care and Service Occupations',
            'PRT': 'Protective Service Occupations',
            'RPR': 'Installation, Maintenance, and Repair Workers',
            'SAL': 'Sales Occupations',
            'SCI': 'Life, Physical, and Social Science Occupations',
            'TRN': 'Transportation and Material Moving Occupations',
        },
        regex=False,
    ),
)

# Create "OCCP_SOC_Group" column for simplification
american_community_survey_df = american_community_survey_df.assign(
    OCCP_SOC_Group=lambda row: row['OCCP_SOC_Code'].replace(
        to_replace={
            'BUS': 'Business and Management',
            'CLN': 'Services',
            'CMM': 'Professional and Technical',
            'CMS': 'Services',
            'CON': 'Trades and Construction',
            'EAT': 'Services',
            'EDU': 'Professional and Technical',
            'ENG': 'Professional and Technical',
            'ENT': 'Arts, Media, and Entertainment',
            'EXT': 'Trades and Construction',
            'FFF': 'Agriculture and Natural Resources',
            'FIN': 'Professional and Technical',
            'HLS': 'Healthcare',
            'LGL': 'Professional and Technical',
            'MED': 'Healthcare',
            'MGR': 'Business and Management',
            'MIL': 'Military',
            'OFF': 'Sales and Office',
            'PRD': 'Production and Transportation',
            'PRS': 'Services',
            'PRT': 'Services',
            'RPR': 'Trades and Construction',
            'SAL': 'Sales and Office',
            'SCI': 'Professional and Technical',
            'TRN': 'Production and Transportation',
        },
        regex=False,
    ),
)

# Reorder columns
american_community_survey_df = american_community_survey_df.filter(
    items=['REGION', 'STATE', 'SERIALNO', 'SPORDER', 'ESR', 'COW', 'MAR', 'OCCP', 'OCCP_SOC_Code', 'OCCP_SOC', 'OCCP_SOC_Group', 'INDP', 'RAC1P', 'SEX', 'AGEP', 'SCHL', 'HICOV', 'ENG', 'CIT', 'POBP', 'WKHP', 'WAGP'],
)

# Export transformed "american_community_survey_df" to .csv
american_community_survey_df.to_csv(path_or_buf=os.path.join(os.path.expanduser('~'), 'Downloads', 'american-community-survey.csv'), sep=',', na_rep='', header=True, index=False, index_label=None, encoding='utf-8')
```

## Documentation

- [ACS PUMS Technical Documentation](https://www.census.gov/programs-surveys/acs/technical-documentation.html)
- [ACS PUMS Data Dictionary 2023](https://www2.census.gov/programs-surveys/acs/tech_docs/pums/data_dict/PUMS_Data_Dictionary_2023.txt)
- [ACS PUMS Data through File Transfer Protocol (FTP)](https://www2.census.gov/programs-surveys/acs/data/pums/)
