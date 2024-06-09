# Importing packages
import requests
import zipfile
import io
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Function to download INMET's data from specific years
def download_inmet_data(years = range(2023, 2025), path = "https://portal.inmet.gov.br/uploads/dadoshistoricos/", output_path = "DATA"):
    
    # Vary the year of the selected range of years
    for i in years:
        year = str(i)
        url = f'{path}{year}.zip'
        print(url)

        # Initialize a dictionary to hold your dataframes
        dataframes = {}

        # Download the CSV files inside the zip file
        response = requests.get(url)
        if response.status_code == 200:
            print(f'Initializing download from {year}')

            # Use io.BytesIO to convert the bytes-like object to a file-like object
            file_like_object = io.BytesIO(response.content)

            # Use zipfile to handle the zip file
            with zipfile.ZipFile(file_like_object) as z:

                # Get a list of all files in the zip file
                file_names = z.namelist()

                # Filter for CSV files if needed
                csv_files = [f for f in file_names if f.endswith('.CSV')]
                count_files = 0

                # Loop through each CSV file
                for csv_file in csv_files:
                    # print(csv_file)

                    # Read the CSV file directly from the zip file
                    with z.open(csv_file) as f:
                        dfa = pd.read_csv(f, sep=';', decimal=',', skiprows=8, usecols=list(range(19)), header=0, encoding='ISO-8859-1')

                    with z.open(csv_file) as f:
                        dfb = pd.read_csv(f, sep=';', decimal=',', nrows=8, usecols=list(range(2)), header=None, encoding='ISO-8859-1')

                    # Directly assign new column names
                    dfa.columns = ['DATA', 'HORA', 'PRECIPITACAO_TOTAL', 'PRESSAO_ATM', 'PRESSAO_ATM_MAX_HORA_ANT', 'PRESSAO_ATM_MIN_HORA_ANT', 'RADIACAO_GLOBAL',
                                    'TEMPERATURA_AR', 'TEMPERATURA_AR_ORVALHO', 'TEMPERATURA_AR_MAX_HORA_ANT', 'TEMPERATURA_AR_MIN_HORA_ANT',
                                    'TEMPERATURA_AR_ORVALHO_MAX_HORA_ANT', 'TEMPERATURA_AR_ORVALHO_MIN_HORA_ANT', 'UMIDADE_REL_MAX_HORA_ANT', 'UMIDADE_REL_MIN_HORA_ANT',
                                    'UMIDADE_REL', 'VENTO_DIRECAO_HORARIA', 'VENTO_RAJADA_MAXIMA', 'VENTO_VELOCIDADE_HORARIA']
                    
                    # Create new columns with INMET's stations info
                    dfa["REGIAO"] = dfb[1][0]
                    dfa["UF"] = dfb[1][1]
                    dfa["ESTACAO"] = dfb[1][2]
                    dfa["CODIGO"] = dfb[1][3]
                    dfa["LATITUDE"] = dfb[1][4]
                    dfa["LONGITUDE"] = dfb[1][5]
                    dfa["ALTITUDE"] = dfb[1][6]
                    dfa["DATA_DE_FUNDACAO"] = dfb[1][7]

                    # Remove ':', 'UTC' and format time correctly
                    dfa['HORA'] = dfa['HORA'].str.replace(':', '')
                    dfa['HORA'] = dfa['HORA'].str.replace(' UTC', '')
                    dfa['HORA'] = dfa['HORA'].str.pad(width=4, side='left', fillchar='0')  # Ensure time is always 4 digits
            
                    # Replace '/' for '-' and format date correctly
                    dfa['DATA'] = dfa['DATA'].str.replace('/', '-')

                    # Turn Latitude, longitude and altitude numerical variables
                    for col in ['LATITUDE', 'LONGITUDE', 'ALTITUDE']:
                        dfa[col] = dfa[col].str.replace(',', '.')
                        dfa[col] = pd.to_numeric(dfa[col], errors='coerce')

                    # Replace -9999.0 values for NA
                    num_cols1 = ['PRECIPITACAO_TOTAL', 'PRESSAO_ATM', 'PRESSAO_ATM_MAX_HORA_ANT', 'PRESSAO_ATM_MIN_HORA_ANT', 'RADIACAO_GLOBAL',
                                'TEMPERATURA_AR', 'TEMPERATURA_AR_ORVALHO', 'TEMPERATURA_AR_MAX_HORA_ANT', 'TEMPERATURA_AR_MIN_HORA_ANT',
                                'TEMPERATURA_AR_ORVALHO_MAX_HORA_ANT', 'TEMPERATURA_AR_ORVALHO_MIN_HORA_ANT', 'UMIDADE_REL_MAX_HORA_ANT', 'UMIDADE_REL_MIN_HORA_ANT',
                                'UMIDADE_REL', 'VENTO_DIRECAO_HORARIA', 'VENTO_RAJADA_MAXIMA', 'VENTO_VELOCIDADE_HORARIA']

                    for col in num_cols1:
                        dfa.loc[dfa[col] < -9000, col] = pd.NA
                                       
                    # Assign the DataFrame to a key in the dictionary based on your parameter
                    df_name = dfb[1][2]
                    dataframes[f'{count_files}'] = dfa
                    count_files += 1
                    del dfa, dfb

            # Use pd.concat to merge all DataFrames in the dictionary into one
            inmet_data = pd.concat(dataframes.values(), ignore_index=True)
            print(f'Stored {len(dataframes)} from a total of {len(csv_files)} files.')
            del dataframes
                    
            # Count the number of stations on the dataset
            print(f'Saved {len(inmet_data.ESTACAO.unique())} from a total of {len(csv_files)} files.')

            # Write the DataFrame to a CSV file
            df_name_csv = f'{output_path}//INMET_DATA_{year}.csv'
            inmet_data.to_csv(df_name_csv, index=False)
            print("Download finished")
            del inmet_data

        else:
            print("Failed to download the file")

# Define the function to calculate missing data
def missing_data_percentage(df):
    missing_data = df.isnull().sum() / len(df) * 100
    return missing_data

# Define de function to calculate missing data for each File from a range of Years
def inmet_data_quality(years = range(2024, 2025), path = 'DATA'):

    all_missing_data = []

    # Calculate the amount of missing data for each year selected
    for year in years:
            file_name = f'{path}//INMET_DATA_{year}.csv'
            print(file_name)
            df = pd.read_csv(file_name)
            missing_data = missing_data_percentage(df)
            all_missing_data.append(missing_data)

    # Combine the missing data percentages into a single DataFrame
    combined_missing_data = pd.concat(all_missing_data, axis = 1)
    combined_missing_data.columns = [f'Year_{i}' for i in years]

    # Save data quality file
    combined_missing_data.to_csv(f'{path}//INMET_DATA_QUALITY.csv', index = True)
    print('Data quality file saved')

# Read already downloaded data from INMET for chosen years from all stations
def read_inmet_data(years = range(2024, 2025), path = 'DATA'):

    years_data = {}

    for year in years:
        file_name = f'{path}\\INMET_DATA_{year}.csv'
        print(file_name)
        df = pd.read_csv(file_name)
        print(f'Data from {year} has shape equal to {df.shape}')   
        years_data[f'{year}'] = df
        del df

    # Concatenating data from the time span
    return pd.concat(years_data.values(), ignore_index=True) 

# # Read already downloaded data from INMET for chosen years from one specific station
def read_inmet_station_data(station: str, years = range(2024, 2025), path = 'DATA'):

    station_year_data = {}
    for year in years:
        file_name = f'{path}\\INMET_DATA_{year}.csv'
        print(file_name)
        df = pd.read_csv(file_name)
        year_data = df[df['CODIGO'] == station]
        del df
        print(year_data.shape)   
        station_year_data[f'{year}'] = year_data
        del year_data

    # Concatenating data from the time span
    return pd.concat(station_year_data.values(), ignore_index=True)

