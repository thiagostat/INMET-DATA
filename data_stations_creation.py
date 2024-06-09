# Importing packages
import pandas as pd

inmet_stations = pd.read_csv('DATA//CatalogoEstaçõesAutomáticas.csv', sep=';', decimal=',', encoding='ISO-8859-1')

# Saving file
inmet_stations.to_csv('DATA//INMET_STATIONS.csv', index = False)


