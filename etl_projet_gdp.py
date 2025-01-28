import pandas as pd 
from bs4 import BeautifulSoup
import sqlite3
import numpy as  np 
import requests
from datetime import datetime


# Code for ETL operations on Country-GDP data

# Importing the required libraries
url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ['Country','GDP_USD_millions']
db_nam e='World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = './Countries_by_GDP.csv'

def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''
    
    response= requests.get(url).text
    soup = BeautifulSoup(response, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = soup.find_all('tbody')
    rows = tables[2].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            if col[0].find('a') is not None and '-' not in col[2]:
                data_dict = {
                    "Country":col[0].a.contents[0],
                    "GDP_USD_millions": col[2].contents[0]
                }
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)

    return df

def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''

    df['GDP_USD_millions'] = df['GDP_USD_millions'].replace({',':''}, regex=True).astype(float)
    df['GDP_USD_billions'] = (df['GDP_USD_millions']/1000).round(2)

    df.drop(columns=['GDP_USD_millions'], axis=1, inplace=True)

    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''
    df.to_csv(csv_path, index=False)
    print(f'DataFrame enrégistré avec success dans {csv_path}')

def load_to_db(df, db_name, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''
    conn = sqlite3.connect(db_name)
    df.to_sql(table_name, con=conn, if_exists='replace', index=False)
    conn.close()
    print(f'DataFrame enregistré avec succès dans la base de données {db_name} sous la table {table_name}')

def run_query(query_statement, db_name):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute(query_statement)
    results = cursor.fetchall()
    conn.close()
    for row in results:
        print(row)

def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing. '''
    with open('progress_log.txt', 'a') as log_file:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_file.write(f'[{timestamp}] {message}\n')
    print(f'[LOG] {message}')

# ETL Pipeline Execution
try:
    log_progress("Début du processus ETL")

    # Extraction
    log_progress("Extraction des données en cours")
    extracted_data = extract(url, table_attribs)
    log_progress("Extraction des données terminée")

    # Transformation
    log_progress("Transformation des données en cours")
    transformed_data = transform(extracted_data)
    log_progress("Transformation des données terminée")

    # Chargement dans un fichier CSV
    log_progress("Chargement des données dans un fichier CSV en cours")
    load_to_csv(transformed_data, csv_path)
    log_progress("Chargement des données dans un fichier CSV terminé")

    # Chargement dans une base de données
    log_progress("Chargement des données dans une base de données en cours")
    load_to_db(transformed_data, db_name, table_name)
    log_progress("Chargement des données dans une base de données terminé")

    # Exécution d'une requête pour vérification
    log_progress("Exécution d'une requête de test sur la base de données")
    test_query = f"SELECT * FROM {table_name} LIMIT 5"
    run_query(test_query, db_name)
    log_progress("Requête exécutée avec succès")

except Exception as e:
    log_progress(f"Erreur : {str(e)}")