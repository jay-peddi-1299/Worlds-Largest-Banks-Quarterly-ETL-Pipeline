# Import required libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime

# Define essential functions
def log_progress(message):
    '''
        This function logs the progress of the ETL job with a message in a text file 'code_log.txt'.
    '''
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    current_timestamp = datetime.now()
    current_timestamp = current_timestamp.strftime(timestamp_format)
    with open('code_log.txt', 'a') as file:
        file.write(current_timestamp + ' : ' + message + '\n')

def extract(url, table_attribs):
    '''
        This function extracts data from the URL, saves it in a dataframe, and returns the dataframe.
    '''
    df = pd.DataFrame(columns=table_attribs)
    html_page = requests.get(url).text
    html_code = BeautifulSoup(html_page, 'html.parser')
    tables = html_code.find_all('tbody')
    rows = tables[0].find_all('tr')

    # Debug 1:
    # print(tables)
    # Debug 2:
    # print(rows)
    # <td><span class="flagicon"><span class="mw-image-border" typeof="mw:File"><a href="/web/20230908091635/https://en.wikipedia.org/wiki/United_States" title="United States"><img alt="United States" class="mw-file-element" data-file-height="650" data-file-width="1235" decoding="async" height="12" src="//web.archive.org/web/20230908091635im_/https://upload.wikimedia.org/wikipedia/en/thumb/a/a4/Flag_of_the_United_States.svg/23px-Flag_of_the_United_States.svg.png" srcset="//web.archive.org/web/20230908091635im_/https://upload.wikimedia.org/wikipedia/en/thumb/a/a4/Flag_of_the_United_States.svg/35px-Flag_of_the_United_States.svg.png 1.5x, //web.archive.org/web/20230908091635im_/https://upload.wikimedia.org/wikipedia/en/thumb/a/a4/Flag_of_the_United_States.svg/46px-Flag_of_the_United_States.svg.png 2x" width="23"/></a></span></span> <a href="/web/20230908091635/https://en.wikipedia.org/wiki/JPMorgan_Chase" title="JPMorgan Chase">JPMorgan Chase</a>

    count = 0
    for row in rows:
        if count < 10:
            cols = row.find_all('td')
            if len(cols) != 0:
                row_dict = {
                    'Name': cols[1].find_all('a')[1].contents[0].strip(),
                    'MC_USD_Billion': float(cols[2].contents[0])
                }
                row_df = pd.DataFrame(row_dict, index=[0])
                df = pd.concat([df, row_df], ignore_index=True)
                count += 1
        else:
            break
    return df

def transform(df, csv_path):
    '''
        This function transforms the dataframe by adding three new calculated columns:
        MC_GBP_Billion, MC_EUR_Billion, and MC_INR_Billion.
    '''
    # Exchange Rate DataFrame
    er_df = pd.read_csv(csv_path)

    # Exchange Rate Dictionary
    exchange_rate = {}

    for idx in range(len(er_df)):
        exchange_rate[er_df.loc[idx, 'Currency']] = er_df.loc[idx, 'Rate'] 

    gbp_rate = float(exchange_rate['GBP'])
    df['MC_GBP_Billion'] = [np.round(x * gbp_rate, 2) for x in df['MC_USD_Billion']]
    
    eur_rate = float(exchange_rate['EUR'])
    df['MC_EUR_Billion'] = [np.round(x * eur_rate, 2) for x in df['MC_USD_Billion']]

    inr_rate = float(exchange_rate['INR'])
    df['MC_INR_Billion'] = [np.round(x * inr_rate, 2) for x in df['MC_USD_Billion']]

    return df

def load_to_csv(df, output_path):
    '''
        This function loads the transformed dataframe to a CSV file.
    '''
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    '''
        This function loads the transformed dataframe to an SQL database.
    '''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    '''
        This function runs queries on the database and displays both the query and its result.
    '''
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_statement)
    print(query_output)

# Initialize necessary variables
URL = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
column = ['Name', 'MC_USD_Billion']

exchange_rate_csv_file = 'exchange_rate.csv'
largest_banks_csv_file = 'largest_banks.csv'

db_name = 'Banks.db'
table_name = 'Largest_Banks'

# Call relevant functions in the order defined by the ETL process and log them
log_progress('Preliminaries complete. Initiating ETL process')

extracted_data = extract(URL, column)
log_progress('Data extraction complete. Initiating Transformation process')

transformed_data = transform(extracted_data, exchange_rate_csv_file)
log_progress('Data transformation complete. Initiating Loading process')

load_to_csv(transformed_data, largest_banks_csv_file)
log_progress('Data saved to CSV file')

conn = sqlite3.connect(db_name)
log_progress('SQL Connection initiated')

load_to_db(transformed_data, conn, table_name)
log_progress('Data loaded to Database as a table. Executing queries')

run_query(f"SELECT * FROM {table_name}", conn)
run_query(f"SELECT AVG(MC_GBP_Billion) FROM {table_name}", conn)
run_query(f"SELECT Name FROM {table_name} LIMIT 5", conn)
log_progress('Process Complete')

conn.close()
log_progress('Server Connection closed')
