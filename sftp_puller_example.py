# -*- coding: utf-8 -*-
"""
@author: benjamin.lewis
"""

import pysftp
import os
import logging
import pandas as pd
import pyodbc
import requests

# Setup logging
logging.basicConfig(level=logging.INFO, filename='sftp_download.log',
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Database Connection Setup
db_conn_str = 'DRIVER={};SERVER=;DATABASE=;UID=;PWD='  # Fill in your database connection details
sql_query = "SELECT application_number FROM import_table"

# Pushover Notification Setup
pushover_token = 'your_token'
pushover_user = 'your_user_key'

def send_pushover_notification(message, title='Alert'):
    """Send notification via Pushover."""
    data = {
        'token': pushover_token,
        'user': pushover_user,
        'message': message,
        'title': title
    }
    response = requests.post('https://api.pushover.net/1/messages.json', data=data)
    if response.status_code == 200:
        logging.info('Pushover notification sent successfully.')
    else:
        logging.error('Failed to send Pushover notification.')

# SFTP Connection Details
sftp_details = {'host': 'your_sftp_host', 'username': 'your_username', 'password': 'your_password'}
remote_dir = '/path/to/remote/csv'
local_dir = '/path/to/local/dir'

def check_for_duplicates(df):
    """Check if any application number in the dataframe exists in the SQL table."""
    conn = pyodbc.connect(db_conn_str)
    existing_apps = pd.read_sql(sql_query, conn)
    conn.close()
    duplicates = df[df['application_number'].isin(existing_apps['application_number'])]
    return not duplicates.empty

def download_csvs():
    with pysftp.Connection(**sftp_details, cnopts=pysftp.CnOpts()) as sftp:
        sftp.cwd(remote_dir)
        for file in sftp.listdir():
            if file.endswith('.csv'):
                local_filepath = os.path.join(local_dir, file)
                if not os.path.exists(local_filepath):
                    sftp.get(file, local_filepath)
                    logging.info(f'Downloaded {file}')
                    df = pd.read_csv(local_filepath)
                    if check_for_duplicates(df):
                        message = f'Duplicate application number found in {file}'
                        send_pushover_notification(message)
                        logging.warning(message)
                else:
                    logging.info(f'File {file} already exists locally. Skipping.')
            else:
                logging.info(f'Skipped non-CSV file: {file}')

if __name__ == '__main__':
    try:
        download_csvs()
    except Exception as e:
        error_message = f'Script failed: {e}'
        logging.error(error_message)
        send_pushover_notification(error_message)
