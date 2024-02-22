# -*- coding: utf-8 -*-
"""
@author: benjamin.lewis
"""

import pandas as pd
import pyodbc
import pysftp
import requests
import logging
from datetime import datetime

logging.basicConfig(filename='data_upload_log.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

db_connection_string = "DRIVER={};SERVER=;DATABASE=;UID=;PWD="

sftp_info = {'host': '', 'username': '', 'password': ''}

pushover_user = ""
pushover_token = ""

def fetch_data(query, conn_string):
    try:
        conn = pyodbc.connect(conn_string)
        data = pd.read_sql(query, conn)
        conn.close()
        logging.info("Data fetched successfully.")
        return data
    except Exception as e:
        logging.error("Error fetching data: {}".format(e))
        send_notification("Data fetch failed", str(e))
        return None

def upload_to_sftp(dataframe, filename):
    with pysftp.Connection(sftp_info['host'], username=sftp_info['username'], password=sftp_info['password']) as sftp:
        with sftp.cd('upload_path'):  # Target directory on SFTP
            dataframe.to_csv(filename, index=False)
            sftp.put(filename)
            logging.info(f"{filename} uploaded successfully.")

def send_notification(title, message):
    payload = {
        "token": pushover_token,
        "user": pushover_user,
        "message": message,
        "title": title
    }
    r = requests.post("https://api.pushover.net/1/messages.json", data=payload)
    if r.status_code == 200:
        logging.info("Notification sent successfully.")
    else:
        logging.error("Failed to send notification.")

if __name__ == "__main__":
    sql_query = "SELECT * FROM your_table"
    data = fetch_data(sql_query, db_connection_string)
    if data is not None:
        file_name = f"data_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
        try:
            upload_to_sftp(data, file_name)
        except Exception as e:
            logging.error("SFTP upload failed: {}".format(e))
            send_notification("SFTP Upload Failed", str(e))
