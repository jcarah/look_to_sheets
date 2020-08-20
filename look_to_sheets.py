from __future__ import print_function
import pickle
import os.path
import csv
from io import StringIO 
import pandas as pd
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import looker_sdk
from looker_sdk import models

# Google configs
scopes = ['https://www.googleapis.com/auth/spreadsheets']
# replace with your own spreadsheet id
spreadsheet_id = '1sSGmmpv2uee_WZP4LHQ8MERPyD2gvk_LTjpn7BXhwT8'
sheet_name='Sheet1'
range = f'{sheet_name}!A1'

# Looker configs

sdk = looker_sdk.init31("looker.ini")
# Set the look_id that you'd like to export
look_to_export=659
look_csv_output = StringIO(sdk.run_look(look_to_export,"csv"))
df = pd.read_csv(look_csv_output, sep =",").fillna('')

def create_service(client_secret_file, api_service_name, api_version, *scopes):
    global service
    scopes = [scope for scope in scopes[0]]
    cred = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            cred = pickle.load(token)

    if not cred or not cred.valid:
        if cred and cred.expired and cred.refresh_token:
            cred.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(client_secret_file, SCOPES)
            cred = flow.run_local_server()

        with open('token.pickle', 'wb') as token:
            pickle.dump(cred, token)

    try:
        service = build(api_service_name, api_version, credentials=cred)
        print(api_service_name, 'service created successfully')
        return service
    except Exception as e:
        print(e)
        #return None

def export_data_to_sheets(service, spreadsheet_id):
    response_date = service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        valueInputOption='RAW',
        range=range,
        body=dict(
            majorDimension='ROWS',
            values=df.T.reset_index().T.values.tolist())
    ).execute()
    print('Sheet successfully Updated')


service = create_service(
    'credentials.json','sheets', 'v4',['https://www.googleapis.com/auth/spreadsheets']
    )
export_data_to_sheets(service, spreadsheet_id)


