from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
import pandas as pd
from datetime import datetime as dt
import os

### ---------- EXAMPLE DOWNLOAD GOOGLESHEET ------------------

# Connect to Drive API
spreadsheet_id = '1-2FvSx-rVma1RCZ0Hu4p7J8DJjN6TpZwz6CgNSwa7CY'
url = 'https://docs.google.com/spreadsheets/d/1-2FvSx-rVma1RCZ0Hu4p7J8DJjN6TpZwz6CgNSwa7CY'
emails = ['treasury@celsius.network',
          'weekly.portfolio@celsius.network']

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
creds = None
creds = Credentials.from_authorized_user_file('token.json', SCOPES)
service = build('sheets', 'v4', credentials=creds)

# Call the Drive v3 API
results = service.files().export_media(
        fileId = spreadsheet_id,
        mimeType = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet').execute()
with open("excel_input/Portfolio_Waterfall_Last.xlsx", "wb") as f:
    f.write(results)
    f.close()

# Below to edit a token that will enable a chosen Scope.
'''flow = InstalledAppFlow.from_client_secrets_file('client_secret_510599516312-aj6d72c90n3fmrbf3ou6gromil06pr8c.apps.googleusercontent.com.json', SCOPES)
creds = flow.run_local_server(port=0)
with open('token.json', 'w') as token:
    token.write(creds.to_json())'''

##### --------------- EXAMPLE BATCH GET -----------------------
# Connect to Drive API
spreadsheet_id = '1-2FvSx-rVma1RCZ0Hu4p7J8DJjN6TpZwz6CgNSwa7CY'


SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
creds = None
creds = Credentials.from_authorized_user_file('token.json', SCOPES)
service = build('sheets', 'v4', credentials=creds)

# Call the Drive v3 API
SAMPLE_RANGE_NAME = 'Undeployed & Underdeployed!B2:O210'
request = service.spreadsheets().values().batchGet(
    spreadsheetId=spreadsheet_id,
    ranges=SAMPLE_RANGE_NAME,
    valueRenderOption='FORMATTED_VALUE')
response = request.execute()
df = pd.DataFrame(response['valueRanges'][0]['values']).dropna().reset_index(drop=True)

### ------------- EXAMPLE BATCH CLEAR/UPDATE -----------------

scope = [
'https://www.googleapis.com/auth/spreadsheets',
'https://www.googleapis.com/auth/drive',
]

last_updated = "Updated at - " + dt.datetime.utcnow().strftime("%m/%d/%Y, %H:%M:%S") +'UTC'
# this is ID for testing
#SPREADSHEET_ID = '1IptNC0hEhwvuyfI4m2kP9-rR-3jDAOhgQaPNkW2I_QQ'
# this is ID for waterfall sheet
SPREADSHEET_ID = "1ZkSLZH2QwHnfdSpQUWAv2qum6xzhemngjWQBJBn_KeM" #Real Last

if os.path.exists('write_token.json'):
        creds = Credentials.from_authorized_user_file('write_token.json', scope)

service = build('sheets', 'v4', credentials=creds)

response = service.spreadsheets().values().clear(
    spreadsheetId=SPREADSHEET_ID,
    range="Categories!A1",
    ).execute()

response = service.spreadsheets().values().update(
    spreadsheetId=SPREADSHEET_ID,
    valueInputOption='RAW',
    range="Categories!A1",
    body=dict(
        majorDimension='ROWS',
        values=[[last_updated]]
)
).execute()