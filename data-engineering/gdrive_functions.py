from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import io
from googleapiclient.http import MediaIoBaseDownload

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/drive']
CREDENTIALSPATH = 'D:\\SeaceSiafAnexo5\\gdrivepy\\MEF\\'

def serviceSheetDrive(credentialsPath):
    credentialsPathToken = credentialsPath + 'token.pickle'
    credentialsPathJson = credentialsPath + 'credentials.json'
    """Shows basic usage of the Drive v3 API.
    Prints the names and ids of the first 10 files the user has access to.
    """
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(credentialsPathToken):
        with open(credentialsPathToken, 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credentialsPathJson, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(credentialsPathToken, 'wb') as token:
            pickle.dump(creds, token)
    service = build('drive', 'v3', credentials=creds)
    return service

def downloadSheetFileFromDrive(fileDriveId,FileDescargadoPath):
    service = serviceSheetDrive(CREDENTIALSPATH)
    request = service.files().export_media(fileId=fileDriveId,mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')             
    fh = io.FileIO(FileDescargadoPath, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print ("Download " , int(status.progress() * 100))
