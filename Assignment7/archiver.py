import zlib
import sys
import io
import os
import pandas as pd
from gcloud import storage
from oauth2client.service_account import ServiceAccountCredentials

class Archiver():
    def __init__(self):
        self.DEFAULT_CREDS = {
                'type': 'service_account',
                'client_id': os.environ['BACKUP_CLIENT_ID'],
                'client_email': os.environ['BACKUP_CLIENT_EMAIL'],
                'private_key_id': os.environ['BACKUP_PRIVATE_KEY_ID'],
                'private_key': os.environ['BACKUP_PRIVATE_KEY'],
        }
        self.credentials = ServiceAccountCredentials.from_json_keyfile_dict(
            credentials_dict
        )
        self.client = storage.Client(credentials=credentials, project='myproject')
        self.bucket = client.get_bucket('mybucket')
        self.blob = bucket.blob('myfile')
        self.blob.upload_from_filename('myfile')

    def receiveWriteCompress(self, data, fname):
        print('Starting compress + write')
        with open(fname+'-compressed', mode="wb") as out:
            for datum in data:
                #print(type(datum))
                datum = str.encode(datum)#bytes(datum)
                #print('after', type(datum))
                compress = zlib.compress(datum, zlib.Z_BEST_COMPRESSION)
                out.write(compress)
        out.close()        
        print('done')    
    
    def pushToCloudStorage(self, creds):
        

if __name__ == "__main__":
    file_archiver = Archiver()
    test_data = ['here','is','some','data']
    file_archiver.receiveWriteCompress(test_data, 'sample')
