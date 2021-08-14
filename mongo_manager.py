from pymongo import MongoClient
from pymongo.mongo_replica_set_client import MongoReplicaSetClient
from pymongo.read_preferences import ReadPreference
from os.path import (
    join,
    exists,
    abspath,
    dirname,
)
import os
from json import loads

class MongoDBManager:
    url = None
    client_ = None
    databases_ = None
    collections_ = None   

    def __init__(self):
        file = 'config/mongo_creds.json'  #read mongo credentials from json file
        pwd = dirname(abspath(__file__))
        #pwd = os.getcwd()
        path = join(pwd, file)

        if not exists(path):
            raise FileNotFoundError('File', path, 'not found')
        f = open(path)
        creds = loads(f.read()) 
        #print(self.path)
        self.url = creds['connection_string']
        #print(self.url)    # dont uncomment this!
        self.client_ = MongoClient(
            self.url, 
            read_preference=ReadPreference.SECONDARY_PREFERRED,
            uuidRepresentation = 'csharpLegacy'
        )
        self.databases_ = self.client_.list_database_names()   

    def get_database(self, dbname):
        if dbname in self.databases_:
            return self.client_[dbname]
        
    def get_collection(self, dbname='', collection_name=''):
        if dbname not in self.databases_:
            raise ValueError('Database', dbname, 'not found')
        db = self.client_[dbname]
        self.collections_ = db.list_collection_names()
        if collection_name not in self.collections_:
            raise ValueError('Collection', collection_name, 'not found')
        collection = db[collection_name]
        return collection
