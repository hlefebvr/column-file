from time import time
import os, csv, json

from .CSVExternalAlgorithm import CSVLocalAlgorithm
from .logger import Logger

class LocalColumnFile:

    def __init__(self, verbose):
        self.dbname = None
        self.schema = None
        self.to_commit = set()
        self.logger = Logger(verbose)
        self.algos = CSVLocalAlgorithm()
    
    def log(self, msg): self.logger.log(msg)

    def create(self, dbname, schema):
        os.mkdir(dbname)
        with open('%s/schema' % dbname, 'w+') as f:
            f.write(json.dumps(schema))
        self.schema = schema
        self.open(dbname, schema_is_set=False)
        self.log("[OK] %s db was created" % dbname)
    
    def open(self, dbname, schema_is_set = False):
        self.dbname = dbname
        if not schema_is_set:
            with open('%s/schema' % dbname, 'r') as f:
                self.schema = json.loads(f.read())
        self.log("[OK] %s db opened" % dbname)
    
    def find(self, hash_keys, sort_keys): return;
    
    def scan(self, sub_hash_keys, sub_sort_keys, filter): return;
    
    def merge(self, hash_keys, sort_keys, column_values):
        row = (time(), 'MERGE') + sort_keys + (json.dumps(column_values),)
        return self._add_to_buffer(hash_keys, row)
    
    def put(self, hash_keys, sort_keys, column_values):
        row = (time(), 'PUT') + sort_keys + (json.dumps(column_values),)
        return self._add_to_buffer(hash_keys, row)
    
    def delete(self, hash_keys, sort_keys):
        row = (time(), 'DELETE') + sort_keys + ("_",)
        return self._add_to_buffer(hash_keys, row)
    
    def commit(self): return;

    def get_schema(self): return self.schema;
    
    def _add_to_buffer(self, hash_keys, row):
        path = self.dbname
        for folder in hash_keys:
            path += "/%s" % folder
            if not os.path.exists(path): os.mkdir(path)
        path += '/buffer.csv'
        mode = 'a+' if os.path.exists(path) else 'a+'
        print(path)
        with open(path, mode) as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow(row)
        self.to_commit.add(hash_keys)
        f.close()
