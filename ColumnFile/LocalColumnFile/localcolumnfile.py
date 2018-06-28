from time import time
import os, csv, json

from .CSVExternalAlgorithm import CSVLocalAlgorithm
from .logger import Logger

class LocalColumnFile:

    def __init__(self, verbose):
        self.dbname = None
        self.schema = None
        self.to_commit = set()
        self.verbose = verbose
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
    
    def commit(self):
        for hash_key in self.to_commit:
            folder_path = "%s/%s" % (self.dbname, '/'.join(hash_key))
            buffer_path = "%s/buffer.csv" % folder_path
            data_path   = "%s/data.csv" % folder_path
            sorted_path = "%s/sorted.csv" % folder_path
            tmp_path    = "%s/tmp.csv" % folder_path
            
            if not os.path.exists(buffer_path):
                self.logger.log("[WARN] Nothing to commit for key %s" % str(hash_key))
                continue
            
            # sort by key coming operations
            get_key_operation = self._get_function_get_key(2)
            self.algos.sort(buffer_path, sorted_path, get_key_operation)

            # apply operation by "merge"
            get_row_key = self._get_function_get_key()
            n = (0,0,0)
            
            ## open files
            eof_data, eof_operation = False, False
            opened_files = []
            tmp_f = open(tmp_path, 'w+')
            opened_files.append(tmp_f)
            buffer_f = open(sorted_path, 'r')
            opened_files.append(buffer_f)
            try:
                data_f = open(data_path, 'r')
                opened_files.append(data_f)
            except FileNotFoundError: eof_data = True
            
            # initialize CSV reading and writing
            operation = csv.reader(buffer_f)
            try: operation_row = next(operation)
            except StopIteration: eof_operation = True
            if not eof_data:
                data = csv.reader(data_f)
                try: data_row = next(data)
                except StopIteration: eof_data = True
            result = csv.writer(tmp_f)

            #
            class Row:
                def __init__(self, csv_writer, verbose):
                    self.current_key = None
                    self.current_row = None
                    self.delete = False
                    self.csv_writer = csv_writer
                    self.counter = {'PUT': 0, 'DELETE': 0, 'MERGE': 0}
                    self.logger = Logger(verbose)
                def _new_row(self, key, row):
                    self.commit()
                    self.current_key = key
                    self.current_row = row
                    self.delete = False
                def set_data(self, key, row):
                    if self.is_current(key): raise ValueError("Duplicate key was found : %s" % str(key))
                    self._new_row(key, row)
                def apply_operation(self, key, row):
                    operation_type = row[1]
                    if not self.is_current(key):
                        if operation_type == 'DELETE':
                            self.logger.log("[WARN] Trying to delete inexistant data %s" % str(hash_key + key))
                            return;
                        else: self._new_row(key, row[2:])
                    elif operation_type == 'DELETE': self.delete = True
                    elif operation_type == 'PUT':
                        self.current_row = row[2:]
                        self.delete = False
                    elif operation_type == 'MERGE':
                        n_key = len(key)
                        columns_index = 2 + n_key
                        new_columns = json.loads(row[columns_index])
                        previous_columns = json.loads(self.current_row[n_key])
                        resulting_columns = { **previous_columns, **new_columns }
                        self.current_row = key + (json.dumps(resulting_columns),)
                        self.delete = False
                    self.counter[operation_type] += 1
                def commit(self):
                    if not self.delete and self.current_row is not None:
                        self.csv_writer.writerow(self.current_row)
                def is_current(self, key): return self.current_key == key
                def get_count(self): return self.counter
            
            curr_row = Row(result, self.verbose)
            while not eof_data or not eof_operation:
                if not eof_operation: operation_key = get_key_operation(operation_row)
                if not eof_data: data_key = get_row_key(data_row)
                
                if eof_data or (not eof_operation and operation_key > data_key):
                    curr_row.apply_operation(operation_key, operation_row)
                    try: operation_row = next(operation)
                    except StopIteration: eof_operation = True
                elif eof_operation or (not eof_data and operation_key < data_key):
                    curr_row.set_data(data_key, data_row)
                    try: data_row = next(data)
                    except StopIteration: eof_data = True
                else:
                    if not eof_data and not curr_row.is_current(data_key):
                        curr_row.set_data(key_data, row_data)
                    curr_row.apply_operation(operation_key, operation_row)
                    try: operation_row = next(operation)
                    except StopIteration: eof_operation = True
                    try: data_row = next(data)
                    except StopIteration: eof_data = True

            ## close files
            for f in opened_files: f.close()
            
            ## clean folder
            os.remove(buffer_path)
            os.remove(sorted_path)
            os.rename(tmp_path, data_path)

            n = curr_row.get_count()
            self.logger.log("[OK] changes to partition %s have been applied (in total : %s merge, %s put, %s delete)" % (str(hash_key), str(n['MERGE']), str(n['PUT']), str(n['DELETE'])))

    def get_schema(self): return self.schema;
    
    def _get_function_get_key(self, offset = 0):
        schema_sort = self.get_schema()['sort']
        n_sort = len(schema_sort)
        def get_key(row):
            raw_sort_key = row[offset:(n_sort + offset)]
            sort_key = tuple()
            for i, key in enumerate(raw_sort_key):
                if schema_sort[i][1] == 'integer': sort_key += (int(key),)
                elif schema_sort[i][1] == 'float': sort_key += (float(key),)
                else: sort_key += (str(key),)
            return sort_key
        return get_key
    
    def _add_to_buffer(self, hash_keys, row):
        path = self.dbname
        for folder in hash_keys:
            path += "/%s" % folder
            if not os.path.exists(path): os.mkdir(path)
        path += '/buffer.csv'
        mode = 'a+' if os.path.exists(path) else 'a+'
        with open(path, mode) as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow(row)
        self.to_commit.add(hash_keys)
        f.close()
