from time import time
import os, csv, json

from .CSVExternalAlgorithm import CSVLocalAlgorithm
from .logger import Logger
from queue import Queue

class LocalColumnFile:

    def __init__(self, verbose):
        self.dbname = None
        self.schema = None
        self.to_commit = set()
        self.verbose = verbose
        self.logger = Logger(verbose)
        self.algos = CSVLocalAlgorithm()
    
    def log(self, msg): return self.logger.log(msg)

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
    
    def get(self, hash_keys, sort_keys, report_error):
        def fail():
            error_msg = "get operation did not match %s" % str(hash_keys + sort_keys)
            self.logger.log("[WARN] %s" % error_msg)
            if not report_error: return hash_keys + sort_keys + ({},)
            else: raise ValueError(error_msg)
        
        get_key = self._get_function_get_key()

        # check hash key exists
        path = self.dbname
        for folder in hash_keys:
            path += "/%s" % folder
            if not os.path.exists(path): return fail()
        
        # check partition has data to be looked for
        data_file = "%s/data.csv" % path
        if not os.path.exists(data_file): return fail()

        # binary search
        index = self.algos.binary_search(data_file, sort_keys, get_key)

        # move to line where the data SHOULD be if it were present
        # and read result
        with open(data_file, 'r') as f:
            f.seek(index)
            csv_reader = csv.reader(f)
            try: row = next(csv_reader)
            except: return fail()
        
        # check result is what we expected
        row = self._parse_row(row)
        if get_key(row) == sort_keys: return (hash_keys + sort_keys,) + (row[-1],)
        return fail()
    
    def _parse_row(self, row):
        get_key = self._get_function_get_key()
        sort_keys = get_key(row)
        value = json.loads(row[len(sort_keys)])
        return sort_keys + (value,)
    
    def scan(self, sub_hash_keys, sub_sort_keys, row_filter):
        class RowIterator:
            def __init__(self, root, binary_search, split_key, get_key):
                self.root = root
                self.hash_keys = Queue()
                self.hash_keys.put(sub_hash_keys)
                self.sub_sort_keys = sub_sort_keys
                self.filter = row_filter
                self.f = None
                self.csv_reader = None
                self.current_sub_hash_key = None
                self.split_key = split_key
                self.get_key = get_key
                self.binary_search = binary_search
            
            def next_hash(self):
                # if there are no more hash keys to be explored, end of iterator
                if self.hash_keys.empty(): raise StopIteration
                
                # let's take the next hash key
                next_sub_hash_key = self.hash_keys.get()
                next_folder = self.root
                for key in next_sub_hash_key: next_folder += "/%s" % str(key)
                data_path = "%s/data.csv" % next_folder

                # if the folder contains a data.csv file
                # we need to enumerate its rows (with respect to the sort keys) 
                if os.path.exists(data_path):
                    if self.f is not None: self.f.close()
                    
                    # binary search to the sort key
                    index = self.binary_search(data_path, self.sub_sort_keys, self.get_key)
                    
                    # let's open the file at the right position
                    self.f = open(data_path, 'r')
                    self.csv_reader = csv.reader(self.f)
                    self.f.seek(index)
                    self.current_sub_hash_key = next_sub_hash_key

                    return;
                
                # otherwise, list subfolders
                sub_folders = os.listdir(next_folder)

                # if there are any subfolders, we need to explore them
                if len(sub_folders) > 0:
                    # add every subfolder to exploration queue
                    for sub_folder in sub_folders:
                        if not os.path.isdir("%s/%s" % (next_folder, sub_folder)): continue
                        h, s = self.split_key( next_sub_hash_key + (sub_folder,) )
                        self.hash_keys.put(h)
                
                return self.next_hash()
            
            def __iter__(self):
                self.next_hash() # loads first partition
                return self;
            
            def __next__(self):
                # read the next line
                try: value = next(self.csv_reader)
                # if it fails, load next partition
                except StopIteration:
                    self.next_hash()
                    return self.__next__()
                
                # let's build the resulting current row
                complete_row = tuple(self.current_sub_hash_key) + tuple(value)
                value = json.loads(complete_row[-1])
                complete_key = complete_row[:-1]
                hash_key, sort_key = self.split_key(complete_key)
                sort_key = self.get_key(sort_key) # get_key does cast key columns
                obj_to_return = (hash_key + sort_key,) + (value,)

                # if we stop being accurate with respect with sort key
                # it means that we are done with this partition
                if sort_key[:len(self.sub_sort_keys)] != self.sub_sort_keys: raise self.next_hash()
                
                # if the current row passes filter, return it
                # otherwise skip to next row
                if self.filter(obj_to_return): return obj_to_return
                else: return self.__next__()
        
        return RowIterator(self.dbname, self.algos.binary_search, self._split_key, self._get_function_get_key())
    
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
            folder_path = self.dbname
            for key in hash_key: folder_path += '/%s' % str(key)
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
            curr_row.commit()

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

    def _split_key(self, keys, complete = True):
        schema = self.get_schema()
        if keys == None or len(keys) == 0: return tuple(), tuple()
        n_hash, n_sort, n_key = len(schema['hash']), len(schema['sort']), len(keys)
        if n_key > n_hash + n_sort:
            self.log("[ERROR] provided key is too big")
            return;
        schema = (schema['hash'] + schema['sort'])[:n_key]
        error = False
        for value, specification in zip(keys, schema):
            _, expected_type = specification
            actual_type = type(value)
            if expected_type == 'number' and actual_type not in [int, float]: error = True
            if expected_type == 'string' and actual_type != str: error = True
            if error:
                msg = "[ERROR] provided key does not match schema\n\
                    %s expected to be %s" % (str(keys), actual_type)
                self.log(msg)
                raise ValueError(msg)
        if n_hash < n_key: return tuple(keys[:n_hash]), tuple(keys[n_hash:])
        return tuple(keys[:n_hash]), tuple()
