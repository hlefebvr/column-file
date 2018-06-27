import pickle
import os
from random import randrange
from queue import Queue, PriorityQueue, LifoQueue
from math import ceil
import shutil

class FileBase:
    # chunk_size in Mb
    def __init__(self, verbose = True, chunk_size_mb = 10):
        self.dbname = None
        self.verbose = verbose
        self.hash_keys = None
        self.sort_keys = None
        self.to_commit = set()
        self.chunk_size = chunk_size_mb * 10 ** 6
    
    def log(self, txt):
        if (self.verbose):
            print('(FileBase)> ', end = "")
            print(txt)

    def create(self, dbname, hash_keys, sort_keys):
        schema = { "hash": hash_keys, "sort": sort_keys }
        if os.path.exists(dbname): raise ValueError("A folder with this name already exists")
        # create main directory
        os.makedirs(dbname)
        # save db schema
        with open('%s/schema' % dbname, 'wb') as f: f.write(pickle.dumps(schema))
        self.log('%s FileBase created' % dbname)
        self.open(dbname)
    
    def open(self, dbname):
        self.dbname = dbname
        with open('%s/schema' % dbname, 'rb') as f:
            schema = pickle.loads(f.read())
            self.hash_keys = schema['hash']
            self.sort_keys = schema['sort']
    
    def find(self, keys): print('find')

    def put(self, key, value):
        str_keys = lambda keys : ','.join([str(k) for k in keys])
        n_hash = len(self.hash_keys)
        hash_keys = key[:n_hash]
        sort_keys = key[n_hash:]
        path = '%s/' % self.dbname
        for folder in hash_keys:
            path += '%s/' % folder
            if not os.path.exists(path): os.makedirs(path)
        str_hash_keys = str_keys(hash_keys)
        str_sort_keys = str_keys(sort_keys)
        str_value = str(value)
        with open('%s/buffer' % path, 'a+') as f: f.write( '%s,%s,PUT\n' % (str_sort_keys, str_value))
        self.to_commit.add(path)
        self.log('(((%s),%s),%s) was added to buffer, please commit' % (str_hash_keys,str_sort_keys, str_value))

    def commit(self):
        def get_key(row):
            keys = row.split(',')[:len(self.sort_keys)]
            keys[1] = int(keys[1])
            return tuple(keys)
        
        def split_operation_row(row):
            row = row[:-1] # remove \n
            row = row.split(',')
            return row.pop(), ','.join(row)
        
        for folder in self.to_commit:
            if not os.path.exists('%s/buffer' % folder):
                return self.log("Nothing to commit")
            
            # sort file of coming operations
            self.sort('%s/buffer' % folder)

            if not os.path.exists('%s/data' % folder): open('%s/data' % folder, 'w+').close()
            data = open('%s/data' % folder, 'r')
            operations = open('%s/buffer' % folder)
            f = open('%s/tmp' % folder, 'w+')

            def write(txt): f.write('%s\n' % txt)

            row_data, row_operations = data.readline(), operations.readline()
            eof_data, eof_operations = (row_data == ''), (row_operations == '')

            while not eof_operations or not eof_data:
                row_data = row_data[:-1]
                operation_type, row_operations = split_operation_row(row_operations)
                
                if eof_operations:
                    write(row_data)
                    row_data = data.readline()
                
                elif eof_data:
                    if operation_type == 'PUT':
                        write(row_operations)
                        row_operations = operations.readline()
                    else: raise ValueError('Unexpected operation %s' % operation_type)
                
                else:
                    if operation_type == 'PUT':
                        
                        if get_key(row_operations) == get_key(row_data):
                            write(row_operations)
                            row_operations = operations.readline()
                            row_data = data.readline()
                        
                        elif get_key(row_operations) > get_key(row_data):
                            write(row_operations)
                            row_operations = operations.readline()
                        
                        else:
                            write(row_data)
                            row_data = data.readline()

                    elif operation_type == 'DELETE':
                        
                        if get_key(row_operations) == get_key(row_data):
                            row_operations = operations.readline()
                            row_data = data.readline()
                        
                        else:
                            write(row_data)
                            row_data = data.readline()
                    
                    else: raise ValueError("Unknown operation type : %s" % row_operations[-1])
                
                eof_data, eof_operations = (row_data == ''), (row_operations == '')

            f.close()
            data.close()
            operations.close()
            os.remove('%s/data' % folder)
            os.remove('%s/buffer' % folder)
            os.rename('%s/tmp' % folder, '%s/data' % folder)

    def delete(self, key): print('delete')

    def close(self): print('close')

    # external sort
    def sort(self, mainfile):
        def row_to_tuple(row):
            row = row.split(',')
            row[1] = int(row[1])
            return tuple(row)
        def tuple_to_row(t): return ','.join(str(x) for x in t)
        class MemoryChunk:
            def __init__(self, max_size):
                self.count, self.size = 0, 0
                self.f_chunk = None
                self.in_memory_chunk, self.filenames = [], []
                self.new_chunk()
                self.max_size = max_size
            
            def new_chunk(self):
                if self.f_chunk is not None: self.f_chunk.close()
                self.count += 1
                filename = '%s.chunk.%s' % (mainfile, self.count)
                self.f_chunk = open(filename, 'w+')
                self.filenames.append(filename)
                self.size = 0
                self.in_memory_chunk = []
                
            def flush(self, end = False):
                to_write = ""
                for row in sorted(self.in_memory_chunk):
                    to_write += ','.join(str(x) for x in row)
                if to_write != '': self.f_chunk.write(to_write)
                else:
                    del self.filenames[self.count-1]
                    os.remove('%s.chunk.%s' % (mainfile, self.count))
                if not end: self.new_chunk()
                else: self.f_chunk.close()
            
            def add(self, row):
                self.size += len(row)
                row = row.split(',')
                row[1] = int(row[1]) # to do manage different schemas
                self.in_memory_chunk.append(row)
                if self.size >= self.max_size: self.flush()
        
        # split big file into smaller sorted chunk
        with open(mainfile) as f:
            memory_chunk = MemoryChunk(self.chunk_size)
            for row in f: memory_chunk.add(row)
            memory_chunk.flush(True)
        
        # k-way merge
        way = []
        fd = open('%s.sorted' % mainfile, 'w+')
        for filename in memory_chunk.filenames:
            f = open(filename, 'r')
            way.append( [f, f.readline()])
        while len(way) > 0:
            argmin = None
            for i, f in enumerate(way):
                _f, row = f
                if argmin is None or row_to_tuple(row) < row_to_tuple(way[argmin][1]): argmin = i
            fd.write(way[argmin][1])
            new_row = way[argmin][0].readline()
            if new_row == '':
                way[argmin][0].close()
                del way[argmin]
            else: way[argmin][1] = new_row
        for filename in memory_chunk.filenames: os.remove(filename)
        os.remove(mainfile)
        os.rename('%s.sorted' % mainfile, mainfile)
