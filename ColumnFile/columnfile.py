from .LocalColumnFile import LocalColumnFile
import os

class ColumnFile:

    # Class constructor
    def __init__(self, verbose = False):
        self.manager = LocalColumnFile(verbose)
        self.manager.log("[OK] Column-File session started")
    
    # Creates a new db
    # dbname : path to new db
    # schema : dict containing two keys : hash and sort
    #           - hash : list of couples (attribute_name, attribute_type)
    #           - sort : list of couples (attribute_name, attribute_type)
    #          attribute_type is either "string" or "number"
    def create(self, dbname, schema):
        # dbname check
        if type(dbname) is not str:
            self.manager.log("[ERROR] dbname must be a string")
            return;
        if os.path.exists(dbname):
            self.manager.log("[ERROR] a database with this name already exists")
            return;
        # schema check
        if type(schema) is not dict:
            self.manager.log("[ERROR] schema attribute must be a dict")
            return;
        if schema.keys() in ['hash', 'sort']:
            self.manager.log("[ERROR] schema must contain 'hash' and 'sort' key definitions")
            return;
        def is_valid_key_definition(definition):
            if type(definition) is not list: return False
            for key_specification in definition:
                if len(key_specification) != 2: return False
                if key_specification[1] not in ['integer', 'float', 'string']: return False;
            return True
        for key_type in ['hash', 'sort']:
            if not is_valid_key_definition(schema[key_type]):
                self.manager.log("[ERROR] %s key definition must be a list of (attribute_name, attribute_type)" % key_type)
                return;
        # we can safely create the db
        return self.manager.create(dbname, schema)
    
    # Opens existing db
    # dbname : path to existing db
    def open(self, dbname):
        # dbname check
        if type(dbname) is not str:
            self.manager.log("[ERROR] dbname must be a string")
            return;
        if not os.path.exists(dbname):
            self.manager.log("[ERROR] Cannot find database")
        # we can safely open the db
        return self.manager.open(dbname)
    
    # Returns columns associated to the key or None if key does not exists
    # key : hash + sort key
    def get(self, key, report_error = False):
        try: hash_keys, sort_keys = self.manager._split_key(key)
        except:
            self.manager.log("[ERROR] provided key does not match schema.\n\
            get operation requires a complete key (hash + sort) matching db schema")
            return;
        return self.manager.get(hash_keys, sort_keys, report_error)

    # Returns an iterator of data associated to the sub key or None if nothing is found
    # sub_key : hash + sub_sort_key or sub_hash
    # fileter : lambda function to filter the results
    # (note that the function will still enumerate data 
    # but will return only those matching the filter criteria)
    def scan(self, sub_key = None, row_filter = lambda _: True):
        if type(sub_key) != tuple: sub_key = (sub_key,)
        try: hash_keys, sort_keys = self.manager._split_key(sub_key, complete=False)
        except ValueError as e:
            print(e)
            self.manager.log("[ERROR] provided key does not match schema\n\
            scan operation requires sub key (hash + sort) match key order")
            self.manager.log("[ERROR] %s" % e)
            return;
        return self.manager.scan(hash_keys, sort_keys, row_filter)
    
    # Updates data associated to key,
    # if data exists, columns are merged with existing ones
    # if data does not exist, new entry is created
    # key : hash + sort key
    # column_values : dict with column names: values
    def merge(self, key, column_values):
        try: hash_keys, sort_keys = self.manager._split_key(key)
        except ValueError as e:
            self.manager.log("[ERROR] provided key does not match schema\n\
            merge operation requires a complete key (hash + sort) matching db schema")
            self.manager.log("[ERROR] %s" % e)
            return;
        return self.manager.merge(hash_keys, sort_keys, column_values)
    
    # Updates data associated to key,
    # if data exists, columns are replaced by those provided in parameter
    # if data does not exist, new entry is created
    # key : hash + sort key
    # column_values : dict with column names: values
    def put(self, key, column_values):
        try: hash_keys, sort_keys = self.manager._split_key(key)
        except ValueError as e:
            self.manager.log("[ERROR] provided key does not match schema\n\
            put operation requires a complete key (hash + sort) matching db schema")
            self.manager.log("[ERROR] %s" % e)
            return;
        return self.manager.put(hash_keys, sort_keys, column_values)
    
    # Deletes data
    # key : hash + sort key
    def delete(self, key):
        try: hash_keys, sort_keys = self.manager._split_key(key)
        except:
            self.manager.log("[ERROR] provided key does not match schema\n\
            delete operation requires a complete key (hash + sort) matching db schema")
            self.manager.log("[ERROR] %s" % e)
            return;
        return self.manager.delete(hash_keys, sort_keys)
    
    # Commits changes to db
    def commit(self): return self.manager.commit()
    
    # Returns db schema
    def get_schema(self): return self.manager.get_schema()
    
    # "Private"
    # Returns hash key and sort key from complete key
    # Throws exception if key does not match schema
    # keys : complete key tuple
    # complete : if true, key is expected to be complete
    # def manager._split_key(self, keys, complete = True):
    #     schema = self.get_schema()
    #     if keys == None or len(keys) == 0: return tuple(), tuple()
    #     n_hash, n_sort, n_key = len(schema['hash']), len(schema['sort']), len(keys)
    #     if n_key > n_hash + n_sort:
    #         self.manager.log("[ERROR] provided key is too big")
    #         return;
    #     schema = (schema['hash'] + schema['sort'])[:n_key]
    #     error = False
    #     for value, specification in zip(keys, schema):
    #         _, expected_type = specification
    #         actual_type = type(value)
    #         if expected_type == 'number' and actual_type not in [int, float]: error = True
    #         if expected_type == 'string' and actual_type != str: error = True
    #         if error:
    #             msg = "[ERROR] provided key does not match schema\n\
    #                 %s expected to be %s" % (str(keys), actual_type)
    #             self.manager.log(msg)
    #             raise ValueError(msg)
    #     if n_hash < n_key: return tuple(keys[:n_hash]), tuple(keys[n_hash:])
    #     return tuple(keys[:n_hash]), tuple()