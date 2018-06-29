from math import ceil
from io import StringIO
import os, csv

default_get_key = lambda row : row[0]

class CSVLocalAlgorithm:
    def sort(self, filename, output_filename, get_key = default_get_key, chunk_size_mb = 10):
        folder = os.path.dirname(filename)
        # helper class which accumulates in memory data up to max_size byte
        # then sorts resulting rows and writes them to a new chunk file
        class ChunkSorter:
            def __init__(self, max_size):
                self.chunk_count = 0
                self.size, self.max_size = 0, max_size
                self.f_chunk, self.in_memory_chunk = None, []
                self.new_chunk()
            def new_chunk(self):
                self.chunk_count += 1
                if self.f_chunk is not None: self.f_chunk.close()
                self.f_chunk = open(self._filename(self.chunk_count), 'w+')
                self.size = 0
                del self.in_memory_chunk[:]
            def flush(self, end = False):
                if len(self.in_memory_chunk) > 0:
                    csv_writer = csv.writer(self.f_chunk)
                    for row in sorted(self.in_memory_chunk, key = get_key): csv_writer.writerow(row)
                elif end:
                    self.f_chunk.close()
                    os.remove(self._filename(self.chunk_count))
                if not end: self.new_chunk()
                else: self.f_chunk.close()
            def add(self, row):
                self.size += len(str(row))
                self.in_memory_chunk.append(row)
                if self.size >= self.max_size: self.flush()
            def get_filenames(self): return [ self._filename(i) for i in range(1, self.chunk_count + 1) ]
            def _filename(self, i): return "%s/chunk.%s.csv" % (folder, str(i))
        
        # split bug file into smaller sorted ones
        with open(filename, 'r') as f:
            reader = csv.reader(f)
            chunk_sorter = ChunkSorter(chunk_size_mb * (10 ** 6))
            for row in reader: chunk_sorter.add(row)
            chunk_sorter.flush(True)

        # k-way merge
        self.k_way_merge(chunk_sorter.get_filenames(), output_filename, get_key)
    
    def binary_search(self, filename, sort_keys, get_key = default_get_key, first_occurence = False):
        with open(filename, 'r') as f:
            a = a_end = 0
            f.seek(0,2) # got to end of file
            b = b_end = f.tell()
            c, c_old = 0, -1

            # while a < b:
            while c != c_old:
                c_old = c
                c = round( (a+b)/2 )
                f.seek(c)

                # got to begining of line
                while f.read(1) != '\n':
                    c -= 1
                    if c > 0: f.seek(c)
                    else:
                        f.seek(0)
                        break
                
                c = f.tell()
                csv_row = StringIO(f.readline())
                c_end = f.tell()
                csv_reader = csv.reader(csv_row)
                try: row = next(csv_reader)
                except: return c_end

                key = get_key(row)
                if sort_keys < key: b = c
                elif sort_keys > key: a = c_end
                elif not first_occurence: return c
                else: b = c_end # go find first occurence

        return c
    
    def k_way_merge(self, filenames, output_filename, get_key = default_get_key):
        way = list()
        with open(output_filename, 'w+') as merged_f:
            csv_writer = csv.writer(merged_f)
            # initialize ways with first row
            for filename in filenames:
                f = open(filename, 'r')
                csv_reader = csv.reader(f)
                try: row = next(csv_reader)
                except: continue
                way.append( [f, csv_reader, row] )
            # as long as there are ways
            while len(way) > 0:
                argmin, minimum = None, None
                for i, curr_way in enumerate(way):
                    _, csv_reader, row = curr_way
                    key = get_key(row)
                    if argmin is None or minimum > key:
                        argmin, minimum = i, key
                csv_writer.writerow(way[argmin][2])
                try: way[argmin][2] = next(way[argmin][1])
                except StopIteration: # we are at the end of the file
                    way[argmin][0].close()
                    del way[argmin]
        for filename in filenames: os.remove(filename)
