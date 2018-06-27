from math import ceil, fabs

# returns (index, values) if found
# returns (index, None) if not found
# where index is the index where the object
# should have been in order to keep the list sorted
def find(stop_id, stop_type, timestamp):
    csv_path = './test.csv'
    timestamp = str(timestamp)
    key = (stop_id, stop_type, timestamp)

    with open(csv_path,'r') as f:
        a = 0
        b = sum(len(row) for row in f)
        c_old = None
        c = ceil( (a+b) / 2 )
        
        # while c != c_old:
        while fabs( a - b ) >= 1:
            
            c_old = c
            c = ceil( (a+b) / 2 ) # center of interval
            
            # Go to then center of the interval
            # However, we are somewhere on the line
            # We need to find the begining of the line
            f.seek(c)
            char = f.read(1)
            while char != '\n':
                c -= 1
                if c <= -1: break
                f.seek(c)
                char = f.read(1)
            f.seek(c+1)
            
            # Let's apply binary search
            line = f.readline().split(',')
            curr_key = tuple(line[:3])
            if (key < curr_key): b = c
            elif (key > curr_key): a = c
            else:
                line.append(int(line.pop()))
                return (c + 1, tuple(line))

        return (c, None)

r = find("stop_1","departure",1)

print(r)
