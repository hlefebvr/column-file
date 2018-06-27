with open('./test.csv') as f:
    n = sum(len(x) for x in f)
    m = round(n/2)
    f.seek(m)
    f.readline()
    print(f.readline())