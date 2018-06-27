from filebase import FileBase
from random import randint
from sys import argv

db = FileBase(verbose = False)

if len(argv) == 2:
    db.create('test-db', ['stop_id'], ['type', 'timestamp'])

    types = ['A', 'D', 'T']
    for i in range(100):
        for n in range(10):
            db.put(('station%s' % n, types[randint(0, len(types)-1)], randint(0, 100000)), randint(0, 50))

    db.put(('station2', 'D', 128), 23456)

    db.commit()

else:
    db.open('test-db')
    r = db.find(('station2', 'D'))
    for x in r: print(x)
