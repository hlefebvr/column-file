from ColumnFile import ColumnFile
from random import randint

db = ColumnFile(verbose = True)

db.create("test-db", {
    "hash": [ ("station", "string") ],
    "sort": [ ("type", "string"), ("timestamp", "integer") ]
})

types = ['A', 'D', 'T']

for i in range(10):
    for n in range(1000):
        key = ("station%s" % str(i), types[randint(0,2)], randint(0, 10000))
        db.merge(key, { "number": n })
db.put(('station0', 'A', 1900), { "value": True })
db.commit()

r = db.find(('station0', 'T', 34))

print(r)
