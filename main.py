from ColumnFile import ColumnFile
from random import randint

db = ColumnFile()

db.create("test-db", {
    "hash": [ ("station", "string") ],
    "sort": [ ("type", "string"), ("timestamp", "integer") ]
})

types = ['A', 'D', 'T']

for i in range(10):
    for n in range(10):
        key = ("station%s" % str(i), types[randint(0,2)], randint(0, 10000))
        db.merge(key, { "number": n })
        db.merge(key, { "number2": n+1 })
db.put(('station0', 'A', 0), { "value": True })
db.commit()

r = db.scan()

for x in r: print(x)
