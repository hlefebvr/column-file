from ColumnFile import ColumnFile

db = ColumnFile(verbose = True)

db.create("test-db", {
    "hash": [ ("station", "string") ],
    "sort": [ ("type", "string"), ("timestamp", "number") ]
})

db.put(("station1", "A", 3), { "key": "value" })
db.merge(("station1", "A", 3), { "key": "value" })
db.delete(("station1", "A", 3))

db.commit()
