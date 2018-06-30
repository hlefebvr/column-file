from ColumnFile import ColumnFile
from random import randint

db = ColumnFile(verbose = True)

# create the database
db.create("demo-db", {
    "hash": [ ("year", "integer"), ("month", "integer") ],
    "sort": [ ("day", "integer"), ("timestamp", "float") ]
})

# open the existing database
db.open("demo-db")

# let's add some data
# for year in range(2012, 2018)