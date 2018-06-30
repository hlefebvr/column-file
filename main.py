from ColumnFile import ColumnFile
from random import randint
import numpy as np

db = ColumnFile(verbose = True)

# create the database
db.create("demo-db", {
    "hash": [ ("year", "integer") ],
    "sort": [ ("month", "integer"), ("day", "integer") ]
})

# open the existing database
db.open("demo-db")

# let's add some data
for year in range(2012, 2018):
    for month in range(1, 13):
        for day in range(1, 32):
            operation_type = ['sell', 'buy'][randint(0,1)]
            amount = randint(1, 500)
            db.put((year, month, day), { "operation_type": operation_type, "amount": amount })

# change a specific column value
db.merge((2016, 6, 23), { "operation_amount": 250 })

# do NOT forget to commit
db.commit()

# retrieve one specific row and print its operation type
key, values = db.get((2016, 6, 23))
print(values['operation_amount'])

# compute average sell amount in 2016
results = db.scan(2016, row_filter = lambda row: row[1]['operation_type'] == 'sell')
sum_amount, n = sum(map(lambda row : np.array([row[1]['amount'], 1]), results))
average = sum_amount / n
print('Average = ', average)
