import mysql.connector
import csv

current_columns = []
current_rows = []
with open('/mnt/c/Users/dobbi/desktop/emissions/data/FAOSTAT_data.csv') as csv_file:
    csv_reader = csv.reader(csv_file)

    for i, line in enumerate(csv_reader):
# creates current_columns list from first line in csv_file
        if i == 0:
            current_columns = line
# adds row to rows from lines 2 onward in the csv_file
        if i != 0:
            current_rows.append(line)

# creates new table structure
columns = ['country', 'gas', 'cause', 'yr_recorded', 'unit', 'total']
rows = []

for cr in current_rows:
    if cr[3].lower() == 'united states of america':
        rows.append((cr[3], cr[5], cr[7], cr[9], cr[12], cr[13]))

# connects python script to MySQL db
mydb = mysql.connector.connect(
    host = 'localhost',
    user = 'root',
    passwd = '',
    database = 'emissions'
)
mycursor = mydb.cursor()
# creates a query to import restructured data in proper format to MySQL db
rows_inserted = 1
query = 'INSERT INTO ' + 'fao' + str(tuple(columns)).replace("'", "") + ' VALUES '
for r in rows:
    mycursor.execute(query + str(tuple(r)))
    print(str(rows_inserted) + ' rows inserted.')
    rows_inserted += 1
mydb.commit() 