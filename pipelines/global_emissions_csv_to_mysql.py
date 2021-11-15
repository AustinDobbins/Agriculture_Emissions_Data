import mysql.connector
import csv

current_columns = []
rows = []
years = []
# connect to original csv
with open('/mnt/c/Users/dobbi/desktop/emissions/data/global_emissions_by_country.csv') as csv_file:
    csv_reader = csv.reader(csv_file)
    
    for i, line in enumerate(csv_reader):
# creates current_columns list from first line in csv_file
        if i == 0:
            current_columns = line
# adds row to rows from lines 2 onward in the csv_file
        if i != 0:
            rows.append(line)

# fills years list 
for i, column in enumerate(current_columns[5:]):
    years.append(int(column))

# finds all countries with missing data points
countries_to_yeeet = []
line_yeeeted = []
for i, r in enumerate(rows):
    for column in r:
        if column.lower() == 'n/a' and r[0] not in countries_to_yeeet:
            countries_to_yeeet.append(r[0])

# creates structure for modified table 
new_columns = ('country', 'data_source', 'sector', 'gas', 'unit', 'total_emissions', 'year_emitted')
new_rows = []
structured_rows = []

# adds new row to new_rows if the data doesnt have null entries
for r in rows:
    if r[0] not in countries_to_yeeet:
        new_rows.append(r)

# restructures rows to accomplish having a row entry to MySQL table for each year of data
# this makes working in tableu easier than having 1 entry per country with a column for each year 
for nr in new_rows:
    for i, total_emissions in enumerate(nr[5:]):
        structured_rows.append((nr[0], nr[1], nr[2], nr[3], nr[4], total_emissions, years[i]))

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
query = 'INSERT INTO ' + 'global_totals' + str(tuple(new_columns)).replace("'", "") + ' VALUES '
for sr in structured_rows:
    mycursor.execute(query + str(tuple(sr)))
    print(str(rows_inserted) + ' rows inserted.')
    rows_inserted += 1
mydb.commit() 