import mysql.connector as mariaDB
import csv
import sys

db_conn = mariaDB.connect(host='localhost', user='root', passwd="password")
Query = "SELECT * FROM mock_data_db.mock_data_table;"

cur = db_conn.cursor()
cur.execute(Query)
result = cur.fetchall()
print(result)

c = csv.writer(open('sqldumb.csv', 'w'))
for x in result:
    c.writerow(x)