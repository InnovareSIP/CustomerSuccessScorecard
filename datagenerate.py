import mysql.connector as mariaDB
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import argparse
import csv
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:/Users/crodr/Downloads/bqKeys.json"

def getData(query, cursor):
    cursor.execute(query)
    res = cursor.fetchall()
    return res

def makeFile(res,cursor,table):
    col_names = [i[0] for i in cursor.description]
    tablename = table +  ".csv"
    output_file = csv.writer(open(tablename, 'w', newline=''))
    output_file.writerow(col_names)
    for row in res:
        output_file.writerow(row)

def connectData(dbName):
    db_conn = mariaDB.connect(host='localhost', user='root', passwd="password")
    cur = db_conn.cursor()
    copyTables(cur,dbName)

def copyTables(cur,dbName):
    tablenames = getTableNames(cur,dbName)
    count = 1
    for table in tablenames:
        print(count)
        count +=1
        query = "SELECT * FROM " + dbName + "." + table
        makeFile(getData(query, cur), cur, table)
        sendtobq(table,dbName)


def getTableNames(cursor, dbName):
    query = "SHOW TABLES from " + dbName
    cursor.execute(query)
    tablenames = [i[0] for i in cursor.fetchall()]
    return tablenames

def sendtobq(table, dbName):
    client = bigquery.Client()
    filename = './' + table + ".csv"
    dataset_id = "{}.{}".format(client.project,dbName)
    table_id = table
    dataset = bigquery.Dataset(dataset_id)
    try:
        client.get_dataset(dataset_id)  # Make an API request.
        print("Dataset {} already exists".format(dataset_id))
    except NotFound:
        print("Dataset {} is not found".format(dataset_id))
        dataset = client.create_dataset(dataset)
    table_ref = dataset.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    # job_config.skip_leading_rows = 1
    job_config.autodetect = True
    with open(filename, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
    job.result()  # Waits for table load to complete.
    print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))

def main():
   
    parser = argparse.ArgumentParser()
    parser.add_argument("dbName", help="Name of the database you would like to copy")
    args = parser.parse_args()
    connectData(args.dbName)

if __name__=="__main__":
    main()
