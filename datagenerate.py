import mysql.connector as mariaDB
from google.cloud import bigquery
import argparse
import csv
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:/Users/crodr/Downloads/bqKeys.json"

def getData(Query, cursor):
    cursor.execute(Query)
    res = cursor.fetchall()
    return res

def makeFile(res, cursor):
    col_names = [i[0] for i in cursor.description]
    output_file = csv.writer(open('sqldump.csv', 'w', newline=''))
    output_file.writerow(col_names)
    for x in res:
        output_file.writerow(x)
    sendtobq()

def connectData(file):
    db_conn = mariaDB.connect(host='localhost', user='root', passwd="password")
    fd = open(file, 'r')
    Query = fd.read()
    fd.close()
    print(type(Query))
    cur = db_conn.cursor()
    makeFile(getData(Query, cur), cur)   

def sendtobq():
    client = bigquery.Client()
    filename = './sqldump.csv'
    dataset_id = 'Test_set'
    table_id = 'Test_table'
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
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
    parser.add_argument("fileName", help="Name of the SQL file you would like to run")
    args = parser.parse_args()
    connectData(args.fileName)

if __name__=="__main__":
    main()
