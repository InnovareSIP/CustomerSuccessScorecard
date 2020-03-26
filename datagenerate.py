import mysql.connector as mariaDB
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import argparse
import csv
import os
import configparser
import sys
import glob
import fnmatch
from utils import bqueries, scorecard
import datetime

#Get BigQuery credentials required to connect to the dataset and run the script
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./config/bqconfig.json"


#Get database credenatials required to connect to local and staging databases
def get_creds(env):
    try:
        config = configparser.ConfigParser()
        config.read("./config/dbconfig.ini")
        creds = (dict(config.items(env)))
        return creds
    except:
        print('Could not find dbconfig.ini')

#Conect to database, use credentials from dbconfig.ini
def connect_data(db_name, creds):
    try: 
        db_conn = mariaDB.connect(host=creds['host'], user=creds['user'], passwd=creds['passwrd'])
        print(f'Connetected to {db_name} sucessfully')
    except:
        print(f'Could not connect to {db_name}')
    else:    
        cur = db_conn.cursor()
        copy_tables(cur,db_name)

#execute on database that is connected    
def get_data(query, cursor):
    cursor.execute(query)
    res = cursor.fetchall()
    return res

#Get the names of the connected database and create a list of them
def get_table_names(cursor, db_name):
    query = f'SHOW TABLES from {db_name}'
    cursor.execute(query)
    tablenames = [i[0] for i in cursor.fetchall()]
    return tablenames

#make csv files from connected database and output them to ./output
def make_file(res,cursor,table):
    col_names = [i[0] for i in cursor.description]
    tablename = f'{table}.csv'
    output_file = csv.writer(open(f'./export/{tablename}', 'w', newline=''))
    output_file.writerow(col_names)
    for row in res:
        output_file.writerow(row)

#Copy tables from ./output folder and send them to a BigQuery dataset, specified my db_name
def copy_tables(cur,db_name):
    tablenames = get_table_names(cur,db_name)
    for table in tablenames:
        query = f'SELECT * FROM {db_name}.{table}'
        make_file(get_data(query, cur), cur, table)

#configure the BigQuery job that will be created the dataset on tables
def sendtobq(table, db_name, location):
    try:
        client = bigquery.Client()
    except:
        print("Could not connect to Big Query")
        sys.exit()
    else:
        print("Connected to Big Query successfully")
        filename = f'./{location}/{table}.csv'
        dataset_id = f'{client.project}.{db_name}'
        table_id = table
        dataset = bigquery.Dataset(dataset_id)
        try:
            client.get_dataset(dataset_id)
            print(f'Dataset {dataset_id} exists: inserting data')
        except NotFound:
            print(f'Dataset {dataset_id} is not found:creating dataset')
            dataset = client.create_dataset(dataset)
        
        table_ref = dataset.table(table_id)
        job_config = bigquery.LoadJobConfig()
        job_config.autodetect = True
        job_config.skip_leading_rows = 1
        job_config.max_bad_records = 10
        job_config.allow_quoted_newlines = True
        #This setting accounts for a csv file that uses the string NULL for null values
        # job_config.null_marker = "NULL"
        job_config.qoute = ""
        job_config.source_format = bigquery.SourceFormat.CSV
        with open(filename, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
        try:
            job.result()
        except:
            print(job.errors)  
        print(f'Loaded {job.output_rows} rows into {dataset_id}:{table_id}.')

#Configues and send the SQL queries from th./utils directory to BigQuery
def send_query(query,db_name,dataset,func,date):
    client = bigquery.Client()
    table_id = f"{client.project}.{db_name}.{func}_{date}"
    job_config = bigquery.QueryJobConfig(destination=table_id)
    query_job = client.query(query,job_config=job_config)
    results = query_job.result()

#Configures the job for BigQuery to take the SQL query to create the scorecard
#Will append to a table if a tablename is given, else it will create a new table
def send_scorecard(query,date,dataset=None, tablename=None):
    client = bigquery.Client()
    dataset_id = f'{client.project}.{dataset}'
    #check for supplied agruments and create the appropriate table and/or dataset
    if tablename and dataset:
        table_id = f"{client.project}.{dataset}.{tablename}"
    elif dataset:
        table_id = f"{client.project}.{dataset}.scorecard_{date}"
    elif tablename:
        table_id = f"{client.project}.score_card_result_{date}.{tablename}"
    else:
        dataset_id = f'{client.project}.score_card_result_{date}'
        table_id = f"{client.project}.score_card_result_{date}.scorecard_{date}"
    dataset = bigquery.Dataset(dataset_id)
    job_config = bigquery.QueryJobConfig(
        destination=table_id,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
    try:
        client.get_dataset(dataset_id)
        print(f'Dataset {dataset_id} exists: inserting data')
    except NotFound:
        print(f'Dataset {dataset_id} is not found:creating dataset')
        dataset = client.create_dataset(dataset)
    query_job = client.query(query,job_config=job_config)
    if(query_job.errors):
        print(query_job.errors)
    print(f'Loaded rows into {dataset_id}:{table_id}.')

#export csv files from ./export directory to BigQuery
def export(dataset):
    for f in os.listdir('./export'):
        if fnmatch.fnmatch(f, '*.csv'):
            f = os.path.splitext(f)[0]
            sendtobq(f,dataset,"export")

def main():
    #define command line arguments the script will accept
    parser = argparse.ArgumentParser(description="CL app to connect MySQL to BigQuery")
    group1 = parser.add_argument_group("Send scorecard", "Only the scorecard argument is required, omiting the dataset argument will create a new dataset for the scorecard result table, omiting the table argument will create a new table with a timestamp as its name")
    group3 = parser.add_argument_group("Export files","export csv files from the ./export directory.")
    group2 = parser.add_argument_group("Database Connection", "Specify an enviroment:local, staging, production and a database to copy from")
    group1.add_argument("-dataset", help="Query function to call on dataset")
    group1.add_argument("-table", help="name of table to send scorecard results")
    group1.add_argument("-scorecard", help="dataset to run query on")
    group2.add_argument("-copy", help="Name of the enviroment you would like to copy from")
    group2.add_argument("-db", help="Name of the database you would like to copy")
    group3.add_argument("-export", help="Export files in ./export to dataset. Specify the dataset to send the data.")
    # parser.add_argument("-q", help="Query function to call on dataset")
    args = parser.parse_args()
    #get todays date to add to final scorecard table
    date = datetime.datetime.today()
    #formats the date for BigQuery
    date = date.strftime("%b_%d_%Y_%H_%M")
    #Check for command line arguments
    if args.db and args.copy:
        credentials = get_creds(args.copy)
        if(credentials):
            connect_data(args.db, credentials)
    elif args.export:
        export(args.export)
    elif args.scorecard:
        query = scorecard.get_scorecard(args.scorecard)
        send_scorecard(query,date, args.dataset, args.table)
    

if __name__=="__main__":
    main()
