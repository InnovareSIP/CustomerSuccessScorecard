# DataGenerate
Automate process to take a SQL file to generate a CSV file and upload it to andexternal site 
# Requirements
Python 3.8
# Installation
In the scripts directory run

pip3 install -r requirements.txt

this will install all required modules for the script to run

Obtain your Big Query service account keys by following this guide
https://cloud.google.com/bigquery/docs/quickstarts/quickstart-client-libraries
rename file to bqkeys.json and move to the config folder of this scripts directory

Create a db.ini and edit it to include the following information and configure it to connect to your database
\[local\]
host=
user=
passwrd=

\[staging\]
host=
user=
passwrd=

\[production\]
host=
user=
password=
# Running the script
The script takes you arguments: \[name of the enviroment you want to connect to\] \[name of database to copy\]

\> py datagenerate.py local mock_data_db

This would copy all tables from your local mock_data_db and upload them to big query inside of the dataset mock_data_db