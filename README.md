# DataGenerate
Automate process to take a SQL file to generate a CSV file and upload it to an external site 
# Requirements
Python 3.8
# Installation
In the scripts directory run

pip3 install -r requirements.txt

this will install all required modules for the script to run

Obtain your Big Query service account keys by following this guide and save it to your local machine.
https://cloud.google.com/bigquery/docs/quickstarts/quickstart-client-libraries
Rename the file to bqconfig.json and move it to the /config folder of this scripts directory

Create a dbconfig.ini in the /config directory. Edit the file to include the correct host, user, and password of the databases you would like to connect to.

You only need to configure the databases you would like to connect to ie if you would only like to connect only to a local database this file only needs to the \[local\] section to be configured. 

Your dbconfig file should look as follows:

\[local\]<br/>
host=local_host<br/>
user=local_user<br/>
passwrd=local_password<br/>

\[staging\]<br/>
host=staging_host<br/>
user=staging_user<br/>
passwrd=staging_password<br/>

\[production\]<br/>
host=production_host<br/>
user=production_username<br/>
passwrd=production_password<br/>

Your config folder should now contain a dbconfig.ini file and a bqconfig.json file

# Running the script
The script takes the following arguments:

-h or --help will display the following msg on the command line and exit

Send scorecard:
  Only the scorecard argument is required, omiting the dataset argument will create a new dataset for the scorecard
  result table, omiting the table argument will create a new table with a timestamp as its name

  -dataset DATASET      name of dataset to create/send scorecard results
  -table TABLE          name of table to create/send scorecard results
  -datasource DATASOURCE
                        dataset to run query on

Export files:
  export csv files from the ./export directory.

  -bqimport BQIMPORT    Export files in ./export to dataset. Specify the dataset to send the data.

Database Connection:
  Specify an enviroment:local, staging, production and a database to copy from

  -copyfrom COPYFROM    Name of the enviroment you would like to copy from
  -database DATABASE    Name of the database you would like to copy

Example:
python3 datagenerate.py -datasource inno_db_export -dataset score_card_results -table results

This would query the inno_db_export dataset in your BigQuery project and create a dataset named score_card_results and create a table names results in that dataset