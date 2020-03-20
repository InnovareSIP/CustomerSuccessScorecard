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
The script requires two arguments in order to copy tables on to bigquery: --evnName \[name of the enviroment you want to connect to\] --dbName \[name of database to copy\]<br/>
Example:<br/>
\> python3 datagenerate.py --envlocal local --dbName mock_data_db

This would copy all tables from your local mock_data_db and upload them to big query inside of the dataset mock_data_db. Local CSV files would be saved to the /output folder of the scripts directory

The script can also send gener to an existing bigquery dataset, arguments required, --sc \[name of dataset to send results to\] --dataset \[name of dataset to query\]

Example:
python3 datagenerate.py --sc score_card_results --dataset inno_db_export

This would query the inno_db_export dataset in your BigQuery project and create a table in the score_card_results dataset with the results.