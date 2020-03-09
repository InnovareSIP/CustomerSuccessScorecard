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
Rename the file to bqkeys.json and move it to the /config folder of this scripts directory

Create a config.ini in the /config directory. Edit the file to include the following information in order to configure it to connect to your databases.  You only need to configure the databases you would like to connect to ie if you would only like to connect only to a local database this file only needs to the \[local\] section to be configured. 

\[local\]<br/>
host=\[local host\]<br/>
user=\[local user\]<br/>
passwrd=\[local password\]<br/>

\[staging\]<br/>
host=\[staging host\]<br/>
user=\[staging user\]<br/>
passwrd=\[staging password\]<br/>

\[production\]<br/>
host=\[production host\]<br/>
user=\[production username\]<br/>
passwrd=\[production password\]<br/>

# Running the script
The script requires two arguments in order to run: \[name of the enviroment you want to connect to\] \[name of database to copy\]<br/>
Example:<br/>
\> py datagenerate.py local mock_data_db

This would copy all tables from your local mock_data_db and upload them to big query inside of the dataset mock_data_db. Local CSV files would be saved to the /output folder of the scripts directory