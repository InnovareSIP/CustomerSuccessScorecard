from inspect import getmembers, isfunction
from . import bqueries 

#Excutes all funtions in bqueries.py and builds the sql query to send to BigQuery
def get_scorecard(dataset):
    #Get all function names as a list
    list_of_functions =  [i[0] for i in [o for o in getmembers(bqueries) if isfunction(o[1])]]
    count = 0
    q1 =""
    q2 =""
    q3 =""
    #Build the SQL query
    for i in list_of_functions:
        count +=1
        #Call each function and give each CTE in SQL query a unique alias. Each function call will return part of our SQL query
        q1 += f"table_{count} AS({(getattr(bqueries, i)( dataset, f'tbl{count}_' ))}),"
        #Function names also match the column header aliases, so we can get the names here and build part of our query
        q2 += f"{i}, "
        if count + 1 <= len(list_of_functions):
            #Build out the JOINS of our query but we stop early so that we don't refernce a table that does not exist
            q3 += f"LEFT JOIN table_{count + 1} ON table_{count}.tbl{count}_organization = table_{count + 1}.tbl{count + 1}_organization \n"
    #Strip the trailing comma from our query
    q1 = q1.rstrip(",")
    #Build the final query with the correct format and insert a created at column to our final results table
    q1 = f"WITH {q1}\nSELECT tbl1_organization AS organization, {q2} CURRENT_TIMESTAMP AS created_at\n FROM table_1\n{q3}"
    
    return q1
