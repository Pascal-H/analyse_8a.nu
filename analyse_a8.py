#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    File name: analyse_8a.py
    Author: Pascal Hecker
    Date created: 02/08/2018
    Python Version: 3.6
"""

### Import modules ###

# sqlite3 Module to access SQLite databases
import sqlite3
# pandas Module to handle structured data from databases in Python
import pandas as pd


### Functions ###

def preview_tables(table_names, limit, connection):
    """
    Fetch data from handed over tables.
    
    This function takes in a list of table names an returns a list of
    DataFrames with the content and selected number of rows from those tables.
    
    Parameters
    ----------
    table_names : list of strings
        List with strings of the names of tables to fetch data from. 
    
    limit : integer
        Number of rows to fetch from each table. If set to 0: return data
        from all rows.
    
    connection : SQLite Connection Object
        Connection object with an open connection to the target 
        SQLite database.
    
    Returns
    -------
    fetched_tables : list
        The data of every table with the selected number of rows is stored in
        a DataFrame. All DataFrames are appended to a list and returned.
    
    """
    
    # List to contain all the fetched data from the database
    fetched_tables = []
    # Get data from every handed over table name
    for cur_table in table_names:
        # Compose the query string
        # In case limit = 0: SELECT * without LIMIT
        if limit == 0:
            cur_query = "SELECT * FROM {name};".format(
                    name = cur_table)
        else:
            cur_query = "SELECT * FROM {name} LIMIT {limit};".format(
                    name = cur_table, limit = limit)
        
        # Fetch data based on query string for the current table (cur_table)
        cur_result = pd.read_sql_query(cur_query, con = connection)
        
        # Append DataFrame with current data set to list with all results
        fetched_tables.append(cur_result)
    
    # Return list with DataFrames of all tables
    return fetched_tables


def get_table_names(connection):
    """
    Get all table names from the connected data base.
    
    
    Parameters
    ----------
    connection : SQLite Connection Object
        Connection object with an open connection to the target 
        SQLite database.
    
    Returns
    -------
    table_names : list of strings
        List with all table names of the connected SQLite database as strings.
    
    """
    
    ## Query to extract the names of all tables from a database ##
    # sqlite_master: Master listing of all database objects
    # Filter by name to get only the names of all tables
    tables_query = """
        SELECT name 
        FROM sqlite_master
        WHERE type='table'
        ORDER BY name;
    """
    
    # Save returned table names from query to data frame
    tables_info = pd.read_sql_query(tables_query, connection)
    
    # Extract the table names to the table_names list
    table_names = []
    for cur_table_name in tables_info.name:
        table_names.append(cur_table_name)
    
    # Return the list with all table names
    return table_names


def get_column_info(tables_names, connection):
    """
    Gets column metadata from the handed over tables.
    
    Parameters
    ----------
    table_names : list of strings
        List with strings of the names of tables to fetch data from. 
    
    connection : SQLite Connection Object
        Connection object with an open connection to the target 
        SQLite database.
    
    Returns
    -------
    tables_column_infos : dictionary
        Dictionary with a key for each table and the respective column metadata
        contained as a Data Frame.
    
    """
    
    # Dinctionary to contain the table name and column metadata key-value pairs
    tables_column_infos = {}
    # Fetch the column metadata for each table name in the list
    # PRAGMA: SQLite specific meta commands and queries
    # table_info: returns all configuration data on the column of each table
    for cur_table in tables_names:
        columns_query = """
            PRAGMA table_info({name});
        """.format(
            name = cur_table)
        
        # Fetch the column metadata of the current table with the current query
        # and save it to a DataFrame
        cur_column_info = pd.read_sql_query(columns_query, connection)
        
        # For the 8a.nu data set: cid = column index is the first column
        # Set this as the index of the DataFrame
        cur_column_info = cur_column_info.set_index('cid')
        
        # Use the name of the current talbe as the key in the output dictionary
        # and assign the DataFrame with the respective metadata of the columns
        tables_column_infos[cur_table] = cur_column_info
    
    # Return the dictionary with all table names and column metadata
    return tables_column_infos


### Main ###

# Get the data set, before running the script in Python:
# https://www.kaggle.com/dcohen21/8anu-climbing-logbook
# In parent directory: unpacked in folder '/8a_climbing'

# Connect to local SQLite database with the 8a.nu Climbing Logbook data set
conn = sqlite3.connect('../8a_climbing/database.sqlite')


## Manually query the database ##

# Simple query to get a first impression of the table 'user'
query_distinct = "SELECT * FROM user LIMIT 50;"

# Query to get the meta-info of the table 'user
query_pragma = "PRAGMA table_info(user);"

# Fetch the data from the database and read it into a DataFrame 
result_distinct = pd.read_sql_query(query_distinct, conn)

# Fetch the meta-data
result_pragma = pd.read_sql_query(query_pragma, conn)

# Print the header of the DataFrame to get a first impression
print(result_distinct.head(10))

# Print the meta-data:
# cid = column id; name = column name; type = data tpye;
# notnull = must have values; dflt_value = default-value; pk = primary key
print(result_pragma)


## Use function to peak into all tables ##
# List with all table names of 8a.nu database
table_names = ["ascent", "grade", "method", "user"]
# Limit the number of rows to return from each table
limit = 100

# Call function and fetch list of DataFrames with data for each table
preview_list = preview_tables(table_names, limit, conn)

# Print the head of all data sets of all tables in the 8a.nu database
for cur_table_data in preview_list:
    print(cur_table_data.head())


## Automatically fetch all table names and get the column metadata ##

# Get list with all table names from the connected database
table_names_lst = get_table_names(conn)

# Get dictionary assigning the column metadata to every table name
tables_dct = get_column_info(table_names_lst, conn)

# Print the column attributes from the 'first' entry of the dictionary
column_attributes = list(tables_dct["ascent"].columns.values)
print(column_attributes)
# Legend for the abreviation for the column metadata labels
attributes_legend =  "Legend for the column attributes:\n"
attributes_legend += "cid:        column identifier\n"
attributes_legend += "name:       column name\n"
attributes_legend += "type:       data type\n"
attributes_legend += "notnull:    must contain values\n"
attributes_legend += "dflt_value: default value\n"
attributes_legend += "pk:         primary key marker\n\n"
print(attributes_legend)

# Print the Table name (key) and the column metadata (value) for all tables
for cur_table in tables_dct.items():
    print("Table Name: " + str(cur_table[0]))
    print("Column Data:\n" + str(cur_table[1]) + "\n\n")


# Close the connection to the SQLite database at the end of the session
conn.close()