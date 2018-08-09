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



### Main ###

# Get the data set, before running the script in Python:
# https://www.kaggle.com/dcohen21/8anu-climbing-logbook
# In parent directory: unpacked in folder '/8a_climbing'

# Connect to local SQLite database with the 8a.nu Climbing Logbook data set
conn = sqlite3.connect('../8a_climbing/database.sqlite')

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

# Close the connection to the SQLite database at the end of the session
conn.close()