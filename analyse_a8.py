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

# Fetch the data from the database and read it into a DataFrame 
result_distinct = pd.read_sql_query(query_distinct, conn)

# Print the header of the DataFrame to get a first impression
print(result_distinct.head(10))

# Close the connection to the SQLite database at the end of the session
conn.close()