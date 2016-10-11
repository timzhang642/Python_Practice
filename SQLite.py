# -*- coding: utf-8 -*-
"""
Created on Tue Oct  6 21:59:49 2015

@author: yuxuanzhang
"""

# this is Python's built-in SQL tool that can only be used locally(doesn't require a server).
# It's a convenient database prototyping tool.
import sqlite3 

connection = sqlite3.connect('csc455_1.db') #create/connect to a local database

# create a table
query = """
CREATE TABLE students
(ID CHAR(5) NOT NULL,
Name VARCHAR(25),
Standing VARCHAR(8), 
Credits INTEGER, 
CONSTRAINT student_PK PRIMARY KEY(ID)
);"""
#CONSTRAINT student_FK FOREIGN KEY(another_ID) REFERENCES another_table(another_ID) 

connection.execute(query)

# insert rows of data 
newdata = [('23456', 'Tim', 'Grad', 6), ('12345', 'Floyd', 'Undergrad', 3), ('45678', 'Jerry', 'Grad', 5)]
statement = "INSERT INTO students VALUES(?, ?, ?, ?)"
connection.executemany(statement, newdata)

# selecting data from table
cursor = connection.execute('select * from students')
rows = cursor.fetchall()
print rows


# put query result into a dataframe
import pandas.io.sql as sql

print sql.read_sql('select * from students', connection)