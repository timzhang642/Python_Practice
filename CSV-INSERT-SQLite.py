# -*- coding: utf-8 -*-
"""
Created on Tue Oct  6 22:44:24 2015

@author: yuxuanzhang
"""

import sqlite3 

connection = sqlite3.connect('csc455_3.db') #create/connect to a local database

# create a table
query1 = """
CREATE TABLE city
(Chauffeur_City VARCHAR(40) NOT NULL,
Chauffeur_State CHAR(2),
CONSTRAINT city_pk PRIMARY KEY(Chauffeur_City)
);"""

try:    
    connection.execute(query1)
except:
    pass # do not create table if it already exists

# create another table
query2 = """
CREATE TABLE record(
  License_Number NUMBER(6),
  Renewed VARCHAR(6),
  Status VARCHAR(20),
  Status_Date DATE,
  Driver_Type VARCHAR(20),
  License_Type VARCHAR(15),
  Original_Issue_Date DATE,
  Name VARCHAR(100),
  Sex VARCHAR(20),
  Chauffeur_City VARCHAR(40),
  Record_Number CHAR(11) NOT NULL,
  CONSTRAINT record_pk PRIMARY KEY(Record_Number),
  CONSTRAINT record_fk FOREIGN KEY (Chauffeur_City) REFERENCES city(Chauffeur_City)
);"""

try:    
    connection.execute(query2)
except:
    pass # do not create table if it already exists

# read new data
import pandas as pd

newdata=pd.read_csv('Public_Chauffeurs_Short.csv')

# put data into nested list, each sub list is a row
record_list = [] # for record table


i = 0
while i < len(newdata):
    tempList = []
    for elem in newdata.ix[[i]].values[0]:
        tempList.append(elem)
    
    #city_list.append(tempList[9:11])
    
    del tempList[10]
    record_list.append(tempList)
    i = i +1

insert_record = "INSERT INTO record VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
connection.executemany(insert_record, record_list)


# store unique city-state pairs
city_list = [] # for city table
j=0
city_state_pair=newdata.groupby(['Chauffeur City','Chauffeur State'])['Chauffeur City'].count().index.values
    
for elem in city_state_pair:
    tempList = []
    tempList.append(elem[0])
    tempList.append(elem[1])
    city_list.append(tempList)

insert_city = "INSERT INTO city VALUES(?, ?)"
connection.executemany(insert_city, city_list)

#如果想从右边interface试不同的query,
#就在右边直接用：sql.read_sql('select * from tweets', connection)
