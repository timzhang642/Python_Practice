# -*- coding: utf-8 -*-
"""
Created on Wed Oct 21 19:33:16 2015

@author: dgv359
"""

def generateInsert(table, valList):

    # Open the insert statement with the table name
    insStr = "INSERT INTO "+table+" VALUES("

    # Iterate through every element in the list of values.
    # enumerate gives us an index which will keep track of value's position
    for (index, value) in enumerate(valList):

        # Check if this value is a number or a NULL entry
        if type(value) == type(0) or value == 'NULL':
            insStr += str(value)         # Add as-is without quotes
        else:
            insStr += "'"+str(value)+"'" # Otherwise add quotes around the string value

        if index < len(valList)-1: #If this is not the last value, add a comma separator
            insStr += ", "

    return insStr + ");"  # Close the insert statement



ins1 = generateInsert("Students", [2, "NULL", "B+"])
print (ins1)

ins2 = generateInsert("Students", [3, "Jane", "B+", "NULL", 5])
print (ins2)
