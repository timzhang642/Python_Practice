# -*- coding: utf-8 -*-
"""
Created on Tue Sep 29 10:17:59 2015

@author: yuxuanzhang
"""

def generateInsert(table,parameters):
    student_id = parameters[0]
    name = parameters[1]
    grade = parameters[2]
    
    print "INSERT INTO " + table + " VALUES (" + student_id + ',' + "'%s'"%name + ',' + "'%s'"%grade + ");"
    

generateInsert('Students', ['1', 'Jane', 'A-'])

address = "%d '%s', '%s' '%s' %d" % (243, 'S. Wabash', 'Chicago', 'IL', 60604)
print address


