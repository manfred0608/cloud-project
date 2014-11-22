#!/usr/bin/env python

import csv

current_key = None;
current_priority = 0;
outputValue = None;



with open('./output/q4.csv', 'rb') as inFile:
    reader = csv.reader(inFile)
    for row in reader:
        if current_key == row[0] :
            if current_priority == (int)(row[3]):
                outputValue += ','
                outputValue += row[2]
            else :
                outputValue += '#'
                current_priority = (int)(row[3])
                outputValue += str(current_priority)
                outputValue += '#'
                outputValue += row[1]
                outputValue += ':'
                outputValue += row[2]
        else :
            if current_key != None :
                print "\"" + current_key + "\",\"" + outputValue + "\""
                
            current_key = row[0]
            current_priority = (int)(row[3])
            outputValue = row[3]
            outputValue += '#'
            outputValue += row[1]
            outputValue += ':'
            outputValue += row[2]
            
    if current_key != None:
        print "\"" + current_key + "\",\"" + outputValue + "\""
            
                
