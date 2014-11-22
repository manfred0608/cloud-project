import MySQLdb

source_file = 'q4.sample'
db = MySQLdb.connect(host="54.88.109.43",
                     user="twitter", # your username
                      passwd="Qaq4yGmSpfRrFXaw", # your password
                      db="twitter") # name of the data base

cur = db.cursor() 

time_place='2014-05-08Bern'
sql = "SELECT data FROM q4 WHERE time_place=\"" + time_place + "\";"
cur.execute(sql)
    
num_row = int(cur.rowcount)
    
    
if num_row != 0:
    res = cur.fetchone()[0]
    print res
cur.close()
