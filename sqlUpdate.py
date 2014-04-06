#!/usr/bin/python

import MySQLdb as mdb
import sys
import time


validity = "U"
ipver = 4
if len(sys.argv) > 2:
	for i in range(1, len(sys.argv)):
		if(sys.argv[i] == "-v"):
			validity = sys.argv[i+1]
		if(sys.argv[i] == "-ip"):
			ipver = sys.argv[i+1]




#query = "SELECT * FROM `live_routes` WHERE ASN = 9737 OR ASN = 2519 LIMIT 1000"
#query = "SELECT * FROM `live_routes` WHERE ASN = 2519 AND Prefix = '1.0.22.0/23'"

countQuery = "SELECT COUNT(*) FROM `live_routes` WHERE  `live_routes`.`ASN` = 9737 OR `live_routes`.`ASN` = 2519;"

#updateQuery1 = "UPDATE  `bgp`.`live_routes` SET  `Validity` =  '" + validity + "' WHERE  `live_routes`.`ASN` = 2519 AND Prefix = '1.0.22.0/23';"
#updateQuery2 = "UPDATE  `bgp`.`live_routes` SET  `IPver` =  '" + str(ipver) + "' WHERE  `live_routes`.`ASN` = 2519 AND Prefix = '1.0.22.0/23';"
updateQuery1 = "UPDATE  `bgp`.`live_routes` SET  `Validity` =  '" + validity + "' WHERE  `live_routes`.`ASN` = 9737 OR `live_routes`.`ASN` = 2519;"
updateQuery2 = "UPDATE  `bgp`.`live_routes` SET  `IPver` =  '" + str(ipver) + "' WHERE  `live_routes`.`ASN` = 9737 OR `live_routes`.`ASN` = 2519;"

connection=mdb.connect('localhost', 'root', 'skims12345', 'bgp', unix_socket='/opt/lampp/var/mysql/mysql.sock') #Don't worry, not a real password
cursor=connection.cursor()

print "count entries"
m1 = int(round(time.time() * 1000))

result = cursor.execute(countQuery)
entries = cursor.fetchone()

print "start updating db"
m2 = int(round(time.time() * 1000))

cursor.execute(updateQuery1)
cursor.execute(updateQuery2)

connection.commit()

connection.close()

m3 = int(round(time.time() * 1000))

print "finished"
print "\n############# time measure #############"
print "number of results: " + str(entries[0])
print "count query: " + str(m2 - m1) + " ms"
print "update: " + str(m3 - m2) + " ms"
print "all time: " + str(m3 - m1) + " ms"
print "########################################\n"


