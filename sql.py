#!/usr/bin/python
import MySQLdb as mdb
import string
import time

"""
this script uses prepared statements and batch insert
to insert data into sql database
"""

def main():

	file = "queries"
	inputFile = open(file, "r")

	route = True

	rAsn = []
	rPrefix = []
	rVal = []
	rRir = []
	rVrp = []
	rIpver = []
	rBinary = []

	vId = []
	vAsn = []
	vPrefix = []
	vMax = []
	vMin = []
	vBinary = []


	print "start reading data"

	m1 = int(round(time.time() * 1000))

	#counter = 0

	for line in inputFile:
		#if counter == 50:
		#	break
		if "ROUTES" in line:
			route = True
		elif "VRP" in line:
			route = False
		else:
			parts = line.split(';')
			if route:
				rAsn.append(parts[0])
				rPrefix.append(parts[1])
				rVal.append(parts[2])
				rRir.append(parts[3])
				rVrp.append(parts[4])
				rIpver.append(parts[5])
				rBinary.append(parts[6])
			else:
				vId.append(parts[0])
				vAsn.append(parts[1])
				vPrefix.append(parts[2])
				vMax.append(parts[3])
				vMin.append(parts[4])
				vBinary.append(parts[5])
		#counter += 1

	m2 = int(round(time.time() * 1000))

	print "reading: " + str(m2-m1) + "ms"


	connection=mdb.connect('localhost', 'root', 'skims12345', 'bgp', unix_socket='/opt/lampp/var/mysql/mysql.sock') #Don't worry, not a real password
	cursor=connection.cursor()

	### ROUTES ###

	print "ROUTES"
	
	cursor.execute("TRUNCATE routes")	

	sql = "INSERT INTO `bgp`.`test_routes` (`ASN`, `Prefix`, `Validity`, `RIR`, `VRP`, `IPver`, `bin`) VALUES (%s, %s, %s, %s, %s, %s, %s);"

	params = [(str(rAsn[i]), rPrefix[i], rVal[i], rRir[i], rVrp[i], str(rIpver[i]), rBinary[i]) for i in range(len(rAsn))]

	m1 = int(round(time.time() * 1000))
	
	cursor.executemany(sql, params)
	
	m2 = int(round(time.time() * 1000))
	
	connection.commit()

	m3 = int(round(time.time() * 1000))

	print "execute: " + str(m2-m1) + "ms"
	print "commit: " + str(m3-m2) + "ms"

	### VRP ###

	print "VRP"

	cursor.execute("TRUNCATE vrp")

	sql = "INSERT INTO `bgp`.`test_vrp` (`id`, `ASN`, `IP_Prefix`, `Max_Length`, `Min_Length`, `bin`) VALUES (%s, %s, %s, %s, %s, %s);"

	params = [(str(vId[i]), str(vAsn[i]), vPrefix[i], vMax[i], vMin[i], vBinary[i]) for i in range(len(vId))]

	m1 = int(round(time.time() * 1000))
	
	cursor.executemany(sql, params)
	
	m2 = int(round(time.time() * 1000))
	
	connection.commit()

	m3 = int(round(time.time() * 1000))
	
	print "execute: " + str(m2-m1) + "ms"
	print "commit: " + str(m3-m2) + "ms"

main()

