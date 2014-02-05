#!/usr/bin/python
import fileinput
import re
import datetime
import MySQLdb as mdb
import ipaddr
import datrie
import string
import subprocess
import sys

vrpId = 0

# this method reads the rir data from a file, that was downloaded before starting the script
def readRirData(filename):
	
	file = open(filename, "r")
	dict = {}
	
	for line in file:
		prefix = line.rstrip().split(" ")[0]
		rir = line.rstrip().split(" ")[1]
		dict[prefix] = rir
	
	return dict


def truncateRoutes():
	return "TRUNCATE live_routes"


def truncateVrp():
	return "TRUNCATE live_vrp"

# returns the current timestamp
def getTimestamp():
	return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S');

# this method transforms the ip-prefix into binary
def toBin(ip, length):
	prefix = prefix = "%032d" % (int(bin(ipaddr.IPv4Network(ip).network)[2:]))
	return prefix[:int(length)]


def createTableStrRoutes():
	str = "CREATE TABLE IF NOT EXISTS `live_routes` ( `ASN` int(10) NOT NULL DEFAULT '0', `Prefix` varchar(43) NOT NULL DEFAULT '', `Validity` varchar(2) DEFAULT NULL, `RIR` varchar(10) DEFAULT NULL, `VRP` varchar(100) DEFAULT NULL, `IPver` tinyint(1) DEFAULT NULL, `bin` varchar(128) DEFAULT NULL, PRIMARY KEY (`ASN`,`Prefix`)) ENGINE=InnoDB DEFAULT CHARSET=latin1;"
	return str


def createTableStrVrp():
	str = "CREATE TABLE IF NOT EXISTS `live_vrp` ( `id` int(11) NOT NULL AUTO_INCREMENT, `ASN` int(10) NOT NULL DEFAULT '0', `IP_Prefix` varchar(43) NOT NULL DEFAULT '', `Max_Length` tinyint(3) NOT NULL DEFAULT '0', `Min_Length` tinyint(3) NOT NULL DEFAULT '0', `bin` varchar(128) DEFAULT NULL, PRIMARY KEY (`ASN`,`IP_Prefix`,`Max_Length`,`Min_Length`), KEY `id` (`id`)) ENGINE=InnoDB  DEFAULT CHARSET=latin1 AUTO_INCREMENT=6707 ;"
	return str

# this method creates the tables live_routes and live_vrp
def createTable():
	connection=mdb.connect('localhost', 'root', 'skims12345', 'bgp', unix_socket='/opt/lampp/var/mysql/mysql.sock') #Don't worry, not a real password
	cursor=connection.cursor()
	print "Create Table `live_vrp`"
	cursor.execute(createTableStrVrp())
	print "Create Table `live_routes`"
	cursor.execute(createTableStrRoutes())
	connection.commit()
	connection.close()

# this method returns a string that inserts vrp data into the database
def insertStrVrp(ip, min_length, max_length, asn):
	return "INSERT INTO `bgp`.`live_vrp` (`id`, `ASN`, `IP_Prefix`, `Max_Length`, `Min_Length`, `bin`) VALUES (%s, "+asn+", '"+ip+"', '"+max_length+"', '"+min_length+"', '"+toBin(ip, min_length)+"');"

# this method returns a string that inserts routes data into the database
def insertStrRoutes(ip, length, asn, validity, vrp, rir):	
	return "INSERT INTO `bgp`.`live_routes` (`ASN`, `Prefix`, `Validity`, `RIR`, `VRP`, `IPver`, `bin`) VALUES ("+asn+", '"+ip+"/"+length+"', '"+validity+"', '"+rir+"', '"+vrp+"', 4, '"+toBin(ip, length)+"');"


def execute(str, cursor):
	cursor.execute(str)


def printList(list, file):
	for item in list:
		file.write(item + "\n")


def printDict(dict, file):
	for key in dict:
		if dict[key] == "":
			continue
		else:
			file.write(key + " " + dict[key] + "\n")

# this method inserts a new route string into the list
def insertRoute(list, insertStr):
	list.append(insertStr)

# this method inserts a new vrp string into the dictionary an returns its id
def insertVrp(dict, insertStr):
	if insertStr in dict:
		return dict[insertStr]
	else:
		global vrpId
		vrpId += 1
		dict[insertStr] = str(vrpId)
		return str(vrpId)

# this method inserts all route insertion strings into the table
def insertSqlFromListRoutes(list, cursor):
	for item in list:
		execute(item, cursor)

# this method inserts all vrp insertion strings into the table
def insertSqlFromDictVrp(dict, cursor):
	for key in dict:
		execute(key % (dict[key]), cursor)


# this method creates all html files using curl
def createHtmlFiles():
	subprocess.call("curl http://localhost/rpki-live/perrir.php > /opt/lampp/htdocs/rpki-live/perrir.html", shell=True)
	subprocess.call("curl http://localhost/rpki-live/maps.php > /opt/lampp/htdocs/rpki-live/maps.html", shell=True)
	subprocess.call("curl http://localhost/rpki-live/trends.php > /opt/lampp/htdocs/rpki-live/trends.html", shell=True)
	subprocess.call("curl http://localhost/rpki-live/global.php > /opt/lampp/htdocs/rpki-live/global.html", shell=True)
	subprocess.call("curl http://localhost/rpki-live/top10.php > /opt/lampp/htdocs/rpki-live/top10.html", shell=True)	
	subprocess.call("curl http://localhost/rpki-live/perrir.php?rir=afrinic > /opt/lampp/htdocs/rpki-live/afrinic.html", shell=True)
	subprocess.call("curl http://localhost/rpki-live/perrir.php?rir=apnic > /opt/lampp/htdocs/rpki-live/apnic.html", shell=True)
	subprocess.call("curl http://localhost/rpki-live/perrir.php?rir=arin > /opt/lampp/htdocs/rpki-live/arin.html", shell=True)
	subprocess.call("curl http://localhost/rpki-live/perrir.php?rir=lacnic > /opt/lampp/htdocs/rpki-live/lacnic.html", shell=True)
	subprocess.call("curl http://localhost/rpki-live/perrir.php?rir=ripe > /opt/lampp/htdocs/rpki-live/ripe.html", shell=True)
	subprocess.call("curl http://localhost/rpki-live/validitytables.php > /opt/lampp/htdocs/rpki-live/validitytables.html", shell=True)
	subprocess.call("curl http://localhost/rpki-live/validitytables.php?v=V > /opt/lampp/htdocs/rpki-live/validitytables_V.html", shell=True)
	subprocess.call("curl http://localhost/rpki-live/validitytables.php?v=I% > /opt/lampp/htdocs/rpki-live/validitytables_I.html", shell=True)
	subprocess.call("curl http://localhost/rpki-live/validitytables.php?v=IA > /opt/lampp/htdocs/rpki-live/validitytables_IA.html", shell=True)
	subprocess.call("curl http://localhost/rpki-live/validitytables.php?v=IP > /opt/lampp/htdocs/rpki-live/validitytables_IP.html", shell=True)
	subprocess.call("curl http://localhost/rpki-live/validitytables.php?v=IQ > /opt/lampp/htdocs/rpki-live/validitytables_IQ.html", shell=True)
	subprocess.call("curl http://localhost/rpki-live/validitytables.php?v=IB > /opt/lampp/htdocs/rpki-live/validitytables_IB.html", shell=True)
	subprocess.call("curl http://localhost/rpki-live/ipcomp.php > /opt/lampp/htdocs/rpki-live/ipcomp.html", shell=True)
	subprocess.call("curl http://localhost/rpki-live/index.php > /opt/lampp/htdocs/rpki-live/index.html", shell=True)

# this method checks why a route is invalid
def checkValidity(vrpArray, routeAsn, routeBin):
	validity = ""
	for vrpData in vrpArray:
		vrpAsn = vrpData[0]
		vrpMaxLen = vrpData[1]
		vrpBin = vrpData[2]
		
		#Check if length is too great (prefix too specific)
		if (int(vrpAsn) == int(routeAsn)) and (len(routeBin) > int(vrpMaxLen)):
			if (len(vrpBin) == int(vrpMaxLen)):
				validity = "IP"
			else:
				validity = "IQ"
		#Check if AS number mismatches but prefix falls within valid range
		elif (int(vrpAsn) != int(routeAsn)) and (len(routeBin) >= len(vrpBin)) and (len(routeBin) <= int(vrpMaxLen)):
			validity = "IA"
		#Check if both AS does not match and prefix is too specific
		elif (int(vrpAsn) != int(routeAsn)) and (len(routeBin) > int(vrpMaxLen)):
			validity = "IB"
			
		##############################
		#Validity states:            #
		##############################
		#                            #
		# IP = Fixed length exceeded #
		# IQ = Length range exceeded #
		# IA = AS does not match     #
		# IB = Prefix too specific   #
		#      AND AS does not match #
		#  V = Valid                 #
		#  U = Unknown               #
		##############################
	return validity

# main method
def main():
	
	listRoutes = []
	dictVrp = {}
	
	# read rir data from a file
	rirFile = "rirs"
	dictRir = readRirData(rirFile)
	
	# create the 2 tables if they do not exist
	createTable()
	
	# handle database connecton
	connection=mdb.connect('localhost', 'root', 'skims12345', 'bgp', unix_socket='/opt/lampp/var/mysql/mysql.sock') #Don't worry, not a real password
	cursor=connection.cursor()
	
	# regex patterns for 3 cases
	patternNotFound = '-1\|(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\s+(\d+)\s+(\d+)'
	patternInvalid = '0\|(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\s+(\d+)\s+(\d+)\|(.+)'
	patternValid = '1\|(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\s+(\d+)\s+(\d+)\|(.+)'
	
	# debug
	"""
	datasetId = 1
	file = open("dataset" + str(datasetId), "w")
	file2 = open("dataset_vrp" + str(datasetId), "w")
	"""
	
	# debug, comment next for loop if use
	"""
	lines = ("-1|186.17.16.0 20 23201", "1|200.35.183.0 24 26617|26617 200.35.176.0 20 24", "1|5.149.83.0 24 59457|35567 5.149.64.0 19 24,59457 5.149.64.0 19 24", "1|80.79.148.0 22 34708|6453 80.79.144.0 20 24,9051 80.79.144.0 20 24,34708 80.79.144.0 20 24,39275 80.79.144.0 20 24", "0|198.176.45.0 24 55079|6079 198.176.44.0 22 24,6939 198.176.44.0 22 24,12271 198.176.44.0 22 24,20473 198.176.44.0 22 24,36236 198.176.44.0 22 24", "Next")
	for line in lines:
	"""
	
	# receive the rib data via pipe
	for line in fileinput.input():
		# now the parser collects new data, so we truncate the tables and insert our collected data
		if "Next" in line:
			
			print str(getTimestamp())
			
			# truncate tables
			print "truncate old data"
			execute(truncateRoutes(), cursor)
			execute(truncateVrp(), cursor)
			
			# insert data from dict into table
			print "insert routes data"
			insertSqlFromListRoutes(listRoutes, cursor)
			print "insert vrp data"
			insertSqlFromDictVrp(dictVrp, cursor)
			
			connection.commit()
			
			print str(getTimestamp())
			
			# debug
			"""
			print "file1"
			printList(listRoutes, file)
			print "file2"
			printDict(dictVrp, file2)
			"""
			
			# delete list and dict and create new
			del listRoutes
			listRoutes = []
			dictVrp.clear()
			
			# create html files
			createHtmlFiles()
			
			# debug
			#subprocess.call("curl http://localhost/rpki-live/global.php > /opt/lampp/htdocs/rpki-live/global"+str(datasetId)+".html", shell=True)
			
			global vrpId
			vrpId = 0
			
			# debug
			"""
			file.close()
			file2.close()
			datasetId += 1
			file = open("dataset" + str(datasetId), "w")
			file2 = open("dataset_vrp" + str(datasetId), "w")
			"""
			
		
		# use regex on input line
		notFound = re.match(patternNotFound, line)
		invalid = re.match(patternInvalid, line)
		valid = re.match(patternValid, line)
		
		# prefix was not found
		if notFound:
			# rir
			ipPart = str(notFound.group(1)).split(".")[0]
			if ipPart in dictRir.keys():
				rir = dictRir[ipPart]
			else:
				rir = ""
			# addr len asn
			insertRoute(listRoutes, insertStrRoutes(notFound.group(1), notFound.group(2), notFound.group(3), 'U', '', rir))
		
		# prefix is invalid
		elif invalid:
			vrp = ""
			# handle vrp
			parts = invalid.group(4).split(",")
			# this is needed for check, why route is invalid
			vrpArray = [[] for i in range(len(parts))]
			counter = 0
			
			for part in parts:
				# ROA-ASN IP MaskMin MaskMax
				subPart = part.split(" ")
				id = insertVrp(dictVrp, insertStrVrp(subPart[1], subPart[2], subPart[3], subPart[0]))
				vrp += id + ", "
				# this is needed for check, why route is invalid
				vrpArray[counter] = (subPart[0], subPart[3], toBin(subPart[1], subPart[2]))
				counter += 1
			
			# now check why the route is invalid
			validity = checkValidity(vrpArray, invalid.group(3), toBin(invalid.group(1), invalid.group(2)))
			# route handling
			# rir
			ipPart = str(invalid.group(1)).split(".")[0]
			if ipPart in dictRir.keys():
				rir = dictRir[ipPart]
			else:
				rir = ""
			# addr len asn
			insertRoute(listRoutes, insertStrRoutes(invalid.group(1), invalid.group(2), invalid.group(3), validity, vrp, rir))
		
		# prefix is valid
		elif valid:
			vrp = ""
			# handle vrp
			parts = valid.group(4).split(",")
			for part in parts:
				# ROA-ASN IP MaskMin MaskMax
				subPart = part.split(" ")
				id = insertVrp(dictVrp, insertStrVrp(subPart[1], subPart[2], subPart[3], subPart[0]))
				vrp += id + ", "
			
			# route handling
			# rir
			ipPart = str(valid.group(1)).split(".")[0]
			if ipPart in dictRir.keys():
				rir = dictRir[ipPart]
			else:
				rir = ""
			# addr len asn
			insertRoute(listRoutes, insertStrRoutes(valid.group(1), valid.group(2), valid.group(3), 'V', vrp, rir))
		
		else:
			print "No pattern matched"
		
	connection.commit()
	connection.close()




main()








