#!/usr/bin/python
import fileinput
import re
import datetime
import ipaddr
import string
import subprocess
import sys

from neo4jrestclient.client import GraphDatabase

vrpId = 0
vrpNodes = {}

##################### CLASSES #####################
class Vrp:
	id=0
	asn=0
	ip=""
	max=0
	min=0
	binary=""

	def __init__(self, id, asn, ip, max, min, binary):
		self.id=id
		self.asn=asn
		self.ip=ip
		self.max=max
		self.min=min
		self.binary=binary

	def __eq__(self, other):
		if self.asn==other.asn and self.ip==other.ip and self.max==other.max and self.min==other.min and self.binary==other.binary:
			return True
		else:
			return False

	def __hash__(self):
		return hash(str(self.asn)+str(self.ip)+str(self.max)+str(self.min)+str(self.binary))

	def setId(self, id):
		self.id=id

class Route:
	asn=0
	prefix=""
	val=""
	rir=""
	ipver=0
	binary=""
	vrp=[]

	def __init__(self, asn, prefix, val, rir, ipver, binary, vrp):
		self.asn = asn
		self.prefix = prefix
		self.val = val
		self.rir = rir
		self.ipver = ipver
		self.binary = binary
		self.vrp=vrp



##################### DEBUG #####################

def printList(list, file):
	for item in list:
		file.write(item + "\n")

def printDict(dict, file):
	for key in dict:
		if dict[key] == "":
			continue
		else:
			file.write(key + " " + dict[key] + "\n")



##################### METHODS #####################

# this method reads the rir data from a file, that was downloaded before starting the script
def readRirData(filename):
	
	file = open(filename, "r")
	dict = {}
	
	for line in file:
		prefix = line.rstrip().split(" ")[0]
		rir = line.rstrip().split(" ")[1]
		dict[prefix] = rir
	
	return dict

# this method clears the graph database
def truncateDatabase():
	gdb = GraphDatabase("http://localhost:7474/db/data/")
	q = "MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r"
	gdb.query(q=q)

# returns the current timestamp
def getTimestamp():
	return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S');

# this method transforms the ip-prefix into binary
def toBin(ip, length):
	prefix = prefix = "%032d" % (int(bin(ipaddr.IPv4Network(ip).network)[2:]))
	return prefix[:int(length)]

# this method inserts a new route string into the list
def insertRoute(list, route):
	list.append(route)

# this method inserts a new vrp string into the dictionary an returns its id
def insertVrp(dict, vrp):
	if vrp in dict:
		return dict[vrp]
	else:
		global vrpId
		vrpId += 1
		dict[vrp] = str(vrpId)
		return str(vrpId)

def insertRouteToNeo4j(route):
	vrps = route.vrp
	gdb = GraphDatabase("http://localhost:7474/db/data/")
	# insert node in graph db
	routeNode = gdb.nodes.create(asn=route.asn, prefix=route.prefix, val=route.val, rir=route.rir, ipver=route.ipver, bin=route.binary)
	# add label to node
	routeNode.labels.add("Route")
	# create relationships
	for vrp in vrps:
		routeNode.relationships.create(":HAS_VRP", vrpNodes[vrp])
	# return the node
	return routeNode

def insertVrpToNeo4j(vrp, vrpId):
	gdb = GraphDatabase("http://localhost:7474/db/data/")
	# insert node in graph db
	vrpNode = gdb.nodes.create(id=vrpId, asn=vrp.asn, ip=vrp.ip, max=vrp.max, min=vrp.min, bin=vrp.binary)
	# add label to node
	vrpNode.labels.add("Vrp")
	# return the node
	return vrpNode

# this method inserts all routes into graph db
def insertNeo4jFromListRoutes(list):
	for route in list:
		insertRouteToNeo4j(route)

# this method inserts all vrp into graph db
def insertNeo4jFromDictVrp(dict):
	for key in dict:
		vrpId = dict[key]
		vrpNode = insertVrpToNeo4j(key, vrpId)
		vrpNodes[vrpId] = vrpNode


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
	print "start"
	print str(getTimestamp())
	file = open("sql_input", "r")
	for line in file:
	# receive the rib data via pipe
	#for line in fileinput.input():
		# now the parser collects new data, so we truncate the tables and insert our collected data
		if "Next" in line:
			print str(getTimestamp())
			
			# truncate graph db
			print "truncate old data"
			truncateDatabase()
			
			print "vrp: " + str(len(dictVrp))
			print "routes: " + str(len(listRoutes))

			# insert data from dict into graph db
			print "insert vrp data"
			insertNeo4jFromDictVrp(dictVrp)

			print str(getTimestamp())

			print "insert routes data"
			insertNeo4jFromListRoutes(listRoutes)
			
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
			#print "Create HTML files"
			#createHtmlFiles()
			
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
			
			print "Wait for next dataset"
		
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

			ip = notFound.group(1)
			length = notFound.group(2)
			asn = notFound.group(3)
			# create route object
			route = Route(asn, ip + "/" + length, "U", rir, 4, toBin(ip, length), [])
			# insert route in list
			insertRoute(listRoutes, route)
		
		# prefix is invalid
		elif invalid:
			vrp = []
			# handle vrp
			parts = invalid.group(4).split(",")
			# this is needed for check, why route is invalid
			vrpArray = [[] for i in range(len(parts))]
			counter = 0
			
			for part in parts:
				# ROA-ASN IP MaskMin MaskMax
				subPart = part.split(" ")
				#vrp4 = Vrp(0, 41872, "193.247.108.0", 24, 22, "1100...")
				asn = subPart[0]
				ip = subPart[1]
				max_length = subPart[2]
				min_length = subPart[3]
				vrpData = Vrp(0, asn, ip, max_length, min_length, toBin(ip, min_length))
				id = insertVrp(dictVrp, vrpData)
				vrp.append(id)
				# this is needed for check, why route is invalid
				vrpArray[counter] = (asn, min_length, toBin(ip, max_length))
				counter += 1
			
			# now check why the route is invalid
			ip = invalid.group(1)
			length = invalid.group(2)
			asn = invalid.group(3)

			validity = checkValidity(vrpArray, asn, toBin(ip, length))
			# route handling
			# rir
			ipPart = str(ip).split(".")[0]
			if ipPart in dictRir.keys():
				rir = dictRir[ipPart]
			else:
				rir = ""
			# create route object
			route = Route(asn, ip + "/" + length, validity, rir, 4, toBin(ip, length), vrp)
			# insert route in list
			insertRoute(listRoutes, route)
		
		# prefix is valid
		elif valid:
			vrp = []
			# handle vrp
			parts = valid.group(4).split(",")
			for part in parts:
				# ROA-ASN IP MaskMin MaskMax
				subPart = part.split(" ")
				asn = subPart[0]
				ip = subPart[1]
				max_length = subPart[2]
				min_length = subPart[3]
				vrpData = Vrp(0, asn, ip, max_length, min_length, toBin(ip, min_length))
				id = insertVrp(dictVrp, vrpData)
				vrp.append(id)
			
			# route handling
			# rir
			ipPart = str(valid.group(1)).split(".")[0]
			if ipPart in dictRir.keys():
				rir = dictRir[ipPart]
			else:
				rir = ""

			ip = valid.group(1)
			length = valid.group(2)
			asn = valid.group(3)
			# create route object
			route = Route(asn, ip + "/" + length, "V", rir, 4, toBin(ip, length), vrp)
			# insert route in list
			insertRoute(listRoutes, route)
		
		#else:
			#print "No pattern matched"
	print "end"
	print str(getTimestamp())
		




main()








