#!/usr/bin/python

# neo4j rest client
from neo4jrestclient.client import GraphDatabase

import re
import datetime
import time

# returns the current timestamp
def getTimestamp():
	return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S');


gdb = GraphDatabase("http://localhost:7474/db/data/")

q = "match (n:Route) where n.asn=\"9737\" or n.asn=\"2519\" return n limit 1000"
#q = "match (n:Route) where n.asn=\"2519\" and n.prefix=\"1.0.22.0/23\" return n"


m1 = millis = int(round(time.time() * 1000))
print "start query db"

results = gdb.query(q=q)


m2 = millis = int(round(time.time() * 1000))

print "extract ids"


resultIds = []

for result in results:
	regex = "\D+\d+\D+(\d+)\D+"
	res = re.match(regex, result[0]['traverse'])
	id = res.group(1)
	resultIds.append(id)

m3 = millis = int(round(time.time() * 1000))

print "lookup ids and change content"


for id in resultIds:
	n = gdb.node[id]
	#print n['val']
	#print str(id) + ": " + str(n['asn']) + " - " + str(n['prefix'] + " - " + str(n['val']))
	
	# if 4 then change to 6, if 6 change to 4
	if n['ipver'] == 4:
		n['ipver'] = 6
	elif n['ipver'] == 6:
		n['ipver'] = 4

	# if U then change to V, if V change to U
	
	if n['val'] == "U":
		n['val'] = "V"
	elif n['val'] == "V":
		n['val'] = "U"
	

m4 = millis = int(round(time.time() * 1000))

print "finished"
print "\n############# time measure #############"
print "number of results: " + str(len(results))
print "querying: " + str(m2 - m1) + " ms"
print "id extracting: " + str(m3 - m2) + " ms"
print "change content: " + str(m4 - m3) + " ms"
print "all time: " + str(m4 - m1) + " ms"
print "########################################\n"




