#!/usr/bin/python
from socket import *
import re
import xml
import xml.dom.minidom as dom
import string
import signal
import sys
import datrie
import time
from pprint import pprint

def parse(xml, update, withdrawal): 
	l = [] 
	try:
		tree = dom.parseString(xml)
	except xml.parsers.expat.ExpatError:
		print >> sys.stderr, xml
		return []

	d = {}
	for i in tree.firstChild.childNodes: 
		if i.nodeName == "ASCII_MSG": 
			schluessel = wert = None
			for elems in i.childNodes: 
				if elems.nodeName == "UPDATE": 
					if elems.firstChild.nodeName == "WITHDRAWN":
						count = int(elems.firstChild.getAttribute("count"))
						if ((update and withdrawal) or ((count == 0) and not withdrawal and update) or ((count > 0) and not update and withdrawal)):
							#print "count: " + str(count) + "\tupdate: " + str(update) + "\twithdrawal: " + str(withdrawal)
							if count == 0:
								d["update"] = update
								d["withdrawal"] = False
							if count > 0:
								d["update"] = False
								d["withdrawal"] = withdrawal
							d["count"] = int(elems.firstChild.getAttribute("count"))
							for update in elems.childNodes: 
								if(update.nodeName == "NLRI"):
									for nlri in update.childNodes:
										if(nlri.nodeName == "PREFIX"):
											for prefix in nlri.childNodes:
												if(prefix.nodeName == "ADDRESS"):
													for txt in prefix.childNodes:
														s = txt.data.split("/")
														if not "prefix" in d:
															d["prefix"] = []
														p = {}
														p["address"] =  s[0]
														p["len"] = s[1]
														d["prefix"].append(p)
								if(update.nodeName == "PATH_ATTRIBUTES"):
									for pattr in update.childNodes:
										if(pattr.nodeName == "ATTRIBUTE"):
											for pattr in pattr.childNodes:
												if(pattr.nodeName == "AS_PATH"):
													for asPath in pattr.childNodes:
														if(asPath.nodeName == "AS_SEG"):
															length =  int(asPath.getAttribute("length"))
															origin_as = asPath.childNodes[length-1]
															d['as_path'] = asPath.childNodes
															d["origin_as"] = str(origin_as.childNodes[0].data)
												# IPV6 prefix
												if(pattr.nodeName == "MP_REACH_NLRI"):
													for mpReachNlri in pattr.childNodes:
														if(mpReachNlri.nodeName == "NLRI"):
															for nlri in mpReachNlri.childNodes:
																if(nlri.nodeName == "PREFIX"):
																	for prefix in nlri.childNodes:
																		if(prefix.nodeName == "ADDRESS"):
																			for txt in prefix.childNodes:
																				s = txt.data.split("/")
																				if not "prefix" in d:
																					d["prefix"] = []
																				p = {}
																				p["address"] =  s[0]
																				p["len"] = s[1]
																				d["prefix"].append(p)
			break;
	if("prefix" in d):
		l.append(d)
	return l

def insertTrie(trie, address, length, asn, update, withdrawal, count):
	key = u''+address+" "+length+" "+asn
	if (update and not withdrawal):
		#print "update"
		trie[key] = True
		"""
		if (key in trie):
			if (trie[key] == ""):
				print "empty path, insert element"
				trie[key] = element
			else:
				print "already in, overwrite element"
				trie[key] = element
		else:
			print "not in, insert into trie"
			trie[key] = element
		"""
	elif (not update and withdrawal):
		#print "withdrawal"
		trie[key] = False
	
	return trie


def printTrie(trie):
	state = datrie.State(trie)
	state.walk(u'')
	it = datrie.Iterator(state)
	while it.next():
		if it.data() == False:
			continue
		else:
			print(it.key())
	return


def readFile(fileV4, fileV6, trie):
	
	# open ipv4 file
	input = open(fileV4, "r")
	pattern = '(\d+)\,(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})/(\d{1,2}).*'
	
	for line in input:
		result = re.match(pattern, line)
		if result:
			address = result.group(2)
			length = result.group(3)
			asn = result.group(1)
			update = True
			withdrawal = False
			count = 0
			insertTrie(trie, address, length, asn, update, withdrawal, count)

	# open ipv6 file
	input = open(fileV6, "r")
	pattern = '(\d+)\,(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]).){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]).){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))/(\d{1,3}),.*'

	for line in input:
		result = re.match(pattern, line)
		if result:
			address = result.group(2)
			length = result.group(32)
			asn = result.group(1)
			update = True
			withdrawal = False
			count = 0

			insertTrie(trie, address, length, asn, update, withdrawal, count)
	
	return trie

def main(update, withdrawal, host, port, trie, t, inputfileV4, inputfileV6):
		
		if inputfileV4 and inputfileV6:
			trie = readFile(inputfileV4, inputfileV6, trie)
			print 'trie created'
			

		
		cli = socket( AF_INET,SOCK_STREAM)
		cli.connect((host, port))
		data =""
		msg = ""
		signal.signal(signal.SIGPIPE, signal.SIG_DFL)
		signal.signal(signal.SIGINT, signal.SIG_IGN)
		
		pastTime = 0
		diff = 0
		
		
		while True:
			
			diff = round(time.time()) - pastTime
			
			if diff >= t:
				pastTime = round(time.time())
				#print "times up, lets print the trie"
				printTrie(trie)
				#trie = None
				#trie = datrie.Trie(string.printable)
				diff = 0
				print "Next"
			else:
				data = cli.recv(1024) #14= </BGP_MESSAGE>
				if(re.search('</BGP_MESSAGE>', msg)):
					l = msg.split('</BGP_MESSAGE>', 1)
					bgp_update = l[0] + "</BGP_MESSAGE>"
					bgp_update = string.replace(bgp_update, "<xml>", "")
					d = parse(bgp_update, update, withdrawal)
					msg = ''.join(l[1:])
					for i in d:
						for j in i["prefix"]:
							path = []
							for k in i["as_path"]:
								path.append(str(k.childNodes[0].data))
							#print j["address"] + " " + j["len"] + " " + i["origin_as"]# + " " + str(i["update"]) + " " + str(i["withdrawal"]) + " " + str(i["count"])# +"\t" + string.join(path, ",")
							trie = insertTrie(trie, j["address"], j["len"], i["origin_as"], i["update"], i["withdrawal"], i["count"])
				msg += str(data)
		

if len(sys.argv) < 8:
	print "wrong selection, choose -u for updates and/or -w for withdrawals, -h host, -p port, -t time_in_seconds, [-v4 inputfile, -v6 inputfile]\nExample:\n python bgpmonParser.py -u -w -h livebgp.netsec.colostate.edu -p 50001 -t time_in_seconds [-v4 riswhoisdump.IPv4 -v6 riswhoisdump.IPv6]"
else:
	update = False
	withdrawal = False
	host = ""
	port = 0
	t = 0
	inputfileV4 = None
	inputfileV6 = None
	for i in range(1, len(sys.argv)):
		if(sys.argv[i] == "-u"):
			update = True
		if(sys.argv[i] == "-w"):
			withdrawal = True
		if(sys.argv[i] == "-h"):
			host = sys.argv[i+1]
		if(sys.argv[i] == "-p"):
			port = int(sys.argv[i+1])
		if(sys.argv[i] == "-t"):
			t = int(sys.argv[i+1])
		if(sys.argv[i] == "-v4"):
			inputfileV4 = sys.argv[i+1]
		if(sys.argv[i] == "-v6"):
			inputfileV6 = sys.argv[i+1]
	
	#print str(update) + "\t" + str(withdrawal) + "\t" + host + "\t" + str(port)
	trie = datrie.Trie(string.printable)
	main(update, withdrawal, host, port, trie, t, inputfileV4, inputfileV6)
