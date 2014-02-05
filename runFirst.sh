#!/bin/bash

# get basic IPv4 riswhoisdump
wget -N http://www.ris.ripe.net/dumps/riswhoisdump.IPv4.gz &> /dev/null
gunzip -f riswhoisdump.IPv4.gz
sed '/^%/d' riswhoisdump.IPv4 | sed '/^$/d' | awk '$3 > 4 { print $0 }' | cut -f1,2 | sed -r '/^\{[0-9]*,[0-9]*/d' | sed 's/\t/,/g' | sed 's/{//g' | sed 's/}//g' | awk '{ print $0 ",U,,,4"}' | uniq > riswhoisdump.IPv4.new
mv riswhoisdump.IPv4.new riswhoisdump.IPv4

# get basic IPv6 riswhoisdump
wget -N http://www.ris.ripe.net/dumps/riswhoisdump.IPv6.gz &> /dev/null
gunzip -f riswhoisdump.IPv6.gz
sed '/^%/d' riswhoisdump.IPv6 | sed '/^$/d' | awk '$3 > 4 { print $0 }' | cut -f1,2 | sed -r '/^\{[0-9]*,[0-9]*/d' | sed 's/\t/,/g' | sed 's/{//g' | sed 's/}//g' | awk '{ print $0 ",U,,,6"}' | uniq > riswhoisdump.IPv6.new
mv riswhoisdump.IPv6.new riswhoisdump.IPv6

# get rir data
wget -N http://www.iana.org/assignments/ipv4-address-space/ipv4-address-space.txt
grep -Eo "whois.*.net" ipv4-address-space.txt | cut -d '.' -f2 > row2
grep whois.*.net ipv4-address-space.txt | grep -oE "[0-9]{3}/8" | cut -d '/' -f1 | sed -E 's/^0*//' > row1
paste --delimiter=' ' row1 row2 > rirs
rm row1 row2
rm ipv4-address-space.txt