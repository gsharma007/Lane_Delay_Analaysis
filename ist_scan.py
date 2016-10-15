from sys import argv
#script, input_file_name = argv
import csv
import json
import sys
import os
script, wd = argv
pp = wd
os.chdir(pp)

input_file = csv.DictReader(open("ist_samplechennai.csv"))
output_file = csv.writer(open("ist_scannedchennai.csv", "wb+"))
output_file.writerow(["conn.cdn","conn.id","conn.vh","conn.o","conn.d","sd","sl","ss"])
csv.field_size_limit(sys.maxsize)
i=0
for row in input_file:
    if row.get("s",None):
        for scan in json.loads(row.get("s",None)):
            try:
                output_file.writerow([row.get("conn.cdn",""),row.get("conn.id",""),row.get("conn.vh",""),row.get("conn.o",""),row.get("conn.d",""),scan.get("sd",""),scan.get("sl",""),scan.get("ss","")])
            except Exception,e:
                i=i+1
    else:
        i=i+1
        pass
print i
