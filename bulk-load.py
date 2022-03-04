#!/usr/bin/python3

import json
import csv
import re
import glob, os
import sys
import timeit
from influxdb_client import WritePrecision, InfluxDBClient, Point
from influxdb_client.client.write.retry import WritesRetry
from influxdb_client.client.write_api import SYNCHRONOUS
from multiprocessing import Pool
import time

# URL for Influxdb2 access
url="http://localhost:8086"
# bucket to store measurements into influxdb2
bucket="bulkstat/yearly"
# Access token for Influxdb2
token="9iDwnBC78yjiG9hGFgvdiXSAHn2_t3uctiXK9LwErtLGb53ZHBfh_RdLlGGKoujjiIonppNQR-FZKq1GhqqcaA=="
# Organization name
org="CPM Ltd"
# config folder
config="/home/serkin/Tele2/bulk"
# Path to bulkstats counters description (from companion-vpc-AA.BB.CC.tgz where AABBCC - StarOS release used)
bulkDocFile=config+"/BulkstatStatistics_documentation.csv"
# Disconnect reasons description (output from "show session disconnect-reasons verbose" command)
bulkDR=config+"/r21-disc-reasons.txt"
# MME bulkstats schema configuration (output from "show bulkstats schema" on MME)
bulkCfgFileMME=config+"/r21-mme-schema.txt"
# SAE bulkstats schema configuration (output from "show bulkstats schema" on SAEGW)
bulkCfgFileSAE=config+"/r21-sae-schema.txt"
# list of schemas' names used to collect statistics from
bulkList=config+"/bulk-schemas.txt"
# Path to the uncompressed bulkstats files
bulkDir=config+"/mmefiles"

def readWorkingSchemas(fileN):
  lineDict={}
  for line in open(fileN):
    lineDict[line.rstrip('\n')]='true'
  return lineDict

def bulkDocDict(bulkdoc,dr):

# Read bulk stats documentation and disconnect reasons csv files (bulkdoc,drdoc) and build dictionary in the form of:
# schema.counter: 
#   'schema': row[0],
#   'counter': row[1],
#   'dtype': row[2], # Data type: INT16, INT32, INT64, FLOAT, STRING
#   'stype': row[3], # Incremental, Gauge, Primary-Key
#   'status': row[8],# Standard, Proprietary
#   'descr': row[5],  # Description
#   'drid':           # Disconnect Reason ID

  dictlist=[];
  docdict={}
  dot='.'
  dochead=['Schema','Counter','Data-type','Statistics-type','Change','Description','Triggers','Availability','Standard or Proprietary']
  fluxtpl = {
              "measurement": "",
              "time": "",
              "tags": {
              },
              "fields": {
              }
            }
# Read from bulks stats documentation csv file
  fh=open(bulkdoc,"r")
  bulklines=fh.readlines();
  for line in (bulklines):
    line1=line.replace('\\"', '\'')
    lines=line1.splitlines()
    for row in csv.reader(lines, delimiter=',', quotechar='"'):
      docdict.update({dot.join([row[0],row[1]]):
                       {'schema': row[0],
                        'var': row[1],
                        'dtype': row[2],
                        'stype': row[3],
                        'status': row[8],
                        'descr': row[5],
                        'drid': 'N/A'
                       }
                     }
                    )
  fh.close()
# Read from  bulks stats disconnect reasons csv file
  fh=open(dr,"r")
  bulklines=fh.readlines()
  for line in (bulklines):
    row=re.split(r'[\(\)\s\t]',line)
    docdict.update({dot.join(['system','disc-reason-'+row[1]]):
                     {'schema': 'system',
                      'var': 'disc-reason-'+row[1],
                      'dtype': 'INT64',
                      'stype': 'Incremental',
                      'status': 'Standard',
                      'descr': row[0],
                      'drid': row[1]
                      }
                   }
                  )
  fh.close()
  return docdict
# end of bulkDocDict

def bulkCfgDict(bulkcfg):
  schemaDict={}
  dot='.'
  fh=open(bulkcfg,"r")
  bulklines=fh.readlines();
  for line in (bulklines):
    schemaStr=[]
    lines=line.splitlines()
    for row in csv.reader(lines, delimiter=',', quotechar='%'):
      for i in range (7,len(row)-1):
        try:
          if workingSchemas[row[2]]:
            if len(row[i])!=0:
              schemaStr.append({'name': row[i], 'type': di[dot.join([row[1],row[i]])]['stype'], 'dtype': di[dot.join([row[1],row[i]])]['dtype']})
            else:
              schemaStr.append({'name': 'dummy-'+str(i), 'type': 'Incremental', 'dtype': 'INT32'})
        except KeyError: 
          continue
      try:
        if workingSchemas[row[2]]:
          schemaDict[row[2]]=schemaStr
      except KeyError: 
        continue
  fh.close()
  return schemaDict
# end of bulkCfgDict

def processBulk(fnam):
  influxBulk = []
  n=fnam.split('_')
  f = open(fnam,"r")
  bulklines=f.readlines()[1:-1];
  for line in (bulklines):
    lines=line.splitlines()
    for row in csv.reader(lines, delimiter=',',quotechar='"'):
      try:
        if workingSchemas[row[2]]:
          fluxtpl = {
              'measurement': '',
              'time': '',
              'tags': {
              },
              'fields': {
              }
            }
          if row[2] == 'systemSch71' and len(row[10]) == 0:
            row[10] = "0=0;"
          fluxtpl['measurement'] = row[2]
          fluxtpl['time'] = int(row[3])
          if row[11].startswith('%vlrname%'):     # skip fucking broken bulk string
              continue
          for i in range (7,len(bulkDict[row[2]])+7):
            if bulkDict[row[2]][i-7]['type'] == "Primary-key":
              if bulkDict[row[2]][i-7]['dtype'] == 'INT16' or bulkDict[row[2]][i-7]['dtype'] == 'INT32':
                fluxtpl['tags'][bulkDict[row[2]][i-7]['name']] = int(row[i])
              if bulkDict[row[2]][i-7]['dtype'] == 'INT64' or bulkDict[row[2]][i-7]['dtype'] == 'FLOAT':
                fluxtpl['tags'][bulkDict[row[2]][i-7]['name']] = float(row[i])
              if bulkDict[row[2]][i-7]['dtype'] == 'STRING':
                fluxtpl['tags'][bulkDict[row[2]][i-7]['name']] = str(row[i])
            else:
              if len(row[i])==0:
                row[i]=0
              if str(row[i]).find('nan') != -1:
                row[i]=0
              if bulkDict[row[2]][i-7]['dtype'] == 'INT16' or bulkDict[row[2]][i-7]['dtype'] == 'INT32':
                fluxtpl['fields'][bulkDict[row[2]][i-7]['name']] = int(row[i])
              if bulkDict[row[2]][i-7]['dtype'] == 'INT64' or bulkDict[row[2]][i-7]['dtype'] == 'FLOAT':
                fluxtpl['fields'][bulkDict[row[2]][i-7]['name']] = float(row[i])
              if bulkDict[row[2]][i-7]['dtype'] == 'STRING':
                fluxtpl['fields'][bulkDict[row[2]][i-7]['name']] = str(row[i])
          fluxtpl['tags']['node'] = n[0]  # set node tag from filename (see fnam.split above)
          if fluxtpl['measurement'].startswith('sgsnSch') and (fluxtpl['tags']['lac']) != 0:    # Skip non-zero 2G/3G RAI and save only total stats
            continue
          else:
#            print(json.dumps(fluxtpl, indent=2))
            influxBulk.append(fluxtpl)
          if fluxtpl['measurement'] == 'systemSch71':	                                        # we need to process disconnect reasons separately
            drlist=fluxtpl['fields']['disc-reason-summary'].split(';')
            for i in range (0,len(drlist)-1):
              fluxdr = {
                'measurement': '',
                'time': '',
                'tags': {
                },
                'fields': {
                }
              }
              drentry=drlist[i].split('=')
              fluxdr['measurement'] = 'DiscReasons'
              fluxdr['time'] = int(row[3])
              fluxdr['tags']['node'] = n[0]
              fluxdr['tags']['disc-reason'] = di['system.disc-reason-'+drentry[0]]['descr']
              fluxdr['fields']['disc-count'] = float(drentry[1])
              influxBulk.append(fluxdr)
      except KeyError: 
        continue
  try:
    write_api.write(bucket=bucket, org=org, record=influxBulk, write_precision=WritePrecision.S)
    return True
  except Exception as exc:
    return exc
  finally:
    write_api.close()

def workOnFile(fnam):
  try:
    f = open(bulkDir+'/'+fnam+'.p')
    return(None)
  except FileNotFoundError:
    print('processing '+fnam+' ..',end=' ')
    result = processBulk(fnam)
    if result is True:
      print('success')
    else:
      print(result)
      retirn(None)
  try:
    f = open(bulkDir+'/'+fnam+'.p', 'w')
    return(True)
  except FileExistsError:
    return(None)
  f.close()

#
# Main
# run: bulk-load.py <hostname>, where hostname - node to process bulkstats

fList=sys.argv[1]+"*.csv"
print ('looking for files:', fList)

workingSchemas=readWorkingSchemas(bulkList)
di=bulkDocDict(bulkDocFile,bulkDR)
bulkDict={}
"""
 Building array of dictionaries for bulk schemas. Enumerated from '0'
'gprsSch10': 
[{'type': 'Primary-key', 'dtype': 'STRING', 'name': 'vpnname'}, 
{'type': 'Primary-key', 'dtype': 'INT32', 'name': 'vpnid'}, 
{'type': 'Primary-key', 'dtype': 'STRING', 'name': 'servname'}, 
{'type': 'Primary-key', 'dtype': 'INT32', 'name': 'nse-id'}, 
{'type': 'Incremental', 'dtype': 'INT64', 'name': 'llc-frame-stats-ui-unciph-rx'}, 
{'type': 'Incremental', 'dtype': 'INT64', 'name': 'llc-frame-stats-ui-unciph-tx'}, 
{'type': 'Incremental', 'dtype': 'INT64', 'name': 'llc-frame-stats-ui-unciph-data-frames-rx'}, 
{'type': 'Incremental', 'dtype': 'INT64', 'name': 'llc-frame-stats-ui-unciph-data-frames-tx'}, 
{'type': 'Incremental', 'dtype': 'INT64', 'name': 'llc-frame-stats-ui-unciph-data-octets-rx'}, 
{'type': 'Incremental', 'dtype': 'INT64', 'name': 'llc-frame-stats-ui-unciph-data-octets-tx'}, 
{'type': 'Incremental', 'dtype': 'INT64', 'name': 'llc-frame-stats-xid-rcvd'}, 
{'type': 'Incremental', 'dtype': 'INT64', 'name': 'llc-frame-stats-xid-sent'}, 
{'type': 'Incremental', 'dtype': 'INT64', 'name': 'bytes-sent-to-bsc'}, 
{'type': 'Incremental', 'dtype': 'INT64', 'name': 'packets-sent-to-bsc'}, 
{'type': 'Incremental', 'dtype': 'INT64', 'name': 'bytes-rcvd-from-bsc'}, 
{'type': 'Incremental', 'dtype': 'INT64', 'name': 'packets-rcvd-from-bsc'}, 
{'type': 'Gauge', 'dtype': 'INT32', 'name': 'gprs-num-subs-gea0-capable'}, 
{'type': 'Gauge', 'dtype': 'INT32', 'name': 'gprs-num-subs-gea1-capable'}, 
{'type': 'Gauge', 'dtype': 'INT32', 'name': 'gprs-num-subs-gea2-capable'}, 
{'type': 'Gauge', 'dtype': 'INT32', 'name': 'gprs-num-subs-gea3-capable'}, 
{'type': 'Gauge', 'dtype': 'INT32', 'name': 'gprs-num-subs-gea0-negotiated'}, 
{'type': 'Gauge', 'dtype': 'INT32', 'name': 'gprs-num-subs-gea1-negotiated'}, 
{'type': 'Gauge', 'dtype': 'INT32', 'name': 'gprs-num-subs-gea2-negotiated'}, 
{'type': 'Gauge', 'dtype': 'INT32', 'name': 'gprs-num-subs-gea3-negotiated'}],
"""
bulkDict.update(bulkCfgDict(bulkCfgFileMME))
# print('\r')
# print(json.dumps(bulkDict['cardSch8'], indent=2))
# print(json.dumps(bulkDict['gprsSch10'], indent=2))
# sys.exit()
# print('\r')
bulkDict.update(bulkCfgDict(bulkCfgFileSAE))


retries = WritesRetry(total=3, retry_interval=1, exponential_base=2)
client = InfluxDBClient(url=url, token=token, org=org, retries=retries, enable_gzip=True)
write_api = client.write_api(write_options=SYNCHRONOUS)

os.chdir(bulkDir)
blist=[]
for bfile in sorted(glob.glob(fList)):
  blist += [bfile]
for fil in blist:
  if workOnFile(fil) is True:
    continue
  else:
    continue
    print(fil)

client.close()
sys.exit()
