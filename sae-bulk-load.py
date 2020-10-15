#!/usr/bin/python3

import json
import csv
import re
import glob, os
import sys
import timeit
from influxdb import InfluxDBClient

config="/home/serkin/Tele2/bulk"				# config folder
bulkDocFile=config+"/BulkstatStatistics_documentation.csv"      # file from StarOS companion archive
bulkDR=config+"/r21-disc-reasons.txt"              		# show session disconnect-reasons verbose output (clean off extra strings except DRs)
bulkCfgFileMME=config+"/r21-mme-schema.txt"        		# show bulkstat schema CLI output
bulkCfgFileSAE=config+"/r21-sae-schema.txt"        		# clean off extra strings except named schemas' srtrings
bulkList=config+"/sae-bulk-schemas.txt"				# Required schemas' list one per line
bulkDir=config+"/files"					# bulkstats files folder
influx = InfluxDBClient(database='bulkstat')			# InfluxDB database connection (local or remote)

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
          row = list(filter(None, row))
          fluxtpl['measurement'] = row[2]
          fluxtpl['time'] = int(row[3])
          for i in range (7,len(row)):
            if bulkDict[row[2]][i-7]['type'] == "Primary-key":
              if bulkDict[row[2]][i-7]['dtype'] == 'INT16' or bulkDict[row[2]][i-7]['dtype'] == 'INT32':
                fluxtpl['tags'][bulkDict[row[2]][i-7]['name']] = int(row[i])
              if bulkDict[row[2]][i-7]['dtype'] == 'INT64' or bulkDict[row[2]][i-7]['dtype'] == 'FLOAT':
                fluxtpl['tags'][bulkDict[row[2]][i-7]['name']] = float(row[i])
              if bulkDict[row[2]][i-7]['dtype'] == 'STRING':
                fluxtpl['tags'][bulkDict[row[2]][i-7]['name']] = str(row[i])
            else:
              if bulkDict[row[2]][i-7]['dtype'] == 'INT16' or bulkDict[row[2]][i-7]['dtype'] == 'INT32':
                if 'nan' in row[i]:
                  row[i]='0.0'
                fluxtpl['fields'][bulkDict[row[2]][i-7]['name']] = int(row[i])
              if bulkDict[row[2]][i-7]['dtype'] == 'INT64' or bulkDict[row[2]][i-7]['dtype'] == 'FLOAT':
                if 'nan' in row[i]:
                  row[i]='0.0'
                fluxtpl['fields'][bulkDict[row[2]][i-7]['name']] = float(row[i])
              if bulkDict[row[2]][i-7]['dtype'] == 'STRING':
                fluxtpl['fields'][bulkDict[row[2]][i-7]['name']] = str(row[i])
          fluxtpl['tags']['node'] = n[0]  # set node tag from filename (see fnam.split above)
          if fluxtpl['measurement'].startswith('sgsnSch') and (fluxtpl['tags']['lac']) != 0:    # Skip non-zero 2G/3G RAI and save only total stats
            continue
          else:
            influxBulk.append(fluxtpl)
          if fluxtpl['measurement'] == 'systemSch71':	                                        # we need to process disconnect reasons separately
            fluxdr = {
              'measurement': '',
              'time': '',
              'tags': {
              },
              'fields': {
              }
            }
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
              fluxdr['measurement'] = 'DiscReasons'
              fluxdr['time'] = int(row[3])
              fluxdr['tags']['node'] = n[0]
              drentry=drlist[i].split('=')
              fluxdr['tags']['disc-reason'] = di['system.disc-reason-'+drentry[0]]['descr']
              fluxdr['fields']['disc-count'] = float(drentry[1])
              influxBulk.append(fluxdr)
      except KeyError: 
        continue
  return influx.write_points(influxBulk,time_precision='s')
#  return(True)

def workOnFile(fnam):
  try:
    f = open(bulkDir+'/'+fnam+'.p')
#    print('skipped '+fnam)
    return(None)
  except FileNotFoundError:
    print('processing '+fnam+' ..',end=' ')
    if processBulk(fnam) is True:
      print('success')
    else:
      print('failed')
      return(False)
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
bulkDict.update(bulkCfgDict(bulkCfgFileMME))
bulkDict.update(bulkCfgDict(bulkCfgFileSAE))

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
#    sys.exit()

sys.exit()
