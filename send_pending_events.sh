#!/usr/bin/env python
#-*- coding: UTF-8 -*-

# autor: Carlos Rueda
# date: 2015-12-21
# mail: carlos.rueda@deimos-space.com
# version: 1.0

########################################################################
# version 1.0 release notes:
# Initial version
########################################################################

from __future__ import division
import time
import datetime
import os
import sys
import calendar
import logging, logging.handlers
import json
from threading import Thread
import MySQLdb
import requests

########################################################################
# configuracion y variables globales
from configobj import ConfigObj
config = ConfigObj('./send_pending_events.properties')

LOG = config['directory_logs'] + "/send_pending_events.log"
LOG_FOR_ROTATE = 10

BBDD_HOST = config['BBDD_host']
BBDD_PORT = config['BBDD_port']
BBDD_USERNAME = config['BBDD_username']
BBDD_PASSWORD = config['BBDD_password']
BBDD_NAME = config['BBDD_name']
MAX_RETRY = config['max_retry']

DEFAULT_SLEEP_TIME = float(config['sleep_time'])

PID = "/var/run/ais_dispatcher"
########################################################################

# Se definen los logs internos que usaremos para comprobar errores
try:
    logger = logging.getLogger('send_pending_events')
    loggerHandler = logging.handlers.TimedRotatingFileHandler(LOG, 'midnight', 1, backupCount=10)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    loggerHandler.setFormatter(formatter)
    logger.addHandler(loggerHandler)
    logger.setLevel(logging.DEBUG)
except:
    print '------------------------------------------------------------------'
    print '[ERROR] Error writing log at %s' % LOG
    print '[ERROR] Please verify path folder exits and write permissions'
    print '------------------------------------------------------------------'
    exit()

########################################################################

if os.access(os.path.expanduser(PID), os.F_OK):
        print "Checking if send_pending_events process is already running..."
        pidfile = open(os.path.expanduser(PID), "r")
        pidfile.seek(0)
        old_pd = pidfile.readline()
        # process PID
        if os.path.exists("/proc/%s" % old_pd) and old_pd!="":
            print "You already have an instance of the send_pending_events process running"
            print "It is running as process %s" % old_pd
            sys.exit(1)
        else:
            print "Trying to start send_pending_events process..."
            os.remove(os.path.expanduser(PID))

#This is part of code where we put a PID file in the lock file
pidfile = open(os.path.expanduser(PID), 'a')
print "send_pending_events process started with PID: %s" % os.getpid()
pidfile.write(str(os.getpid()))
pidfile.close()

########################################################################

########################################################################
# Definicion de funciones
#
########################################################################

def sendPendingEvent():
	 #data = '{"query":{"bool":{"must":[{"text":{"record.document":"SOME_JOURNAL"}},{"text":{"record.articleTitle":"farmers"}}],"must_not":[],"should":[]}},"from":0,"size":50,"sort":[],"facets":{}}'
	 data = '{"eventType":40,"resourceId":7,"timestamp":"2016-04-05T18:05:42.00Z","source":"KYROS_API"}'
	 url = 'https://bl.deimos-space.com/WebServicesWMS/rest/listener/notify'
	 headers = {"Content-type": "application/json", "X-Access": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE1MjY5ODI1MDA1ODksImlzcyI6InN1bW8iLCJyb2xlIjoiYWRtaW5pc3RyYXRvciJ9.CnX6I1puC-H-0AluXge8X4vVlUOfv8x-Nh6VwzxO-n8"}
	 #resp = requests.post(url, data=data, allow_redirects=True)
	 response = requests.post(url, headers=headers, data = data, verify=False)
	 #response = requests.api.request('post', url, data={'bar':'baz'}, verify=False)
	 print "code:"+ str(response.status_code)
	 print "******************"
	 print "headers:"+ str(response.headers)
	 print "******************"
	 print "content:"+ str(response.text)


########################################################################
# Funcion principal
#
########################################################################

def main():
    con = None
    try:
        con = mdb.connect(BBDD_HOST, BBDD_USERNAME, BBDD_PASSWORD, BBDD_NAME)
        cur = con.cursor()

		epoch_actual = calendar.timegm(time.gmtime())
        sql = "SELECT URL, EVENT_JSON_DATA, SENT FROM SUMO_PENDING_EVENT where LIMIT_DATE<" + epoch_actual + " and SENT<" + MAX_RETRY
        logger.debug("sql: " + sql)
		cur.execute(sql)
		numrows = int(cur.rowcount)
		if (numrows>0):
			row = cur.fetchone()
			url = row[0]
			data = row[1]
			retry_counter = row[2]
			# enviar el evento



    except mdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        sys.exit(1)

    finally:
        if con:
            con.close()

if __name__ == '__main__':
    #main()
    sendPendingEvent()

sendPendingEvent()