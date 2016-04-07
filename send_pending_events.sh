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
import MySQLdb as mdb
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
SENT_TIMEOUT = config['sent_timeout']

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

def sendPendingEvent(url, data):
	#data = '{"eventType":40,"resourceId":7,"timestamp":"2016-04-05T18:05:42.00Z","source":"KYROS_API"}'
	#url = 'https://bl.deimos-space.com/WebServicesWMS/rest/listener/notify'
	headers = {"Content-type": "application/json", "X-Access": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE1MjY5ODI1MDA1ODksImlzcyI6InN1bW8iLCJyb2xlIjoiYWRtaW5pc3RyYXRvciJ9.CnX6I1puC-H-0AluXge8X4vVlUOfv8x-Nh6VwzxO-n8"}
	
	try:
		response = requests.post(url, headers=headers, data = data, verify=False, timeout=0.9)
		#print "code:"+ str(response.status_code)
		#print "headers:"+ str(response.headers)
		#print "content:"+ str(response.text)
		
		if (response.status_code!=200):
			logger.debug("Evento enviado: " + data)
			return True
		else:
			return False
	except:
		logger.debug("Error al enviar evento: " + data)


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
		sql = "SELECT ID, URL, EVENT_JSON_DATA, SENT FROM SUMO_PENDING_EVENT where LIMIT_DATE<" + str(epoch_actual) + " and SENT<" + str(MAX_RETRY)
		logger.debug("sql: " + sql)
		cur.execute(sql)
		numrows = int(cur.rowcount)
		if (numrows>0):
			row = cur.fetchone()
			eventId = row[0]
			url = row[1]
			data = row[2]
			retry_counter = row[3]
			# enviar el evento
			# Si se ha enviado bien, lo borro sino incremento en contador de reintentos
			if sendPendingEvent(url, data):
				#borrar el evento
				curDelete = con.cursor()
				sql = "DELETE FROM SUMO_PENDING_EVENT where ID=" + eventId
				logger.debug("sql: " + sql)
				curDelete.execute(sql)
				con.commit()
				curDelete.close()
			else:
				#incrementar el numero de intentos
				curUpdate = con.cursor()
				sql = "UPDATE SUMO_PENDING_EVENT set SENT=" + str(retry_counter + 1) + " where ID=" + str(eventId)
				logger.debug("sql: " + sql)
				curUpdate.execute(sql)
				con.commit()
				curUpdate.close()

	except mdb.Error, e:
		print "Error %d: %s" % (e.args[0], e.args[1])
		sys.exit(1)

	finally:
		if con:
			con.close()

if __name__ == '__main__':
    main()
    #sendPendingEvent()
