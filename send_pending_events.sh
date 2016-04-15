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


########################################################################
# Definicion de funciones
#
########################################################################

def sendPendingEventTest():
	#data = '{"eventType":40,"resourceId":7,"timestamp":"2016-04-05T18:05:42.00Z","source":"KYROS_API"}'
	data = '{"eventType":32,"resourceId":13942,"latitude":54.83243888888889,"longitude":14.027138888888889,"timestamp":"2016-04-12T08:39:50.00Z","expirationTime":"2016-04-12T09:39:50.00Z","source":"KYROS_API"}'
	url = 'https://bl.deimos-space.com/WebServicesWMS/rest/listener/notify'
	headers = {"Content-type": "application/json", "X-Access": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE1MjY5ODI1MDA1ODksImlzcyI6InN1bW8iLCJyb2xlIjoiYWRtaW5pc3RyYXRvciJ9.CnX6I1puC-H-0AluXge8X4vVlUOfv8x-Nh6VwzxO-n8"}	
	try:
		response = requests.post(url, headers=headers, data = data, verify=False, timeout=2)
		print "code:"+ str(response.status_code)
		#print "headers:"+ str(response.headers)
		#print "content:"+ str(response.text)
	except:
		print "Error al enviar evento"

def sendPendingEvent(url, data):
	headers = {"Content-type": "application/json", "X-Access": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE1MjY5ODI1MDA1ODksImlzcyI6InN1bW8iLCJyb2xlIjoiYWRtaW5pc3RyYXRvciJ9.CnX6I1puC-H-0AluXge8X4vVlUOfv8x-Nh6VwzxO-n8"}	
	try:
		logger.debug("--->Envio de evento. data: " + data)
		logger.debug("--->Envio de evento. url: " + url)
		response = requests.post(url, headers=headers, data = data, verify=False, timeout=2)
		print "code:"+ str(response.status_code)
		if (response.status_code == 200):
			logger.info("RESPUESTA: " + str(response.status_code))
			logger.debug("Evento enviado: " + data)
			return True
		else:
			logger.debug("Codigo de error al enviar el evento: " + str(response.status_code))
			return False
	except:
		logger.debug("Error al enviar evento: " + data)
	return False

########################################################################
# Funcion principal
#
########################################################################

def main():
	while True:
		con = None
		try:
			time.sleep(30)
			logger.info("------------------------------")
			con = mdb.connect(BBDD_HOST, BBDD_USERNAME, BBDD_PASSWORD, BBDD_NAME)
			cur = con.cursor()

			actualUTC = long(datetime.datetime.utcnow().strftime('%s'))*1000

			# limpieza. Borro los que tengan el numero de intentos excedido y los que hayan caducado
			curDelete = con.cursor()
			sqlDelete = "DELETE FROM SUMO_PENDING_EVENT where LIMIT_DATE<" + str(actualUTC) + " OR SENT=" + str(MAX_RETRY)
			logger.debug("sql: " + sqlDelete)
			cur.execute(sqlDelete)
			con.commit()
			curDelete.close()

			#sql = "SELECT ID, URL, EVENT_JSON_DATA, SENT FROM SUMO_PENDING_EVENT where LIMIT_DATE>" + str(actualUTC) + " and SENT<" + str(MAX_RETRY)
			sql = "SELECT ID, URL, EVENT_JSON_DATA, SENT FROM SUMO_PENDING_EVENT where LIMIT_DATE>" + str(actualUTC) + " order by EVENT_DATE desc"
			logger.debug("sql: " + sql)
			cur.execute(sql)
			numrows = int(cur.rowcount)
			logger.debug("Eventos a enviar: " + str(numrows))
			row = cur.fetchone()
			while row is not None:			
				eventId = row[0]
				url = row[1]
				data = row[2]
				retry_counter = row[3]
				# enviar el evento
				# Si se ha enviado bien, lo borro sino incremento en contador de reintentos
				#if sendPendingEventTest():
				if sendPendingEvent(url, data):
					#borrar el evento
					curDeletePending = con.cursor()
					sql = "DELETE FROM SUMO_PENDING_EVENT where ID=" + str(eventId)
					logger.debug("sql: " + sql)
					curDeletePending.execute(sql)
					con.commit()
					curDeletePending.close()
				else:
					#incrementar el numero de intentos
					curUpdatePending = con.cursor()
					sql = "UPDATE SUMO_PENDING_EVENT set SENT=" + str(retry_counter + 1) + " where ID=" + str(eventId)
					logger.debug("sql: " + sql)
					curUpdatePending.execute(sql)
					con.commit()
					curUpdatePending.close()
				row = cur.fetchone()

			cur.close()

		except mdb.Error, e:
			logger.error ("Error %d: %s" % (e.args[0], e.args[1]))
			#sys.exit(1)

		finally:
			if con:
				try:
					con.close()
				except:
					pass
			con = None

	#time.sleep(30)

if __name__ == '__main__':
    main()
    #sendPendingEventTest()
