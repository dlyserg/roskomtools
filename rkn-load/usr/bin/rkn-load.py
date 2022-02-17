#!/usr/bin/python3
# -*- coding: utf-8 -*-

## Package: rkn-load
## Version: 2.0-1
## Section: misc
## Architecture: all
## Depends: bash, python3-suds, rkn-common
## Maintainer: Margarita Kosobokova <zolotko92@gmail.net>
## Editor: Lyserg Diethel <kill9pid@hotmail.com>
## Priority: extra
## Description: roskom registry load


# Python
import logging, sys, base64, signal, time, zipfile, os, configparser, sqlite3

# SUDS
from suds.client import Client
from suds.sax.text import Text

logging.basicConfig(filename='/var/log/roskom.log', filemode='a', format=u'%(asctime)s %(levelname)s: %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурационный файл
config = configparser.ConfigParser()
config.read('/etc/roskom/tools.ini')

# База данных
db = sqlite3.connect(config['roskomtools']['database'])

cursor = db.cursor()
cursor.execute("CREATE TABLE IF NOT EXISTS loads (load_id INTEGER PRIMARY KEY AUTOINCREMENT, load_when INTEGER, load_code TEXT, load_state INTEGER)")
cursor.close()
db.commit()

def file_get_contents(filename):
	try:
		with open(filename, 'rb') as f:
			data = f.read()
	except Exception as e:
		print(e)
		return b''
	else:
		return data
def get_result(self, getMethod, code, filename):
        try:
                delay = int(config['load']['delay'])
        except:
                delay = 30

        while True:
                logger.info('Trying to get result...')
                self.print_message("Waiting %d seconds" % (delay,))
                time.sleep(delay)
                self.print_message("Checking result")
                logger.info('Checking result status.')
                result = None
                try:
                        result = getMethod(code)
                except Exception as e:
                        self.handle_exception(e)

                if (result is not None) and ('result' in result) and result['result']:
                        logging.info('Result received.')
                        self.print_message("Got proper result, writing zip file")
                        logging.info('Got result dump ver. %s' % (result['dumpFormatVersion']))
                        try:
                                zip_archive = result['registerZipArchive']
                                data = base64.b64decode(zip_archive)
                                with open(filename, 'wb') as file:
                                        data = base64.b64decode(zip_archive)
                                        file.write(data)
                                logging.info('Downloaded dump %d bytes, MD5 hashsum: %s', os.path.getsize(filename), hashlib.md5(open(filename,'rb').read()).hexdigest())
                                self.print_message("ZIP file: %s - saved" % (filename))
                                logging.info('ZIP file: %s - saved.' % (filename))
                                try:
                                        logging.info('Unpacking.')
                                        with zipfile.ZipFile(filename, 'r') as file:
                                                if self.console:
                                                        file.extractall()
                                                else:
                                                        file.extractall('/var/lib/roskomtools')
                                                file.close()
                                        self.print_message("ZIP file extracted")
                                        self.print_message("Job done!")
                                        break
                                except zipfile.BadZipfile:
                                        logger.error('Wrong file format.')
                        except Exception as e:
                                logger.error('Somthing get wrong!')
                                self.handle_exception(e)
                else:
                        try:
                                if result['resultComment'].decode('utf-8') == 'запрос обрабатывается':
                                        self.print_message("Still not ready")
                                        continue
                                else:
                                        error = result['resultComment'].decode('utf-8')
                                        self.print_message("getRclientesult failed with code %d: %s" % (result['resultCode'], error))
                                        logger.error('getRclientesult failed with code %d: %s' % (result['resultCode'], error))
                                        exit(-1)
                        except Exception as e:
                                logger.info('No updates.')
                                self.handle_exception(e)

class RoskomAPI:
	def __init__(self, url):
		self.url = url
		# Загрузим данные из файлов
		request_xml = file_get_contents('/etc/roskom/request.xml')
		request_xml_sign = file_get_contents('/etc/roskom/request.xml.sign')

		# Представим данные в нужном виде
		self.request_xml = base64.b64encode(request_xml).decode('utf-8')
		self.request_xml_sign = base64.b64encode(request_xml_sign).decode('utf-8')

		self.client = Client(url)
		self.service = self.client.service

	def getLastDumpDate(self):
               ''' возвращает только один параметр: lastDumpDate - время последнего обновления выгрузки. оставлен для совместимости. '''
		return self.service.getLastDumpDate()

	def getLastDumpDateEx(self):
                ''' возвращает метку последнего обновления выгрузки из реестра, информацию о версиях веб-сервиса, памятку и текущий формат выгрузки '''
                return self.service.getLastDumpDateEx()

	def sendRequest(self):
              	''' направление запроса на получение выгрузки из реестра '''
		response = self.service.sendRequest(self.request_xml, self.request_xml_sign, '2.2')
		return dict(((k, v.encode('utf-8')) if isinstance(v, Text) else (k, v)) for (k, v) in response)

	def getResult(self, code):
               ''' получение результата обработки запроса - выгрузки из реестра запрещенных ресурсов '''
		response = self.service.getResult(code)
		return dict(((k, v.encode('utf-8')) if isinstance(v, Text) else (k, v)) for (k, v) in response)

	def getResultSocResources(self, code):
                ''' получение результата обработки запроса - выгрузки из реестра социально значимых ресурсов '''
                response = self.service.getResultSocResources(code)
                return dict(((k, v.encode('utf-8')) if isinstance(v, Text) else (k, v)) for (k, v) in response)

class Command(object):
	client = None
	service = None
	code = None
	api = None
	console = True

	def print_message(self, message):
		if self.console:
			print(message)

	def handle_signal(self, signum, frame):
		print("Exitting on user's request")
		exit(0)

	def handle_exception(self, e):
		print(str(e))
		exit(-1)

	def handle(self, db, console = True):
		self.console = console
		signal.signal(signal.SIGTERM, self.handle_signal)
		signal.signal(signal.SIGQUIT, self.handle_signal)
		signal.signal(signal.SIGINT, self.handle_signal)

		url = 'http://vigruzki.rkn.gov.ru/services/OperatorRequest/?wsdl'
		#url = "http://vigruzki.rkn.gov.ru/services/OperatorRequestTest/?wsdl"

		try:
			self.print_message("Connecting to the API")
			self.api = RoskomAPI(url)
			self.print_message("API connection succeeded")
			logging.info('RKN API connection established.')
		except Exception as e:
			self.handle_exception(e)

		if self.api.request_xml == "" or self.api.request_xml_sign == "":
			self.print_message("No data in request.xml or in request.xml.sign")
			logging.error('No data in request.xml or in request.xml.sign')
			exit(-1)

		# Фактическая и записанная даты, можно сравнивать их и в зависимости от этого делать выгрузку, но мы сделаем безусловную
		try:
                        dump = self.api.getLastDumpDateEx()
                        dump_date = int(max(dump.lastDumpDate, dump.lastDumpDateUrgently) / 1000)
			#dump_date = int(int(self.api.getLastDumpDate()) / 1000)
			our_last_dump = 0

			if dump_date > our_last_dump:
				self.print_message("New registry dump available, proceeding")
				logging.info('Dump registry has updates since last sync.')
			else:
				self.print_message("No changes in dump.xml, but forcing the process")
				logging.info('No changes, nothing do it.')
		except Exception as e:
			self.handle_exception(e)

		cursor = db.cursor()
		when = int(time.time())
		data = (when, '', 1)
		cursor.execute("INSERT INTO loads (load_when, load_code, load_state) VALUES (?, ?, ?)", data)
		load_id = cursor.lastrowid
		cursor.close()
		db.commit()

		try:
			self.print_message("Sending request")
			response = self.api.sendRequest()
			self.print_message("Request sent")
			self.code = response['code'].decode('utf-8')
		except Exception as e:
			self.handle_exception(e)

		cursor = db.cursor()
		data = (self.code, load_id)
		cursor.execute("UPDATE loads SET load_code = ?, load_state = 2 WHERE load_id = ?", data)
		cursor.close()
		db.commit()

		logging.info('Current versions: webservice: %s, dump: %s, soc: %s, doc: %s' % (dump.webServiceVersion, dump.dumpFormatVersion, dump.dumpFormatVersionSocResources, dump.docVersion))
                try:
                        self.print_message("Sending request")
                        logger.info('Sending request.')
                        response = self.api.sendRequest()
                        self.print_message("Request sent")
                        self.code = response['code'].decode('utf-8')
                        logger.info('Got code %s', self.code)
                except Exception as e:
                        self.handle_exception(e)

                cursor = db.cursor()
                data = (self.code, load_id)
                cursor.execute("UPDATE loads SET load_code = ?, load_state = 2 WHERE load_id = ?", data)
                cursor.close()
                db.commit()

                logging.info('Getting unloading the registry of blocked resources.')
                filenamezapret = "zapret.zip"
                get_result(self, self.api.getResult, self.code, filenamezapret)

                logging.info('Getting unloading the registry of of socially significant resources.')
                filenamesoc = "soc.zip"
                get_result(self, self.api.getResultSocResources, self.code, filenamesoc)

                cursor = db.cursor()
                cursor.execute("UPDATE loads SET load_state = 0 WHERE load_id = ?", (load_id,))
                cursor.close()
                db.commit()

if __name__ == '__main__':
	command = Command()
	console = os.isatty(sys.stdin.fileno())
	command.handle(db, console)
