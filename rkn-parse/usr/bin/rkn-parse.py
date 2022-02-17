#!/usr/bin/python3
# -*- coding: utf-8 -*-

# Импорты Python
import logging, sys, sqlite3, configparser, os, errno, glob

logging.basicConfig(filename='/var/log/roskom.log', filemode='a', format=u'%(asctime)s %(levelname)s: %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурационный файл
config = configparser.ConfigParser()
config.read('/etc/roskom/tools.ini')

# Общие модули
sys.path.append('/usr/share/roskomtools')
import rknparser

# База данных
databasecfg = config['roskomtools']['database']
if not os.path.exists(databasecfg):
        print("Database DATABASE doesn't exist, creating now....")
        db = sqlite3.connect(databasecfg)
        cursor = db.cursor()
        logging.info("Database DATABASE doesn't exist, creating now....")
        cursor.execute("CREATE TABLE IF NOT EXISTS content (content_id INT, content_block_type TEXT, content_include_time TEXT, content_urgency_type INT, content_entry_type INT, content_hash TEXT, content_ts INT, content_decision_date TEXT, content_decision_number TEXT, content_decision_org TEXT, register TEXT, PRIMARY KEY (content_id, register))")
        cursor.execute("CREATE TABLE IF NOT EXISTS domains (domain_content_id INT, domain_text TEXT, domain_ts INT, register TEXT)")
        cursor.execute("CREATE TABLE IF NOT EXISTS domain_masks (mask_content_id INT, mask_text TEXT, mask_ts INT)")
        cursor.execute("CREATE TABLE IF NOT EXISTS urls (url_content_id INT, url_text TEXT, url_ts INT)")
        cursor.execute("CREATE TABLE IF NOT EXISTS ips (ip_content_id INT, ip_text TEXT, ip_ts INT, register TEXT)")
        cursor.execute("CREATE TABLE IF NOT EXISTS subnets (subnet_content_id INT, subnet_text TEXT, subnet_ts INT, register TEXT)")
        cursor.execute("CREATE TABLE IF NOT EXISTS ipsv6 (ip_content_id INT, ip_text TEXT, ip_ts INT, register TEXT)")
        cursor.execute("CREATE TABLE IF NOT EXISTS subnetsv6 (subnet_content_id INT, subnet_text TEXT, subnet_ts INT, register TEXT)")
        cursor.execute("CREATE INDEX IF NOT EXISTS domain_content_id_idx ON domains (domain_content_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS mask_content_id_idx ON domain_masks (mask_content_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS url_content_id_idx ON urls (url_content_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS ip_content_id_idx ON ips (ip_content_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS subnet_content_id_idx ON subnets (subnet_content_id)")
        print("Database DATABASES created!")
        logging.info("Database DATABASES created!")
else:
        print("DATABASE exists, clear TABLES")
        db = sqlite3.connect(databasecfg)
        cursor = db.cursor()
        logging.info("Clear TABLES")
        cursor.execute("DELETE FROM urls")
        cursor.execute("DELETE FROM domains")
        cursor.execute("DELETE FROM ips")
        cursor.execute("DELETE FROM subnets")
        cursor.execute("DELETE FROM ipsv6")
        cursor.execute("DELETE FROM subnetsv6")
        cursor.execute("DELETE FROM content")
        cursor.execute("DELETE FROM domain_masks")
cursor.close()
db.commit()

print("Parsing the registry...")
logging.info("Start parsing the registry...")

def try_process(filename, db):
	try:
                if filename == 'dump.xml':
                        register = "PR"
                elif filename == 'register.xml':
                        register = "SR"
                else:
                        register = ""
                rknparser.parse_registry(filename, db, register)
#		rknparser.resolve_all(filename, db)
	except OSError as e:
		print("dump.xml is not accessible")
		logging.error('%s is not accessible' % (filename))
	except:
		print("Parsing failed")
		logging.error('Parsing failed!')
	else:
		print("Finished")
		logging.info('Parsing finished.')

arr = os.listdir()
if os.isatty(sys.stdin.fileno()):
        arr = os.listdir('./')
else:
        arr = os.listdir('/var/lib/roskomtools/')

for file in glob.glob("*.xml"):
        try_process(file, db)
