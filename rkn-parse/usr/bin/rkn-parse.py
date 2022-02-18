#!/usr/bin/python3
# -*- coding: utf-8 -*-

## Package: rkn-parse
## Version: 2.0-1
## Section: misc
## Architecture: all
## Depends: bash, python3, python3-lxml, rkn-load, rkn-common
## Maintainer: Ilya Averkov <ilya@mkpnet.ru>
## Editor: Lyserg Diethel <kill9pid@hotmail.com>
## Priority: extra
## Description: roskom registry parser

# Импорты Python
import logging, sys, sqlite3, configparser, os, errno, glob

# Конфигурационный файл
config = configparser.ConfigParser()
config.read('/etc/roskom/tools.ini')

# Общие модули
sys.path.append('/usr/share/roskomtools')
import rknparser

logging.basicConfig(filename=config['log']['logfile'], filemode='a', format=u'%(asctime)s %(levelname)s: %(message)s', level=logging.INFO)

# База данных
db = sqlite3.connect(config['roskomtools']['database'])
if db is not None:
        cursor = db.cursor()
        tblname = ['urls', 'domains', 'ips', 'subnets', 'ipsv6', 'subnetsv6', 'content', 'domain_masks']
        for tableName in tblname:
                cursor.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name=?; ''', (tableName,))
                if cursor.fetchone()[0] == 1 :
                        logging.info("TABLES: %s exist. Clear." % (tableName))
                        cursor.execute('DELETE FROM {}'.format(tableName))
                else:
                        if tableName == 'urls':
                                cursor.execute('CREATE TABLE IF NOT EXISTS urls (url_content_id INT, url_text TEXT, url_ts INT)')
                                cursor.execute('CREATE INDEX IF NOT EXISTS url_content_id_idx ON urls (url_content_id)')
                        elif tableName == 'domains':
                                cursor.execute('CREATE TABLE IF NOT EXISTS domains (domain_content_id INT, domain_text TEXT, domain_ts INT, register TEXT)')
                                cursor.execute('CREATE INDEX IF NOT EXISTS domain_content_id_idx ON domains (domain_content_id)')
                        elif tableName == 'ips':
                                cursor.execute('CREATE TABLE IF NOT EXISTS ips (ip_content_id INT, ip_text TEXT, ip_ts INT, register TEXT)')
                                cursor.execute('CREATE INDEX IF NOT EXISTS ip_content_id_idx ON ips (ip_content_id)')
                        elif tableName == 'subnets':
                                cursor.execute('CREATE TABLE IF NOT EXISTS subnets (subnet_content_id INT, subnet_text TEXT, subnet_ts INT, register TEXT)')
                                cursor.execute('CREATE INDEX IF NOT EXISTS subnet_content_id_idx ON subnets (subnet_content_id)')
                        elif tableName == 'ipsv6':
                                cursor.execute('CREATE TABLE IF NOT EXISTS ipsv6 (ip_content_id INT, ip_text TEXT, ip_ts INT, register TEXT)')
                        elif tableName == 'subnetsv6':
                                cursor.execute('CREATE TABLE IF NOT EXISTS subnetsv6 (subnet_content_id INT, subnet_text TEXT, subnet_ts INT, register TEXT)')
                        elif tableName == 'content':
                                cursor.execute('CREATE TABLE IF NOT EXISTS content (content_id INT, content_block_type TEXT, content_include_time TEXT, content_urgency_type INT, content_entry_type INT, content_hash TEXT, content_ts INT, content_decision_date TEXT, content_decision_number TEXT, content_decision_org TEXT, register TEXT, PRIMARY KEY (content_id, register))')
                        elif tableName == 'domain_masks':
                                cursor.execute('CREATE TABLE IF NOT EXISTS domain_masks (mask_content_id INT, mask_text TEXT, mask_ts INT)')
                                cursor.execute('CREATE INDEX IF NOT EXISTS mask_content_id_idx ON domain_masks (mask_content_id)')
                        else:
                                pass
        cursor.close()
        db.commit()
else:
        print("Error! cannot create the database connection.")
        logging.error("Error! cannot create the database connection.")

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
