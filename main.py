"""
@@Author: Yaser Meshir
Instalar las librerias:
pip install pymongo
configurar los datos de conexion a mongo en el archivo config.json
"""

from pymongo import MongoClient
import smtplib
import email.message
import datetime
import logging, sys


def _connect_mongo(host,port,username,password,db,authdb):
    if username and password:
        mongo_uri = 'mongodb://%s:%s@%s:%s/%s?authSource=%s' % (username, password, host, port, db,authdb)
        conn = MongoClient(mongo_uri)
    else:
        conn = MongoClient(host,port)
    return conn[db]

def read_mongo(db, query, host, port, username, password, authdb):
    # Connect to MongoDB
    db = _connect_mongo(host=host, port=port, username=username, password=password, db=db, authdb=authdb)
    # Make a query to the specific DB and Collection
    cursor = db.command(query)
    #cursor.close()
    return cursor

def QueryBlocketTimeSec():
    logging.basicConfig(filename="slowQueryKill.log",format='%(asctime)s %(message)s',filemode='w')
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    with open('config.json', 'r') as dict_file:
        dict_text = dict_file.read()
        dict_from_file = eval(dict_text)
        query = {'currentOp': True, 'secs_running': {'$gte': dict_from_file.get('timeout_sec')},'op' : { '$in' : [ 'find', 'aggregate', 'findAndModify', 'command', 'getmore', 'count', 'distinct']}}
        try:
            result = read_mongo('admin', query, dict_from_file.get('host'),dict_from_file.get('port'), dict_from_file.get('user'),dict_from_file.get('password'), dict_from_file.get('db_login'))
            print(result['inprog'])
            for trans in result['inprog']:
                hosts_response = trans['host']
                colecction = trans['ns']
                hosts_request = trans['clientMetadata']['mongos']['host']
                ip_request = trans['clientMetadata']['mongos']['client']
                user = trans['effectiveUsers']
                opid = trans['opid']
                op = trans['op']
                command = trans['command']
                sendMail(opid, command, op, hosts_response, colecction, hosts_request, ip_request, user, dict_from_file.get('msg_subject_kill'))
                logger.info("Procced with Kill to Operation")
                try:
                    #killOp(opid)
                    print("Operation Killed")
                except Exception:
                    logger.error("Error to kill query")
                    continue
        except Exception:
            logger.error("Error in query, not found trans['clientMetadata']['mongos']['host']")

def QueryInsertBlocketTimeSec():
    logging.basicConfig(filename="slowQuery.log",format='%(asctime)s %(message)s',filemode='w')
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    with open('config.json', 'r') as dict_file:
        dict_text = dict_file.read()
        dict_from_file = eval(dict_text)
        query = {'currentOp': True, 'secs_running': {'$gte': dict_from_file.get('timeout_sec')},'op' : { '$in' : ['update', 'insert', 'delete']}}
        try:
            result = read_mongo('admin', query, dict_from_file.get('host'),dict_from_file.get('port'), dict_from_file.get('user'),dict_from_file.get('password'), dict_from_file.get('db_login'))
            print(result['inprog'])
            for trans in result['inprog']:
                hosts_response = trans['host']
                colecction = trans['ns']
                hosts_request = trans['clientMetadata']['mongos']['host']
                ip_request = trans['clientMetadata']['mongos']['client']
                user = trans['effectiveUsers']
                opid = trans['opid']
                op = trans['op']
                command = trans['command']
                sendMail(opid, command, op, hosts_response, colecction, hosts_request, ip_request, user, dict_from_file.get('msg_subject_slow'))
                logger.info("Procced with Kill to Operation")
        except Exception:
            logger.error("Error in query, not found trans['clientMetadata']['mongos']['host']")

def killOp(opid):
    with open('config.json', 'r') as dict_file:
        dict_text = dict_file.read()
        dict_from_file = eval(dict_text)
        query = { "killOp": 1, "op": opid }
        result = read_mongo('admin', query, dict_from_file.get('host'),dict_from_file.get('port'), dict_from_file.get('user'),dict_from_file.get('password'), dict_from_file.get('db_login'))
        print(result)

#def sendMail(opid, command, op, hosts_response, colecction, hosts_request, ip_request, user, subject):
def sendMail():
    logging.basicConfig(filename="slowQueryKill.log",format='%(asctime)s %(message)s',filemode='w')
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    with open('config.json', 'r') as dict_file:
        #Comment this lines when using the function with parameters
        opid = "damon:123456"
        command = "Find"
        op = "Correo de Prueba"
        colecction = "Prueba"
        hosts_response = "Prueba"
        hosts_request = "Prueba"
        ip_request = "Prueba"
        user = "Prueba"

        # Read a config.json to parameters and read template html to send mail
        dict_text = dict_file.read()
        dict_from_file = eval(dict_text)
        now = datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        email_template = open('templateKillOperations.html','r').read()
        email_content = email_template % (opid, now, op, colecction, hosts_response, hosts_request, ip_request, user, command)

        # setup the parameters of the message
        password = ""#dict_from_file.get('msg_password')
        msg = email.message.Message()
        msg['From'] = dict_from_file.get('msg_from')
        msg['To'] = dict_from_file.get('msg_to')
        msg['Subject'] = dict_from_file.get('msg_subject')
        msg.add_header('Content-Type', 'text/html')
        msg.set_payload(email_content)

        try:
            # Create secure connection with server and send email
            smtpObj = smtplib.SMTP(dict_from_file.get('smtp'), dict_from_file.get('port_smtp'))
            smtpObj.sendmail(msg['From'], msg['To'], msg.as_string())
            logger.info("Successfully sent email")
        except smtplib.SMTPException as e:
            logger.error(e, exc_info=True)
            logger.error("Error: unable to send email")

if __name__ == '__main__':
    # globals()[sys.argv[1]]()

    #QueryBlocketTimeSec()
    #QueryInsertBlocketTimeSec()
    sendMail()
