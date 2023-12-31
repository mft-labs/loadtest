import json
import time
import pysftp
from datetime import datetime
import os
import sqlite3
import traceback
import paramiko
import threading
from threading import Thread, Lock
import sys
import string
import random
lock = Lock()
import logging
conf = 'loadtest2.json'
if len(sys.argv) > 1:
    conf = sys.argv[1]

dateTimeObj2 = datetime.now()
log_suffix = dateTimeObj2.strftime('%d%m%Y_%H%M%S%f')
logger = logging.getLogger(__name__)
extra = {'conf': conf}
#LOG_FORMAT = '%(asctime)s %(conf)-20s %(lineno)s:: %(message)s'
LOG_FORMAT = "%(asctime)s-%(levelname)s-%(name)s::%(module)s|%(lineno)s:: %(message)s"
logging.basicConfig(filename=f'loadtest_{log_suffix}.log', format=LOG_FORMAT, encoding='utf-8', level=logging.INFO)
logger = logging.LoggerAdapter(logger, extra)




class RandomString(object):
    def __init__(self, n):
        self.n = n
    def random_letters(self):
        return ''.join(random.choices(string.ascii_letters, k=self.n))

    def random_uppercase(self):
        return ''.join(random.choices(string.ascii_uppercase, k=self.n))

    def random_lowercase(self):
        return ''.join(random.choices(string.ascii_lowercase, k=self.n))

    def random_digits(self):
        return ''.join(random.choices(string.digits, k=self.n))

    def random_upper_digits(self):
        return ''.join(random.choices(string.ascii_uppercase+string.digits, k=self.n))

    def random_lower_digits(self):
        return ''.join(random.choices(string.ascii_lowercase+string.digits, k=self.n))

    def random_letters_digits(self):
        return ''.join(random.choices(string.ascii_letters+string.digits, k=self.n))


class DbManager(object):
    def __init__(self, logger, dbname,add_timestamp=True):
        self.logger = logger
        ts = str(int(time.time() * 1000))
        dateTimeObj = datetime.now()
        timestampStr = dateTimeObj.strftime('%d%m%Y_%H%M%S%f')
        if add_timestamp:
            self.con = sqlite3.connect(f"{dbname}_{timestampStr}.db",check_same_thread=False)
            self.initialize()
        else:
            self.con = sqlite3.connect(f"{dbname}",check_same_thread=False)


    def initialize(self):
        cur = self.con.cursor()
        cur.execute("CREATE TABLE file_upload(fileno,host, port, username,filename,timestamp,status,notes)")
        cur.close()
        self.con.commit()

    def connection(self):
        return self.con

    def add_entry(self, con, fileno, host, port, username, filename, status, notes):
        ts = str(int(time.time() * 1000))
        cur = con.cursor()
        dateTimeObj = datetime.now()
        timestampStr = dateTimeObj.strftime('%Y-%m-%d %H:%M:%S.%f')
        cur.execute("""insert into file_upload(fileno,host, port, username, filename,timestamp, status, notes) 
                        values(?,?,?,?,?,?,?,?)""", (fileno,host, port, username, filename, timestampStr, status, notes))
        cur.close()
        con.commit()

    def retrieve_successful_data(self):
        cursor = self.con.cursor()
        cursor.execute('''SELECT fileno,host, port, username, filename,timestamp, status, notes from file_upload where status = 'success' order by status desc  ''')
        result = cursor.fetchall()
        self.logger.info('Successful Transfers')
        self.logger.info('----------------------------------------------------------------------------------------------')
        for row in result:
            self.logger.info(f'{row[0]:5d} {row[1]:20s} {row[2]:6d} {row[3]:15s} {row[4]:25s} {row[5]:20s} {row[6]:10s}')
        cursor.close()
        self.logger.info('----------------------------------------------------------------------------------------------')

    def retrieve_failure_data(self):
        cursor = self.con.cursor()
        cursor.execute('''SELECT fileno,host, port, username, filename,timestamp, status, notes from file_upload where status = 'failed' order by status desc  ''')
        result = cursor.fetchall()
        self.logger.info('Failed Transfers')
        self.logger.info('----------------------------------------------------------------------------------------------')
        for row in result:
            self.logger.info(f'{row[0]:5d} {row[1]:20s} {row[2]:6d} {row[3]:15s} {row[4]:20s} {row[5]:20s} {row[6]:10s}')
        cursor.close()
        self.logger.info('----------------------------------------------------------------------------------------------')

    def retrieve_failure_count(self):
        cursor = self.con.cursor()
        cursor.execute('''SELECT count(*) from file_upload where status = 'failed' order by status desc  ''')
        result = cursor.fetchone()
        self.logger.info('')
        self.logger.info(f'Failed Transfers count {result[0]}')
        cursor.close()

    def retrieve_successful_count(self):
        cursor = self.con.cursor()
        cursor.execute('''SELECT count(*) from file_upload where status = 'success' order by status desc  ''')
        result = cursor.fetchone()
        self.logger.info('')
        self.logger.info(f'Successful Transfers count {result[0]}')
        cursor.close()

    def retrieve_data(self):
        self.retrieve_successful_data()
        self.retrieve_successful_count()
        self.retrieve_failure_data()
        self.retrieve_failure_count()



class Config(object):
    def __init__(self,conf):
        self.conf = conf
        self.load_config()

    def load_config(self):
        self.config = json.loads(open(self.conf,'r').read())

    def get_config(self):
        return self.config

class SftpServerConnection(pysftp.Connection):
    def __init__(self, *args, **kwargs):
        try:
            if kwargs.get('cnopts') is None:
                cnopts = pysftp.CnOpts()
                cnopts.hostkeys = None
                kwargs['cnopts'] = cnopts
        except pysftp.HostKeysException as e:
            self._init_error = True
            raise paramiko.ssh_exception.SSHException(str(e))
        else:
            self._init_error = False

        self._sftp_live = False
        self._transport = None
        self.banner_timeout = 3000
        super().__init__(*args, **kwargs)

    def __del__(self):
        if not self._init_error:
            try:
                self.close()
            except:
                pass

def upload_file_async( logger, lock, dbmgr, fileno, host, port, username, auth_type, creds, priv_key_pass,  source, target, file_format="AS IS"):
    logger.debug(f'Uploading file item {fileno}')
    localpath=None
    remotepath=None
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    try:
        dateTimeObj = datetime.now()
        timestampStr = dateTimeObj.strftime('%d%m%Y_%H%M%S%f')
        localpath=source
        remotepath=target
        arr = remotepath.split('.')
        format_text = ''
        if file_format == "AS IS":
            format_text = ''
        else:
            if file_format == "FILENO_DATE_TIME":
                format_text = '_'+str(fileno)+'_'+timestampStr
            else:
                if file_format == "TIMESTAMP":
                    format_text = "_"+dateTimeObj.strftime('%Y%m%d%H%M%S%f')
                else:
                    if file_format[0:7] == "RANDOM_":
                        length = int(file_format[7:])
                        generator = RandomString(length)
                        format_text = "_"+generator.random_letters_digits()

        if len(arr)>1:
            newfile = '.'.join(arr[0:-1])
            newfile = newfile+format_text+'.'+arr[-1]
        else:
            newfile = remotepath+format_text
        remotepath=newfile
        logger.info('Sending from %s to %s' %(localpath,remotepath))
        if auth_type == 'password':
            with SftpServerConnection(host=host, port=port, username=username, password=creds, cnopts=cnopts) as sftp:
                lock.acquire()
                logger.info(f'Uploading file from {localpath} to {remotepath} into {host} {port} {username}')
                sftp.put(localpath,remotepath, confirm=False)
                lock.release()
                dbmgr.add_entry(dbmgr.connection(),fileno,host, port, username, remotepath,"success","success")
        else:
            priv_key_pass1 = None
            if priv_key_pass != "":
                priv_key_pass1 = priv_key_pass
            with SftpServerConnection(host=host, port=port, username=username, private_key=creds, private_key_pass=priv_key_pass1,cnopts=cnopts) as sftp:
                lock.acquire()
                logger.info(f'Uploading file from {localpath} to {remotepath} into {host} {port} {username}')
                sftp.put(localpath,remotepath, confirm=False)
                lock.release()
                dbmgr.add_entry(dbmgr.connection(),fileno,host, port, username, remotepath,"success","success")
    except:
        logger.warning(f'Exception arisen {traceback.format_exc()}')
        if localpath!=None and remotepath!=None:
            dbmgr.add_entry(dbmgr.connection(), fileno,host, port, username,remotepath,"failed",traceback.format_exc())
        else:
            dbmgr.add_entry(dbmgr.connection(), fileno, host, port, username,source,"failed",traceback.format_exc())

def download_file_async( logger, lock, dbmgr, fileno, host, port, username, auth_type, creds, priv_key_pass,  source, target, file_format="AS IS"):
    print(f'Downloading file item {fileno}')
    localpath=None
    remotepath=None
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    dateTimeObj = datetime.now()
    timestampStr = dateTimeObj.strftime('%d%m%Y_%H%M%S%f')
    localpath=source
    remotepath=target
    logger.info('Retrieving file from %s to %s' %(remotepath,localpath))
    try:
        if auth_type == 'password':
            with SftpServerConnection(host=host, port=port, username=username, password=creds,cnopts=cnopts) as sftp:
                lock.acquire()
                files = sftp.listdir_attr(remotepath)
                logger.info(f'No. of files available for download: {len(files)}')
                for f in files:
                    #sftp.get(remotepath+'/'+f.filename,localpath+'/'+f.filename)
                    file_to_download = remotepath+'/'+f.filename
                    try:
                        if(sftp.exists(file_to_download)):
                            logger.info(f'Downloading file {file_to_download}' )
                            sftp.get(file_to_download,localpath+'/'+f.filename)
                            logger.info(f'Removing file from sftp server {file_to_download}')
                            sftp.remove(file_to_download)
                    except:
                        logger.warning(f'Exception raised while downloading {file_to_download} {traceback.format_exc()}')
                lock.release()
                dbmgr.add_entry(dbmgr.connection(),fileno,host, port, username, remotepath,"success","success")
        else:
            priv_key_pass1 = None
            if priv_key_pass != "":
                priv_key_pass1 = priv_key_pass
            with SftpServerConnection(host=host, port=port, username=username, private_key=creds,private_key_pass=priv_key_pass1,cnopts=cnopts) as sftp:
                lock.acquire()
                files = sftp.listdir_attr(remotepath)
                logger.info(f'No. of files available for download at {remotepath}: {len(files)}')
                for f in files:
                    file_to_download = remotepath+'/'+f.filename
                    try:
                        if(sftp.exists(file_to_download)):
                            logger.info(f'Downloading file {file_to_download}' )
                            sftp.get(file_to_download,localpath+'/'+f.filename)
                            logger.info(f'Removing file from sftp server {file_to_download}')
                            sftp.remove(file_to_download)
                    except:
                        logger.warning(f'Exception raised while downloading {file_to_download} {traceback.format_exc()}')
                lock.release()
                dbmgr.add_entry(dbmgr.connection(),fileno,host, port, username, remotepath,"success","success")
    except:
        logger.warning(f'Exception arisen {traceback.format_exc()}')
        if localpath!=None and remotepath!=None:
            dbmgr.add_entry(dbmgr.connection(), fileno,host, port, username,remotepath,"failed",traceback.format_exc())
        else:
            dbmgr.add_entry(dbmgr.connection(), fileno, host, port, username,source,"failed",traceback.format_exc())


class LoadTest(object):
    def __init__(self, logger,conf, svc, running_threads):
        self.config = Config(conf).get_config()
        self.nofiles = self.config["nofiles"]
        self.testcases = self.config["testcases"].split(",")
        self.logger = logger
        self.dbmgr = DbManager(logger, "loadtest")
        self.file_format  = "AS IS"
        self.count = 0
        self.running_threads = running_threads
        self.svc = svc

    def prepare_test(self):
        completed = False
        dbmgr = self.dbmgr
        con = dbmgr.connection()
        while True:
            for testcase in self.testcases:
                host = self.config[testcase]["host"]
                hostinfo = host.split(":")
                users = self.config[testcase]["users"].split(",")
                active = self.config[testcase]["active"]
                file_format = "AS IS"
                if 'file_format' in self.config[testcase]:
                    file_format = self.config[testcase]["file_format"]
                if active == "false":
                    continue
                #print(users)
                for user in users:
                    userinfo = self.config[testcase][user]
                    auth_type = userinfo["auth_type"]
                    creds = userinfo[auth_type]
                    priv_key_pass=""
                    if 'priv_key_pass' in userinfo:
                        priv_key_pass = userinfo['priv_key_pass']
                    mode = self.config[testcase]["mode"]
                    target = self.config[testcase]["target"]

                    targetfile = ''
                    if mode == 'upload':
                        files = self.config[testcase]["files"]
                        for filepath in files:
                            try:
                                isdir = os.path.isdir(filepath)
                                files_list = []
                                if isdir:
                                    origin = filepath
                                    files_list = os.listdir(filepath)
                                else:
                                    origin = ''
                                    files_list = [filepath]

                                for fileinfo in files_list:
                                    filepath2 = os.path.join(origin, fileinfo)
                                    filename = os.path.basename(filepath2).split('/')[-1]
                                    if target == '/':
                                        targetfile = target+filename
                                    else:
                                        targetfile = target+'/'+filename
                                    self.count=self.count+1
                                    if self.count > self.nofiles:
                                        return
                                    logger.info(f'Creating new thread for {self.count}')
                                    #t = Thread(target=upload_file_async, args=(lock, dbmgr, self.count,hostinfo[0],int(hostinfo[1]),user,userinfo["password"],filepath2,targetfile,))
                                    #self.running_threads.append(t)
                                    self.svc.add_item(lock, dbmgr, self.count,hostinfo[0],int(hostinfo[1]),user,auth_type,creds,priv_key_pass,mode,filepath2,targetfile,file_format)
                            except:
                                logger.warning(f'Exception raised while prepare for upload {traceback.format_exc()}')
                                dbmgr.add_entry(con, self.count,hostinfo[0], int(hostinfo[1]), user,targetfile,"upload failed",traceback.format_exc())
                    else:
                        self.count = self.count+1
                        downloadpath = self.config[testcase]["downloadpath"]
                        try:
                            self.svc.add_item(lock, dbmgr, self.count,hostinfo[0],int(hostinfo[1]),user,auth_type,creds,priv_key_pass,mode,downloadpath,target,"AS IS")
                        except:
                            logger.warning(f'Exception raised while prepare for download {traceback.format_exc()}')
                            dbmgr.add_entry(con, self.count,hostinfo[0], int(hostinfo[1]), user,target,"download failed",traceback.format_exc())
                        finally:
                            return

            #time.sleep(self.config["delay"])

    def run_test(self):
        logger.info('Preparing for run the test ....')
        self.svc.prepare_threads()
        logger.info('Starting process ....')
        self.svc.start_process()
        logger.info('Waiting for process to complete')
        self.svc.wait()

    def generate_report(self):
        logger.info('Generating report ...')
        self.dbmgr.retrieve_data()

class SftpUploadTest(object):
    def __init__(self, logger, thread_count):
        self.threads = []
        self.item_list = []
        self.current_item = 0
        self.thread_count = thread_count
        self.current_item = -1
        self.logger = logger


    def run_process(self,list):
        for item in list:
            self.process(item['lock'],item['dbmgr'],item['fileno'],
                    item['host'],item['port'],item['username'],item['auth_type'],item['creds'],item['priv_key_pass'],item["mode"],
                    item['source'],item['target'],item['file_format'])
            time.sleep(0.1)

    def process(self,lock, dbmgr, fileno, host, port, username, auth_type, creds, priv_key_pass,mode,source, target,file_format="AS IS"):
        logger.info(f'Uploading file using {threading.current_thread().getName()}')
        if mode == "upload":
            logger.info(f'Uploading file to {host} {port} {username} {source} using {auth_type}')
            upload_file_async(self.logger,lock, dbmgr, fileno, host, port, username, auth_type,creds,priv_key_pass, source, target,file_format)
        else:
            logger.info(f'Downloading files from {host} {port} {username} {source} using {auth_type}')
            download_file_async(self.logger, lock, dbmgr, fileno, host, port, username, auth_type, creds, priv_key_pass,source, target,file_format)

    def prepare_threads(self):
        logger.info('Preparing threads for running')
        for item in self.item_list:
            self.threads.append(Thread(target=self.run_process,args=(item,)))

    def start_process(self):
        logger.info('Starting threads')
        for thread in self.threads:
            thread.start()
            time.sleep(1)

    def wait(self):
        logger.info('Waiting for threads to complete')
        for thread in self.threads:
            thread.join()

    def add_item(self, lock, dbmgr, fileno, host, port, username, auth_type, creds, priv_key_pass, mode, source, target, file_format="AS IS"):
        logger.info(f'Adding item into threads for running for {host} {port} {username}')
        new_item = {}
        new_item['lock'] = lock
        new_item['dbmgr'] = dbmgr
        new_item['fileno'] = fileno
        new_item['host'] = host
        new_item['port'] = port
        new_item['username'] = username
        new_item['auth_type'] = auth_type
        new_item['creds'] = creds
        new_item['priv_key_pass'] = priv_key_pass
        new_item["mode"] = mode
        new_item['source'] = source
        new_item['target'] = target
        new_item["file_format"] = file_format
        self.current_item = self.current_item+1
        if self.current_item > self.thread_count:
            self.current_item = 1
        if len(self.item_list)<self.current_item:
            self.item_list.append([])
        if len(self.item_list) == 0 :
            self.item_list.append([])
        self.item_list[self.current_item-1].append(new_item)

    def show_info(self):
        for listitem in self.item_list:
            for item in listitem:
                print(item)

if __name__ == "__main__":
    logger.info('Running application ...')
    if len(sys.argv) > 1:
        logger.info(f'Using custom configuration from {conf}')
    else:
        logger.info(f'Using default configuration from {conf}')

    process_status = []
    running_threads=[]

    config = Config(conf).get_config()
    sftptester = SftpUploadTest(logger, config["thread_count"])
    app = LoadTest(logger, conf,sftptester,running_threads)
    logger.debug('Preparing for upload/download files ...')
    app.prepare_test()
    app.run_test()
    app.generate_report()
#     print(f'Number of threads {len(running_threads)}')
#     print('Starting upload ....')
#     for t in running_threads:
#         print(f'Starting thread {t.name}')
#         t.start()
#         time.sleep(config["thread_delay"])
#     print('Waiting for upload to finish ...')
#     for t in running_threads:
#         t.join()




