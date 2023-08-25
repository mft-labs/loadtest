import json
import time
import pysftp
from datetime import datetime
import os
import sqlite3
import traceback
import paramiko
from threading import Thread, Lock
import sys
lock = Lock()

class DbManager(object):
    def __init__(self, dbname):
        ts = str(int(time.time() * 1000))
        dateTimeObj = datetime.now()
        timestampStr = dateTimeObj.strftime('%d%m%Y_%H%M%S%f')
        self.con = sqlite3.connect(f"{dbname}_{timestampStr}.db",check_same_thread=False)
        self.initialize()

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


def upload_file_async( lock, dbmgr, fileno, host, port, username, passwd, source, target):
        print(f'Uploading file item {fileno}')
        localpath=None
        remotepath=None
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None

        try:
            with SftpServerConnection(host=host, port=port, username=username, password=passwd,cnopts=cnopts) as sftp:
                dateTimeObj = datetime.now()
                timestampStr = dateTimeObj.strftime('%d%m%Y_%H%M%S%f')
                localpath=source
                remotepath=target
                arr = remotepath.split('.')
                if len(arr)>1:
                    newfile = '.'.join(arr[0:-1])
                    newfile = newfile+'_'+str(fileno)+'_'+timestampStr+'.'+arr[-1]
                else:
                    newfile = remotepath+'_'+str(fileno)+'_'+timestampStr
                remotepath=newfile
                print('Sending from %s to %s' %(localpath,remotepath))
                lock.acquire()
                sftp.put(localpath,remotepath, confirm=False)
                lock.release()
                dbmgr.add_entry(dbmgr.connection(),fileno,host, port, username, remotepath,"success","success")
        except:
            print(f'Exception arised {traceback.format_exc()}')
            if localpath!=None and remotepath!=None:
                dbmgr.add_entry(dbmgr.connection(), fileno,host, port, username,remotepath,"failed",traceback.format_exc())
            else:
                dbmgr.add_entry(dbmgr.connection(), fileno, host, port, username,source,"failed",traceback.format_exc())

class LoadTest(object):
    def __init__(self, conf, running_threads):
        self.config = Config(conf).get_config()
        self.nofiles = self.config["nofiles"]
        self.testcases = self.config["testcases"].split(",")
        self.dbmgr = DbManager("loadtest")
        self.count = 0
        self.running_threads = running_threads

    def run_test(self):
        completed = False
        dbmgr = self.dbmgr
        con = dbmgr.connection()
        while True:
            for testcase in self.testcases:
                host = self.config[testcase]["host"]
                hostinfo = host.split(":")
                users = self.config[testcase]["users"].split(",")
                active = self.config[testcase]["active"]
                if active == "false":
                    continue
                print(users)
                for user in users:
                    userinfo = self.config[testcase][user]
                    files = self.config[testcase]["files"]
                    target = self.config[testcase]["target"]
                    targetfile = ''
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
                                print(f'Creating new thread for {self.count}')
                                t = Thread(target=upload_file_async, args=(lock, dbmgr, self.count,hostinfo[0],int(hostinfo[1]),user,userinfo["password"],filepath2,targetfile,))
                                self.running_threads.append(t)
                        except:
                            print(f'Exception raised while prepare for upload {traceback.format_exc()}')
                            dbmgr.add_entry(con, self.count,hostinfo[0], int(hostinfo[1]), user,targetfile,"upload failed",traceback.format_exc())
            time.sleep(self.config["delay"])

if __name__ == "__main__":
    conf = 'loadtest2.json'
    if len(sys.argv) > 1:
        conf = sys.argv[1]
        print(f'Using custom configuration from {conf}')
    else:
        print(f'Using default configuration from {conf}')

    process_status = []
    running_threads=[]
    config = Config(conf).get_config()
    app = LoadTest(conf,running_threads)
    print('Preparing for upload ...')
    app.run_test()
    print(f'Number of threads {len(running_threads)}')
    print('Starting upload ....')
    for t in running_threads:
        print(f'Starting thread {t.name}')
        t.start()
        time.sleep(config["thread_delay"])
    print('Waiting for upload to finish ...')
    for t in running_threads:
        t.join()




