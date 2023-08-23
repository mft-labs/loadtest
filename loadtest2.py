import json
import time
import pysftp
from datetime import datetime
import os
import sqlite3
import traceback
import paramiko
from threading import Thread

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
        self.banner_timeout = 200
        super().__init__(*args, **kwargs)

    def __del__(self):
        if not self._init_error:
            try:
                self.close()
            except:
                pass


class LoadTest(object):
    def __init__(self, conf):
        self.config = Config(conf).get_config()
        self.nofiles = self.config["nofiles"]
        self.testcases = self.config["testcases"].split(",")
        self.dbmgr = DbManager("loadtest")
        self.count = 0
        #print(self.hosts)

    def upload_file(self, dbmgr, fileno, host, port, username, passwd, source, target):
        localpath=None
        remotepath=None
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        try:
            print(f'Arrived to upload file with {fileno}')
            with SftpServerConnection(host=host, port=port, username=username, password=passwd,cnopts=cnopts) as sftp:
                dateTimeObj = datetime.now()
                timestampStr = dateTimeObj.strftime('%d%m%Y_%H%M%S%f')
                localpath=source
                remotepath=target 
                arr = remotepath.split('.')
                if len(arr)>1:
                    newfile = '.'.join(arr[0:-1])
                    newfile = newfile+'_'+timestampStr+'.'+arr[-1]
                else:
                    newfile = remotepath+'_'+timestampStr
                remotepath=newfile
                print('Sending from %s to %s' %(localpath,remotepath))
                sftp.put(localpath,remotepath,confirm=True)
                dbmgr.add_entry(dbmgr.connection(),fileno,host, port, username,remotepath,"success","success")
        except:
            if localpath!=None and remotepath!=None:
                dbmgr.add_entry(dbmgr.connection(), fileno,host, port, username,remotepath,"failed",traceback.format_exc())
            else:
                dbmgr.add_entry(dbmgr.connection(), fileno, host, port, username,source,"failed",traceback.format_exc())

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
                    #userinfo = self.config[hostinfo[0]][user]
                    #files = self.config[hostinfo[0]]["files"]
                    #target = self.config[hostinfo[0]]["target"]
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
                                self.upload_file(dbmgr, self.count,hostinfo[0],int(hostinfo[1]),user,userinfo["password"],filepath2,targetfile)
                        except:
                            dbmgr.add_entry(con, self.count,hostinfo[0], int(hostinfo[1]), user,targetfile,"upload failed",traceback.format_exc())
            time.sleep(self.config["delay"])

if __name__ == "__main__":
    app = LoadTest("loadtest2.json")
    app.run_test()

