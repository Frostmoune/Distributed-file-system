import rpyc
import os
import random
import time
import argparse
import json
from rpyc.utils.server import ThreadedServer
from collections import Counter

BASE_DIR = str(os.getcwd()).replace('\\', '/')
BASE_SERVER_DIR = BASE_DIR + '/Server/'

PROMPTING_MESSAGE = {
    0: 'Info: Command Accepted.',
    1: 'Error: Permission Denied.',
    2: 'Error: Such file does not exist or it has been removed for.',
    3: 'Error: Another file already exists by this name.',
    4: 'Error: Can not access the file now.',
    9: 'Error: Free lock failed.',
    10: 'Error: The diretory does not exist.'
}

ID_ = 1
# 0-full permission 1-writable 2-readonly 3-private
file_permission = {}
# 0-unlock 1-readlock 2-writelock
file_lock = {}

class Server(rpyc.Service):
    def on_connect(self, conn):
        self.__id = ID_
        self.__base_file_road = BASE_SERVER_DIR + str(self.__id)
        if not os.path.exists(self.__base_file_road):
            os.mkdir(self.__base_file_road)
        self.readConfig()

    # 判断锁冲突
    def isLockConflict(self, key, now_lock):
        file_lock.setdefault(key, [])
        locks = file_lock[key]
        if len(locks) == 0:
            return False
        for lock in locks:
            if lock[1] == 2 or (lock[1] == 1 and now_lock[1] == 2):
                return True
            if now_lock[0] == lock[0]:
                return True
        return False
    
    # 加锁
    def addLock(self, key, now_lock):
        file_lock[key].append(now_lock)
    
    # 释放锁
    def freeLock(self, key, now_lock):
        file_lock.setdefault(key, [])
        if now_lock in file_lock[key]:
            file_lock[key].remove(now_lock)
            return 0
        return 9

    # 下载文件
    def exposed_downloadFile(self, request):
        now_file = self.__base_file_road + '/' + str(request['owner_id']) + '/' + request['file_name']
        key = (request['owner_id'], request['file_name'])
        client_id = request['client_id']
        is_write = 'is_write' in request and request['is_write']
        is_owner = client_id == key[0]
        now_lock = (client_id, 1)

        if not os.path.exists(now_file) or (not is_owner and file_permission.get(key, 0) == 3):
            return 2, None

        if is_write: 
            if (not is_owner) and file_permission.get(key, 0) >= 2:
                return 1, None
            now_lock = (client_id, 2)
        if self.isLockConflict(key, now_lock):
            return 4, None
        
        self.addLock(key, now_lock)
        p = random.random() < 0.025
        bad_char = '*'
        content = []
        with open(now_file, 'r') as f:
            for line in f.readlines():
                now_line = str(line)
                if p:
                    now_line = list(now_line)
                    now_line[random.randint(0, len(now_line) - 1)] = bad_char
                    now_line = ''.join(now_line)
                content.append(now_line)
        if p:
            with open(now_file, 'w') as f:
                for line in content:
                    f.writelines(line)

        return 0, content
    
    # 释放读锁
    def exposed_freeReadLock(self, request):
        key = (request['owner_id'], request['file_name'])
        client_id = request['client_id']
        now_lock = (client_id, 1)
        return self.freeLock(key, now_lock)
    
    # 上传文件
    def exposed_uploadFile(self, request, new_file_content):
        now_file = self.__base_file_road + '/' + str(request['owner_id']) + '/' + request['file_name']
        key = (request['owner_id'], request['file_name'])
        client_id = request['client_id']
        free_lock = 'free_lock' in request and request['free_lock']
        is_create = 'is_create' in request and request['is_create']
        is_owner = client_id == key[0]
        now_lock = (client_id, 2)
        file_lock.setdefault(key, [])

        if not is_create:
            if not is_owner: 
                if file_permission.get(key, 0) == 2:
                    return 1
                elif file_permission.get(key, 0) == 3:
                    return 2
            if not os.path.exists(now_file):
                return 2
            if self.isLockConflict(key, now_lock) and free_lock == 0:
                return 4
        elif os.path.exists(now_file):
            return 3
        
        if free_lock == 0:
            self.addLock(key, now_lock)
        with open(now_file, 'w') as f:
            for x in new_file_content:
                f.writelines(x)
        if is_create:
            file_permission[key] = int(request['mode'])
            # print("Done ...")
        self.freeLock(key, now_lock)
        return 0
    
    # 查找文件
    def exposed_findFile(self, request):
        client_id = request['client_id']
        file_name = request['file_name']

        flag = 0
        res = []
        for now_file in file_permission:
            if now_file[1] == file_name:
                if client_id != now_file[0] and file_permission[now_file] == 3:
                    continue
                res.append(now_file)
                flag = 1

        if flag == 0:
            return 2, None
        return 0, res
    
    # 删除文件
    def exposed_deleteFile(self, request):
        now_file = self.__base_file_road + '/' + str(request['owner_id']) + '/' + request['file_name']
        key = (request['owner_id'], request['file_name'])
        client_id = request['client_id']
        is_owner = client_id == key[0]
        now_lock = (client_id, 2)

        if not os.path.exists(now_file) or (not is_owner and file_permission.get(key, 0) == 3):
            return 2
        if not is_owner and file_permission.get(key, 0) != 0:
            return 1
        if self.isLockConflict(key, now_lock):
            return 4
        
        self.addLock(key, now_lock)
        os.remove(now_file)
        self.freeLock(key, now_lock)
        file_lock.pop(key)
        file_permission.pop(key)
        return 0
    
    # 列出文件信息
    def exposed_listFile(self, request):
        owner_id = request['owner_id']
        client_id = request['client_id']
        is_owner = client_id == owner_id
        
        files = []
        if not os.path.exists(self.__base_file_road + '/' + str(owner_id)):
            return 10, PROMPTING_MESSAGE[10]

        for x in os.listdir(self.__base_file_road + '/' + str(owner_id)):
            if not is_owner and file_permission[(owner_id, x)] == 3:
                continue
            files.append(x)
        return 0, files
    
    # 建立连接
    def exposed_connectionEstablish(self, request):
        if not os.path.exists(self.__base_file_road + '/' + str(request['client_id'])):
            os.mkdir(self.__base_file_road + '/' + str(request['client_id']))
    
    # 存储配置
    def saveConfig(self):
        json_dict = {}
        for x in file_permission:
            json_dict['%d_%s'%(x[0], x[1])] = file_permission[x]
        with open(self.__base_file_road + '/Config.json', 'w') as f:
            json.dump(json_dict, f)
    
    def readConfig(self):
        global file_permission
        if os.path.exists(self.__base_file_road + '/Config.json'):
            with open(self.__base_file_road + '/Config.json', 'r') as f:
                json_dict = dict(json.load(f))
            for x in json_dict:
                key = x.split('_')
                key = (int(key[0]), key[1])
                file_permission[key] = json_dict[x]
    
    def exposed_connectionCancel(self, request):
        client_id = request['client_id']
        for key in file_lock:
            remove_lock = None
            for y in file_lock[key]:
                if y[0] == client_id:
                    remove_lock = y
                    break
            if remove_lock != None:
                file_lock[key].remove(remove_lock)
    
    def on_disconnect(self, conn):
        self.saveConfig()

def run(server):
    try:
        server.start()
    except KeyboardInterrupt:
        server.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type = int, default = 12341,
                       help = 'port')
    args = parser.parse_args()

    port_ = args.port
    ID_ = port_ - 12340
    server = ThreadedServer(Server, hostname = 'localhost', port = port_)
    run(server)