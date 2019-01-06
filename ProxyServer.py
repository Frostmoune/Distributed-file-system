import rpyc
import os
import random
import time
import argparse
import json
from rpyc.utils.server import ThreadedServer
from collections import Counter

BASE_DIR = str(os.getcwd()).replace('\\', '/')
BASE_PROXYSERVER_DIR = BASE_DIR + '/ProxyServer/'
BASE_PROXYSERVER_LOGDIR = BASE_PROXYSERVER_DIR + 'Log/'

# 错误信息
PROMPTING_MESSAGE = {
    0: 'Info: Command Accepted.',
    1: 'Error: Permission Denied.',
    2: 'Error: Such file does not exist or it has been removed for.',
    3: 'Error: Another file already exists by this name.',
    4: 'Error: Can not access the file now.',
    5: 'Error: Command Invalid.',
    6: 'Error: Something wrong occured in checking.',
    7: 'Error: Such client id already exists.',
    8: 'Error: Such client id does not exist.',
    9: 'Error: Free lock failed.',
    10: 'Error: The diretory does not exist.'
}

N = None
clients = None
file_to_servers = {}
log_file = {}
client_connection = {}

class ProxyServer(rpyc.Service):
    def on_connect(self, conn):
        if not os.path.exists(BASE_PROXYSERVER_LOGDIR):
            os.mkdir(BASE_PROXYSERVER_LOGDIR)
        self.readConfig()
    
    # 判断文件坏块
    def check(self, file_contents):
        tuple_contents = list(map(lambda x:tuple(x), file_contents))
        counter = Counter(tuple_contents).most_common()
        for x in counter:
            if x[0] != None:
                return 0, list(x[0])
        return 6, None
    
    # Master处理download请求
    def downloadFile(self, request):
        file_contents = []
        key = (request['owner_id'], request['file_name'])
        client_id = request['client_id']
        error_id, flag = 0, 0
        if key not in file_to_servers:
            return 2, PROMPTING_MESSAGE[2]
        for i in file_to_servers[key]:
            flag, content = clients[i - 1].root.downloadFile(request)
            if flag != 0:
                error_id = i
                break

            file_contents.append(content)
            log_file[client_id].write("Download %s from Server %d Accepted\n"%(key[1], i))
        
        if flag != 0:
            error_message = PROMPTING_MESSAGE[flag]
            log_file[client_id].write("Server %d: %s\n"%(error_id, error_message))
            return flag, error_message
        
        flag, return_file = self.check(file_contents)
        if flag != 0:
            error_message = PROMPTING_MESSAGE[flag]
            log_file[client_id].write("Server %d: %s\n"%(error_id, error_message))
            return flag, error_message

        return flag, return_file
    
    # Master处理upload请求
    def uploadFile(self, request, new_file_content):
        flag = 0
        key = (request['owner_id'], request['file_name'])
        client_id = request['client_id']
        # free_lock = request.get('free_lock', 0)
        is_create = 'is_create' in request and request['is_create']
        if is_create and client_id != key[0]:
            return 5, PROMPTING_MESSAGE[5]
            
        if key not in file_to_servers:
            if not is_create:
                return 5, PROMPTING_MESSAGE[5]

            servers_len = len(clients)
            half_len = ((servers_len) // 2) | 1
            L = [i + 1 for i in range(servers_len)]
            random.shuffle(L)
            servers = L[:half_len]
            file_to_servers[key] = servers

        error_id = 0
        for i in file_to_servers[key]:
            flag = clients[i - 1].root.uploadFile(request, new_file_content)
            if flag != 0:
                error_id = i
                break
            log_file[client_id].write("Upload %s to Server %d Accepted\n"%(key[1], i))
        
        if flag != 0:
            error_message = PROMPTING_MESSAGE[flag]
            log_file[client_id].write("Server %d: %s\n"%(error_id, error_message))
            return flag, error_message
        
        return flag, None
    
    # Master处理delete请求
    def deleteFile(self, request):
        flag = 0
        key = (request['owner_id'], request['file_name'])
        client_id = request['client_id']

        error_id = 0
        for i in file_to_servers[key]:
            flag = clients[i - 1].root.deleteFile(request)
            if flag != 0:
                error_id = i
                break
            log_file[client_id].write("Delete %s on Server %d Accepted\n"%(key[1], i))
        
        if flag != 0:
            error_message = PROMPTING_MESSAGE[flag]
            log_file[client_id].write("Server %d: %s\n"%(error_id, error_message))
            return flag, error_message

        return flag, None
    
    # Master处理find请求
    def findFile(self, request):
        flag = 0
        client_id = request['client_id']
        file_name = request['file_name']

        all_roads = set([])
        for i, x in enumerate(clients):
            flag, roads = x.root.findFile(request)
            if flag != 0:
                log_file[client_id].write("Can not find %s on Server\n"%(file_name))
            else:
                all_roads.update(roads)
                log_file[client_id].write("Find %s on Server %d Accepted\n"%(file_name, i + 1))

        if len(all_roads) == 0:
            return 2, PROMPTING_MESSAGE[2]
        return 0, list(map(lambda x: str(x[0]) + '/' + x[1], all_roads))
    
    # Master处理list请求
    def listFile(self, request):
        flag = 0
        client_id = request['client_id']
        dir_name = request['owner_id']

        all_files = set([])
        for i, x in enumerate(clients):
            flag, files = x.root.listFile(request)
            if flag != 0:
                log_file[client_id].write("Can not find diretory %s on Server %d\n"%(dir_name, i + 1))
                return flag, PROMPTING_MESSAGE[flag]
            else:
                all_files.update(files)
                log_file[client_id].write("List %s on Server %d Accepted\n"%(dir_name, i + 1))

        if len(all_files) == 0:
            return 0, ['No file.']
        return 0, list(all_files)
    
    # Master处理释放读锁
    def freeReadLock(self, request):
        flag = 0
        key = (request['owner_id'], request['file_name'])
        client_id = request['client_id']

        error_id = 0
        for i in file_to_servers[key]:
            flag = clients[i - 1].root.freeReadLock(request)
            if flag != 0:
                error_id = i
                break
            log_file[client_id].write("Free read lock to %s on Server %d Accepted\n"%(key[1], i))
        
        if flag != 0:
            error_message = PROMPTING_MESSAGE[flag]
            log_file[client_id].write("Server %d: %s\n"%(error_id, error_message))
            return flag, error_message

        return flag, None   
    
    # 总的命令处理功能
    def exposed_requestHandle(self, request, new_file_content = None):
        command = request['command']
        client_id = request['client_id']
        if command != 'list':
            log_file[client_id].write("Client %d: %s %s\n"%(request['client_id'], command, request['file_name']))
        else:
            log_file[client_id].write("Client %d: %s %s\n"%(request['client_id'], command, request['owner_id']))
        
        if command == 'download':
            return self.downloadFile(request)
        elif command == 'upload':
            return self.uploadFile(request, new_file_content)
        elif command == 'delete':
            return self.deleteFile(request)
        elif command == 'find':
            return self.findFile(request)
        elif command == 'freereadlock':
            return self.freeReadLock(request)
        elif command == 'list':
            return self.listFile(request)
        
        return 5, PROMPTING_MESSAGE[5]
    
    # Master与用户建立连接
    def exposed_connectionEstablish(self, request):
        client_id = request['client_id']
        if client_id in client_connection:
            return 7, PROMPTING_MESSAGE[7]
        
        client_connection[client_id] = 1
        log_file[client_id] = open(BASE_PROXYSERVER_LOGDIR + 'Log_Client%d.txt'%client_id, 'a')
        now_time = time.strftime("%a %b %d %H:%M:%S %Y\n", time.localtime())
        log_file[client_id].write(now_time)

        flag = 0
        for x in clients:
            x.root.connectionEstablish(request)
        return flag, None
    
    # Master与用户断开连接
    def exposed_connectionCancel(self, request):
        client_id = request['client_id']
        if client_id not in client_connection:
            return 8, PROMPTING_MESSAGE[8]

        client_connection.pop(client_id)
        log_file[client_id].close()
        log_file.pop(client_id)

        flag = 0
        for x in clients:
            x.root.connectionCancel(request)
        return flag, None
    
    # Master保存配置
    def saveConfig(self):
        json_dict = {}
        for x in file_to_servers:
            json_dict['%d_%s'%(x[0], x[1])] = file_to_servers[x]
        with open(BASE_PROXYSERVER_DIR + 'Config.json', 'w') as f:
            json.dump(json_dict, f)
    
    # Master读取配置
    def readConfig(self):
        global file_to_servers
        if os.path.exists(BASE_PROXYSERVER_DIR + 'Config.json'):
            with open(BASE_PROXYSERVER_DIR + 'Config.json', 'r') as f:
                json_dict = dict(json.load(f))
            for x in json_dict:
                key = x.split('_')
                key = (int(key[0]), key[1])
                file_to_servers[key] = json_dict[x]
    
    # Master断开连接
    def on_disconnect(self, conn):
        for x in clients: 
            x.close()
        for x in log_file: 
            log_file[x].close()
        self.saveConfig()
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--n', type = int, default = 5,
                       help = 'n')
    args = parser.parse_args()

    N = args.n
    clients = [rpyc.connect('localhost', 12341 + i) for i in range(N)]
    server = ThreadedServer(ProxyServer, port = 12340)
    server.start()