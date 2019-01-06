import rpyc
import os
import random
import time
import argparse
import copy
from rpyc.utils.server import ThreadedServer
from collections import Counter

BASE_DIR = str(os.getcwd()).replace('\\', '/')
BASE_CLIENT_DIR = BASE_DIR + '/Client/'

# 0-full 1-writable 2-readonly 3-private
FILE_MODE = {
    'full': 0,
    'writable': 1,
    'readonly': 2,
    'private': 3
}

class Client(object):
    def __init__(self, id_):
        self.id = id_
        self.my_dir = BASE_CLIENT_DIR + str(self.id)
        self.cache_dir = self.my_dir + '/' + 'Cache'
        
        self.log_file = None 
        self.conn = None
    
    # 在cache搜索文件
    def searchInCache(self, file_name):
        if os.path.exists(self.cache_dir + '/' + file_name):
            content = []
            with open(self.cache_dir + '/' + file_name) as f:
                for line in f.readlines():
                    content.append(line)
            return content
        return None
    
    # 将文件存储到cache
    def storeInCache(self, file_name, file_content):
        with open(self.cache_dir + '/' + file_name, 'w') as f:
            for x in file_content:
                f.writelines(x)
    
    # 将用户的命令转换成请求格式
    def commandToRequest(self, command):
        list_command = command.split(' ')
        request = {
            'client_id': self.id
        }
        request['command'] = list_command[0]
        request['owner_id'] = int(list_command[1])
        if list_command[0] != 'list':
            request['file_name'] = list_command[2]

        if list_command[0] == 'upload':
            if len(list_command) >= 4:
                request['free_lock'] = list_command[3] == 'free_lock'
                request['is_create'] = list_command[3] == 'is_create'
                if request['is_create']:
                    request.setdefault('mode', 0)
                    if len(list_command) >= 5:
                        request['mode'] = FILE_MODE[list_command[4]]

        elif list_command[0] == 'download':
            if len(list_command) >= 4:
                request['is_write'] = list_command[3] == 'is_write'
        
        return request
    
    # 按行打印文件
    def printFileByRow(self, content):
        i = 1
        for line in content:
            print(i, end = "   ")
            print(line, end = '')
            i += 1

    # i (m) (n): add some text at (mth row) (nth col), then input the text
    # d (m) (n): remove the text at (mth row) (nth col)
    # r (m) (n): replace new text to the text at (mth row) (nth col), then input the new text
    # default: m equals to the number of rows, n equals to None
    # q: exit
    def writeFile(self, file_name, content):
        now_content = content
        while True:
            self.printFileByRow(now_content)
            line_input = str(input()).split(' ')

            if line_input[0] == ':wq':
                break

            row = len(now_content)
            col = None
            now_len = len(line_input)
            if now_len > 1:
                row = int(line_input[1])
            if now_len > 2:
                col = int(line_input[2]) - 1
            if row > len(now_content):
                input("Command Invalid. Press Enter to continue\n")
                continue
            if col != None and col > len(now_content[row - 1]):
                input("Command Invalid. Press Enter to continue\n")
                continue

            if line_input[0] == 'i':
                text = input()
                if col == None:
                    now_content.insert(row, text + '\n')
                else:
                    new_line = now_content[row - 1][:col] + text + now_content[row - 1][col:]
                    now_content[row - 1] = new_line
            
            elif line_input[0] == 'd':
                if col == None:
                    now_content.pop(row - 1)
                else:
                    new_line = now_content[row - 1][:col] + now_content[row - 1][col + 1:]
                    now_content[row - 1] = new_line
            
            elif line_input[0] == 'r':
                text = input()
                if col == None:
                    now_content[row - 1] = text + '\n'
                else:
                    new_line = now_content[row - 1][:col] + text + now_content[row - 1][col + 1:]
                    now_content[row - 1] = new_line
        
        self.storeInCache(file_name, now_content)
        return now_content
    
    # 写入内容到新文件
    def writeNewFile(self, file_name):
        content = []
        if os.path.exists(self.cache_dir + '/' + file_name):
            return None
        with open(self.cache_dir + '/' + file_name, 'w') as f:
            while True:
                text = input()
                if text == ':wq':
                    break
                f.writelines(text + '\n')
                content.append(text + '\n')
        return content

    # 创建新文件
    def create(self, file_name, mode = 'writable'):
        client_file = "%d_%s"%(self.id, file_name)
        content = self.writeNewFile(client_file)
        if content == None:
            message = 'Error: Another file already exists by this name.'
            print(message)
            self.log_file.write(message + '\n')
            return

        request = self.commandToRequest('upload %d %s is_create %s'%(self.id, file_name, mode))

        flag, message = self.conn.root.requestHandle(request, new_file_content = content)
        if flag == 0:
            self.log_file.write('Create %s accepted\n'%file_name)
        else:
            print(message)
            self.log_file.write(message + '\n')
    
    # 写文件
    def write(self, owner_id, file_name, is_remote = 0):
        client_file = '%d_%s'%(owner_id, file_name)
        content = self.searchInCache(client_file)
        free_lock = 0
        if content == None or is_remote:
            request = self.commandToRequest('download %d %s is_write'%(owner_id, file_name))
            flag, content = self.conn.root.requestHandle(request)
            free_lock = 1

            if flag != 0:
                self.log_file.write(content + '\n')
                print(content)
                return
            
            self.storeInCache(client_file, content)
            content = self.searchInCache(client_file)

        new_content = self.writeFile(client_file, content)
        command = 'upload %d %s'%(owner_id, file_name)
        if free_lock:
            command += ' free_lock'
        request = self.commandToRequest(command)
        flag, message = self.conn.root.requestHandle(request, new_content)
        if flag != 0:
            self.log_file.write(message + '\n')
            print(message)
            return
        self.log_file.write('Write %d/%s accepted\n'%(owner_id, file_name))
    
    # 读文件
    def read(self, owner_id, file_name, is_remote = 0):
        client_file = '%d_%s'%(owner_id, file_name)
        content = self.searchInCache(client_file)
        if content == None or is_remote:
            request = self.commandToRequest('download %d %s'%(owner_id, file_name))
            flag, content = self.conn.root.requestHandle(request)
            if flag != 0:
                self.log_file.write(content + '\n')
                print(content)
                return
            
            self.printFileByRow(content)
            input("Press Enter to Continue.")

            request = self.commandToRequest('freereadlock %d %s'%(owner_id, file_name))
            flag, message = self.conn.root.requestHandle(request)
            if flag != 0:
                self.log_file.write(message + '\n')
                print(message)
                return
            self.storeInCache(client_file, content)

        else:
            self.printFileByRow(content)
            input("Press Enter to Continue.")
        self.log_file.write('Read %d/%s accepted\n'%(owner_id, file_name))
    
    # 删除文件
    def delete(self, owner_id, file_name):
        client_file = '%d_%s'%(owner_id, file_name)
        content = self.searchInCache(client_file)

        if content != None:
            os.remove(self.cache_dir + '/' + client_file)
        request = self.commandToRequest('delete %d %s'%(owner_id, file_name))
        flag, content = self.conn.root.requestHandle(request)
        if flag != 0:
            self.log_file.write(content + '\n')
            print(content)
            return
        self.log_file.write('Delete %d/%s accepted\n'%(owner_id, file_name))
    
    # 查找文件（在cache和远程服务器查找）
    def find(self, file_name):
        for x in os.listdir(self.cache_dir):
            temp = x.split('_')
            owner_id, now_file_name = int(temp[0]), temp[1]
            if now_file_name == file_name:
                print('%d/%s in cache'%(owner_id, file_name))
                self.log_file.write('Find %s accepted\n'%(file_name))
        
        request = self.commandToRequest('find %d %s'%(self.id, file_name))
        flag, content = self.conn.root.requestHandle(request)

        if flag != 0:
            self.log_file.write(content + '\n')
            print(content)
            return

        for x in content:
            print(x)
        self.log_file.write('Find %s accepted\n'%(file_name))
    
    # 列出文件
    def list_(self, owner_id, is_cache = 0):
        if is_cache:
            print("Cache:")
            for x in os.listdir(self.cache_dir):
                print('/'.join(x.split('_')))
            self.log_file.write('List cache accepted\n')

        if owner_id != None:
            request = self.commandToRequest('list %d'%(owner_id))
            flag, content = self.conn.root.requestHandle(request)

            if flag != 0:
                self.log_file.write(content + '\n')
                print(content)
                return

            if owner_id == self.id:
                print("Remote:")
            for x in content:
                print(x)
            self.log_file.write('List %d accepted\n'%(owner_id))

    # 连接
    def connect(self):
        request = {
            'client_id': self.id
        }
        self.conn = rpyc.connect("localhost", 12340)
        flag, content = self.conn.root.connectionEstablish(request)
        if flag != 0:
            print(content)
            self.conn.close()
            return False

        if not os.path.exists(self.my_dir):
            os.mkdir(self.my_dir)
        if not os.path.exists(self.cache_dir):
            os.mkdir(self.cache_dir)

        self.log_file = open(self.my_dir + '/' + 'Log.txt', 'a')
        now_time = time.strftime("%a %b %d %H:%M:%S %Y\n", time.localtime())
        self.log_file.write(now_time)
        self.log_file.write('Connect accepted\n')
        return True
    
    # 断开连接
    def disconnect(self):
        request = {
            'client_id': self.id
        }
        flag, content = self.conn.root.connectionCancel(request)
        if flag != 0:
            self.log_file.write(content + '\n')
            print(content)
            return False
        self.log_file.write('Disconnect accepted\n')
        self.log_file.close()
        self.conn.close()
    
    # 随机删除cache中的文件
    def randomCacheRemove(self):
        for x in os.listdir(self.cache_dir):
            file_road = self.cache_dir + '/' + x
            if random.random() < 0.2:
                os.remove(file_road)
    
    # 用户运行
    def run(self):
        flag = self.connect()
        if not flag:
            return
        try:
            while True:
                user_command = input('[%d] >>'%self.id)
                if user_command == '':
                    continue

                if user_command == 'exit':
                    self.disconnect()
                    break

                user_command = user_command.split(' ')
                if user_command[0] == 'create':
                    if len(user_command) > 2:
                        self.create(user_command[1], user_command[2])
                    else:
                        self.create(user_command[1])

                elif user_command[0] == 'find':
                    self.find(user_command[1])

                elif user_command[0] == 'list':
                    if len(user_command) > 1:
                        is_cache = (user_command[1] == 'cache')
                        if is_cache:
                            owner_id = None
                        else:
                            if str.isdigit(user_command[1]):
                                owner_id = int(user_command[1])
                            else:
                                input("Command Invalid. Press Enter to continue\n")
                                continue
                    else:
                        owner_id = self.id
                        is_cache = True
                    self.list_(owner_id, is_cache)

                elif user_command[0] == 'write' or user_command[0] == 'read' or user_command[0] == 'delete':
                    temp = user_command[1].split('/')
                    if len(temp) > 1:
                        owner_id = int(temp[0])
                        file_name = temp[1]
                    else:
                        owner_id = self.id
                        file_name = temp[0]
                    if user_command[0] == 'write':
                        is_remote = False
                        if len(user_command) > 2:
                            is_remote = (user_command[2] == 'remote')
                        self.write(owner_id, file_name, is_remote)

                    if user_command[0] == 'read':
                        is_remote = False
                        if len(user_command) > 2:
                            is_remote = (user_command[2] == 'remote')
                        self.read(owner_id, file_name, is_remote)

                    if user_command[0] == 'delete':
                        self.delete(owner_id, file_name)

                else:
                    input("Command Invalid. Press Enter to continue\n")
                self.randomCacheRemove()
        except KeyboardInterrupt:
            self.disconnect()

if __name__ == '__main__':
    id_ = int(input('>>Please Input your id: '))
    now_client = Client(id_)
    now_client.run()