import os
import shutil

if __name__ == '__main__':
    # 清空所有设置
    base_dir = str(os.getcwd()).replace('\\', '/') + '/'
    for x in os.listdir(base_dir + 'Client'):
        now_road = base_dir + 'Client/' + x
        if os.path.isdir(now_road):
            shutil.rmtree(now_road)
        else:
            os.remove(now_road)
    
    for x in os.listdir(base_dir + 'ProxyServer'):
        now_road = base_dir + 'ProxyServer/' + x
        if os.path.isdir(now_road):
            shutil.rmtree(now_road)
        else:
            os.remove(now_road)
    
    for x in os.listdir(base_dir + 'Server'):
        now_road = base_dir + 'Server/' + x
        if os.path.isdir(now_road):
            shutil.rmtree(now_road)
        else:
            os.remove(now_road)