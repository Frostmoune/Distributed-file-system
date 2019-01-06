import os
import argparse
from multiprocessing import Process

def run(i):
    command = 'python Server.py --port %d'%(12341 + i)
    os.system(command)

# 一次性运行多个Server
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--n', type = int, default = 5,
                       help = 'n')
    args = parser.parse_args()

    N = args.n
    process = [Process(target = run, args = (i, )) for i in range(N)]
    for i in range(N):
        process[i].start()
    
    for i in range(N):
        process[i].join()