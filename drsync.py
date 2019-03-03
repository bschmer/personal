#! /usr/bin/env python

'''
Multi-threaded rsync for use if there are multiple paths to the data
'''

import os
import os.path
import sys
import threading
import Queue
import time
import subprocess
from collections import namedtuple

PathInfo = namedtuple('PathInfo', ['size', 'name', 'path'])
# We want comparisons to be reversed....

PathInfo.__lt__, PathInfo.__gt__ = PathInfo.__gt__, PathInfo.__lt__
PathInfo.__le__, PathInfo.__ge__ = PathInfo.__ge__, PathInfo.__le__

DIRTRIGGER = 0xfeeddeadbeeffeed

def genlist(path, workqueue):
    '''
    Generate list of items that need to be synced
    '''
    for root, dirs, files in os.walk(path):
        for dirname in dirs:
            fullpath = os.path.join(root, dirname).replace(path, '')
            workqueue.put(PathInfo(DIRTRIGGER, dirname, fullpath))
            time.sleep(.2)
        for filename in files:
            fullpath = os.path.join(root, filename)
            size = os.stat(fullpath).st_size
            if size == DIRTRIGGER:
                size -= 1
            workqueue.put(PathInfo(size, filename, fullpath.replace(path, '')))
    workqueue.put(PathInfo(-1, None, None))

def handle(ip_addr, workqueue, cmd, src, dest):
    '''
    Handle the work for a particular IP address
    '''
    dircmd = list(cmd)
    dircmd.append("--no-recursive")
    dircmd.append("--dirs")
    dircmd.append("--include=*/")
    dircmd.append("--exclude=*")
    while True:
        item = workqueue.get()
        if not item.name:
            break

        if item.size == DIRTRIGGER:
            fullcmd = dircmd + ['%s%s/' % (src, item.path), '%s%s/' % (dest, item.path)]
        else:
            fullcmd = cmd + ['%s%s' % (src, item.path), '%s%s' % (dest, item.path)]
        print ' '.join(fullcmd)
        proc = subprocess.Popen(fullcmd)
        if proc.wait():
            print 'Failed', ' '.join(fullcmd)
            break

        workqueue.task_done()

def main():
    '''
    Main program.
    '''
    dest = sys.argv.pop()
    src = sys.argv.pop()

    cmd = ['rsync'] + sys.argv[1:]

    ips = ['172.100.1.15', '172.100.1.11', '172.100.1.16', '172.100.1.19', '172.100.1.28', '172.100.1.22']

    workqueue = Queue.PriorityQueue(24*1024)
    print workqueue
    generator_thread = threading.Thread(target=genlist, args=(src, workqueue))
    generator_thread.start()

    ip_threads = []
    for ip_addr in ips:
        ip_threads.append(threading.Thread(target=handle, args=(ip_addr, workqueue, cmd, src, dest)))

    for curthread in ip_threads:
        curthread.start()

    print generator_thread

    generator_thread.join()

    for _ in range(len(ip_threads)):
        workqueue.put(PathInfo(-1, None, None))
    for curthread in ip_threads:
        curthread.join()

if __name__ == '__main__':
    main()
