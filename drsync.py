#! /usr/bin/env python

import os, sys, os.path, threading, Queue, time, subprocess
from collections import namedtuple

dest = sys.argv.pop()
src = sys.argv.pop()

cmd = ['rsync'] + sys.argv[1:]

ips = ['172.100.1.15', '172.100.1.11', '172.100.1.16', '172.100.1.19', '172.100.1.28', '172.100.1.22']

pathinfo = namedtuple('PathInfo', ['size', 'name', 'path'])
# We want comparisons to be reversed....
a = pathinfo.__lt__
pathinfo.__lt__, pathinfo.__gt__ = pathinfo.__gt__, pathinfo.__lt__
pathinfo.__le__, pathinfo.__ge__ = pathinfo.__ge__, pathinfo.__le__

dirtrigger = 0xfeeddeadbeeffeed


def genlist(path, q):
    for r, d, f in os.walk(src):
        for dirname in d:
            fullpath = os.path.join(r, dirname).replace(src, '')
            q.put(pathinfo(dirtrigger, dirname, fullpath))
            time.sleep(.2)
        for filename in f:
            fullpath = os.path.join(r, filename)
            size = os.stat(fullpath).st_size
            if size == dirtrigger:
                size -= 1
            q.put(pathinfo(size, filename, fullpath.replace(src, '')))
    q.put(pathinfo(-1, None, None))

def handle(ip, q, cmd, src, dest, index=0):
    basecmd = cmd
    dircmd = list(cmd)
    dircmd.append("--no-recursive")
    dircmd.append("--dirs")
    dircmd.append("--include=*/")
    dircmd.append("--exclude=*")
    while True:
        #time.sleep(index*10)
        item = q.get()
        if not item.name:
            break
        
        if item.size == dirtrigger:
            fullcmd = dircmd + ['%s%s/' % (src, item.path), '%s%s/' % (dest, item.path)]
        else:
            fullcmd = cmd + ['%s%s' % (src, item.path), '%s%s' % (dest, item.path)]
        print ' '.join(fullcmd)
        proc = subprocess.Popen(fullcmd)
        rv = proc.wait()
        if rv:
            print 'Failed', ' '.join(fullcmd)
            break
            #time.sleep(0.02)

        q.task_done()
    

q = Queue.PriorityQueue(24*1024)
print q
gt = threading.Thread(target = genlist, args=(src, q))
gt.start()

ipt = []
for index, ip in enumerate(ips):
    ipt.append(threading.Thread(target = handle, args=(ip, q, cmd, src, dest, index)))
map(lambda x: x.start(), ipt)

print gt
'''
handle('ip', q, cmd, src, dest, 1.5)
while True:
    item = q.get()
    handle('ip', q
    if not item.name:
        for i in range(len(ipt)):
            q.put(item)
        break
'''

gt.join()

for i in range(len(ipt)):
    q.put(pathinfo(-1, None, None))
map(lambda x: x.join(), ipt)

