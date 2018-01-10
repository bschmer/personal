#! /usr/bin/env python

'''
TODO:
   - Write docs
   - Monitor periodic command output (as opposed to continually running/constant output commands)
   - Add tests
   - Integer differences: Total since last sample, average over change time, etc
   - Interactive control
   - Version tracking
   - Other field transforms
   - Better field substitution primatives
'''

import os, sys, os.path, subprocess, time, itertools, argparse, string

class spin(object):
    def __init__(self, spinchars='|/-\\', *args, **kwds):
        self._chars = itertools.cycle(spinchars)
        self._lastspin = 0
        self._lastout = time.time()
        self._mincycle = .1

    def spin(self):
        ctime = time.time()
        if self._lastspin + self._mincycle > ctime:
            return
        self._output(next(self._chars), delta=ctime-self._lastout, term='\r')
        self._lastspin = ctime

    def _output(self, msg, delta=None, ctime=None, term='\n'):
        if term is None:
            term = ''
        output = ''
        if ctime is not None:
            output += time.ctime(ctime)
        if delta is not None:
            output += '(%.2f)' % delta

        if output:
            output += ' '
        if msg:
            output += msg
        start = ''
        if term:
            if '\r' in term:
                start = ' '
            output += term

        sys.stdout.write(start + output)
        sys.stdout.flush()

    def output(self, msg, *args, **kwds):
        ctime = time.time()
        self._output(msg, ctime = ctime, delta = ctime - self._lastout, *args, **kwds)
        self._lastout = ctime


def showdiff(a, b):
    if not a:
        return b
    if not b:
        return '%s>' % b
    if a == b:
        return a
    return '%s->%s' % (a,b)

class Key(object):
    def __init__(self, key, *args, **kwds):
        self._key = string.Template(key)

    def handle(self, *args, **kwds):
        d = dict(map(lambda x: ('p%s' % x[0], x[1]), enumerate(args)))
        return self._key.substitute(d)

def watch(cmd, key, comments=False, resolvepid=None, tstamp=None, maxsplit=10000, pidreplace=None):
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    spinner = spin()
    timestamp = ''
    running = []
    history = {}
    cmds = {}
    keyhandler = Key(key)
    '''172.100.1.15 /dev/vdb1       83844100 43976  83800124   1% /var/lib/ceph
    '''
    for l in iter(proc.stdout.readline, b''):
        spinner.spin()
        if l.startswith('#') and not comments:
            continue
        ctime = time.time()
        ld = l.strip().split(None, maxsplit)
        curkey = keyhandler.handle(*ld)

        if resolvepid and len(ld) > resolvepid:
            pid = ld[resolvepid]
            if os.path.exists('/proc/%s' % pid):
                if pid not in cmds:
                    try:
                        fullcmd = open('/proc/%s/cmdline' % pid).read().replace('\0', ' ')
                        #spinner.output(fullcmd)
                    except Exception, e:
                        print e
                        fullcmd = cmd
                    cmds[pid] = fullcmd
                cmd = cmds[pid]
                loc = resolvepid
                if pidreplace and len(ld) > pidreplace:
                    loc = pidreplace
                ld[loc] = cmd
            
        if tstamp is not None and ld[tstamp] != timestamp:
            timestamp = ld[tstamp]
            # Clean up any stragglers                                                                                                                                                                                                                                                  
            for k in history.keys():
                if k not in running:
                    history.pop(k)

        if tstamp is not None  and len(ld) > tstamp:
            ld.pop(tstamp)
        if not curkey in running:
            running.append(curkey)
        if curkey not in history:
            history[curkey] = [''] * 8  # The number doesn't really matter....could be len(ld)
        if ld != history[curkey]:
            spinner.output(' '.join(map(lambda x: showdiff(*x), zip(history[curkey], ld))))
            history[curkey] = ld


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-k', action='store', dest='key', default='$p0')
    parser.add_argument('-p', action='store', dest='pid', default=None)
    parser.add_argument('-r', action='store', dest='pidreplace', default=None)
    parser.add_argument('-t', action='store', dest='tstamp', default=None)
    parser.add_argument('-c', action='store_true', dest='comments', default=False)
    parser.add_argument('rest', nargs=argparse.REMAINDER)
    args = parser.parse_args()

    cmd = ' '.join(args.rest)
    if args.pid:
        args.pid = int(args.pid)
    if args.pidreplace:
        args.pidreplace = int(args.pidreplace)
    if args.tstamp:
        args.tstamp = int(args.tstamp)
    watch(args.rest, args.key, comments=args.comments, resolvepid=args.pid, tstamp=args.tstamp, pidreplace=args.pidreplace)

