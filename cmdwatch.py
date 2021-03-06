#! /usr/bin/env python

'''
TODO:
   - Write docs
   - Add tests
   - Integer differences: Total since last sample, average over change time, etc
   - Interactive control
   - Version tracking
   - Other field transforms
   - Better field substitution primatives
   - Store command/args for reuse/config file
   - Alert triggering: threshold, value over time, average over time, etc
   - Server mode
   - Header saving/hiding
   - "Keyless" input, i.e. iostat output
'''

import os
import os.path
import sys
import subprocess
import time
import itertools
import argparse
import string
import select
from spin import IdleSpin

def showdiff(item0, item1):
    '''
    Show when there's a difference between two values
    '''
    if not item0:
        return item1
    if not item1:
        return '%s>' % item1
    if item0 == item1:
        return item0
    return '%s->%s' % (item0, item1)

class Key(object):
    '''
    Handle substitutions
    '''
    def __init__(self, key):
        self._key = string.Template(key)

    def handle(self, *args, **kwds):
        '''
        Do the substitutions.
        '''
        data = dict()
        data.update(kwds)
        data.update(dict([('p%s' % x[0], x[1]) for x in  enumerate(args)]))
        return self._key.substitute(data)

    @property
    def key(self):
        '''
        Return the key.
        '''
        return self._key

class GenOut(object):
    '''
    Generate output
    '''
    def __init__(self, cmd, sleeptime=1, oneshot=False):
        self._cmd = cmd
        self._sleeptime = sleeptime
        self._oneshot = oneshot

    def __iter__(self):
        ltime = time.time()
        ctime = ltime - self._sleeptime
        timehist = []
        while True:
            if timehist:
                timehist.append(self._sleeptime - (ltime - ctime))
            else:
                timehist.append(0)
            if len(timehist) > 10:
                timehist.pop(0)
            ctime = time.time()
            avg = sum(timehist)/len(timehist)
            deadline = ctime + self._sleeptime + avg
            proc = subprocess.Popen(self._cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            for outputline in iter(proc.stdout.readline, b''):
                yield outputline
            if self._oneshot:
                break
            time.sleep(deadline - time.time())
            ltime = time.time()

    def sleeptime(self, sleeptime):
        '''
        Set the sleep time.
        '''
        self._sleeptime = sleeptime

def watch(cmd, key, comments=False, resolvepid=None, tstamp=None,
          maxsplit=10000, pidreplace=None, sleeptime=1, iterations=0,
          oneshot=False, skip=None):
    '''
    Watch the output from a command.
    '''
    spinner = IdleSpin()
    timestamp = ''
    running = []
    history = {}
    cmds = {}
    keyhandler = Key(key)

    outgen = GenOut(cmd, sleeptime=sleeptime, oneshot=oneshot)
    for index, outputline in enumerate(outgen):
        if select.select([sys.stdin], [], [], .0000000001)[0]:
            cmd = sys.stdin.readline().strip().lower()
            if cmd.startswith('q'):
                # Exit app
                break
            elif cmd.startswith('s'):
                parts = cmd.split()
                try:
                    outgen.sleeptime(int(parts[1]))
                except ValueError:
                    print 'Invalid value: %s' % parts[1]
            elif cmd.startswith('i'):
                parts = cmd.split()
                try:
                    iterations = int(parts[1])
                except ValueError:
                    print 'Invalid value: %s' % parts[1]
            else:
                print 'Unknown command: %s' % cmd
        if iterations and index > iterations:
            break
        spinner.spin()
        if outputline.startswith('#') and not comments:
            continue
        linetokens = outputline.strip().split(None, maxsplit)
        curkey = keyhandler.handle(*linetokens)

        if curkey in skip:
            continue

        if resolvepid and len(linetokens) > resolvepid:
            pid = linetokens[resolvepid]
            if os.path.exists('/proc/%s' % pid):
                if pid not in cmds:
                    try:
                        fullcmd = open('/proc/%s/cmdline' % pid).read().replace('\0', ' ')
                    except IOError:
                        fullcmd = cmd
                    cmds[pid] = fullcmd
                cmd = cmds[pid]
                loc = resolvepid
                if pidreplace and len(linetokens) > pidreplace:
                    loc = pidreplace
                linetokens[loc] = cmd

        if tstamp is not None and linetokens[tstamp] != timestamp:
            timestamp = linetokens[tstamp]
            # Clean up any stragglers
            for k in history:
                if k not in running:
                    history.pop(k)

        if tstamp is not None and len(linetokens) > tstamp:
            linetokens.pop(tstamp)
        if not curkey in running:
            running.append(curkey)
        if curkey not in history:
            history[curkey] = [''] * 8  # The number doesn't really matter....could be len(linetokens)
        if linetokens != history[curkey]:
            spinner.output(' '.join([showdiff(*x) for x in zip(history[curkey], linetokens)]))
            history[curkey] = linetokens


def main():
    '''
    Main program....it's here so that the silly lint rules are masked
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('-k', action='store', dest='key', default='$p0')
    parser.add_argument('-p', action='store', dest='pid', default=None)
    parser.add_argument('-r', action='store', dest='pidreplace', default=None)
    parser.add_argument('-t', action='store', dest='tstamp', default=None)
    parser.add_argument('-i', action='store', dest='interval', type=int, default=1)
    parser.add_argument('-I', action='store', dest='iterations', type=int, default=0)
    parser.add_argument('-c', action='store_true', dest='comments', default=False)
    parser.add_argument('-o', action='store_true', dest='oneshot',
                        default=False, help='Only run the command one time')
    parser.add_argument('-s', action='append', dest='skip', default=[], nargs='+')
    parser.add_argument('rest', nargs=argparse.REMAINDER)
    args = parser.parse_args()

    # Compact any skipped fields
    args.skip = ','.join(list(itertools.chain(*args.skip))).split(',')

    if args.pid:
        args.pid = int(args.pid)
    if args.pidreplace:
        args.pidreplace = int(args.pidreplace)
    if args.tstamp:
        args.tstamp = int(args.tstamp)
    watch(args.rest, args.key, comments=args.comments, resolvepid=args.pid, tstamp=args.tstamp,
          pidreplace=args.pidreplace, sleeptime=args.interval, iterations=args.iterations,
          oneshot=args.oneshot, skip=args.skip)

if __name__ == '__main__':
    main()
