#! /usr/bin/env python3

import os
import sys
import os.path
import subprocess
import time
import datetime
import select
import pdb

class Stamper(object):
    def __init__(self, cmd=None, logfile=None, *args, **kwds):
        self._inhandles = {sys.stdin.fileno(): (sys.stdin, None, 'stdin')}
        self._outhandles = {sys.stdout.fileno(): (sys.stdout, None, 'stdout')}
        self._procs = []
        self._ltime = datetime.datetime.now()

        self._handle('Called as: %s\n' % ' '.join(sys.argv))

        if cmd:
            self.add(cmd)
        if logfile:
            self.addout(logfile)

    def add(self, cmd, *args, **kwds):
        if len(cmd) == 0:
            return
        proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self._handle('Started: \'%s\' as pid %d\n' % (cmd, proc.pid))
        self._inhandles[proc.stderr.fileno()] = (proc.stderr, proc, 'stderr')
        self._inhandles[proc.stdout.fileno()] = (proc.stdout, proc, 'stdout')
        self._procs.append(proc)

    def addout(self, filename, *args, **kwds):
        outfile = open(filename, 'w')
        self._outhandles[outfile.fileno()] = (outfile, None, filename)
        self._handle('Logging output to %s\n' % os.path.abspath(filename))

    def _handle(self, msg, fproc=None, fdesc='control', verbose=False, update=True, *args, **kwds):
        if update:
            self._update()
        for k in self._outhandles.keys():
            ohandle, oproc, odesc = self._outhandles[k]
            if verbose:
                print (ohandle, oproc, odesc)
            if fproc:
                cpid = fproc.pid
            else:
                cpid = os.getpid()
            ohandle.write('%s(%03d.%06d)[%s.%s] - %s' % (self._ctime.isoformat(), self._dtime.seconds, self._dtime.microseconds, cpid, fdesc, msg))
            ohandle.flush()

    def _update(self, *args, **kwds):
        self._ctime = datetime.datetime.now()
        self._dtime = self._ctime - self._ltime

    def run(self, verbose=False, *args, **kwds):
        while self._inhandles:
            if verbose:
                print(self._inhandles, self._procs, self._outhandles)
            rh, wh, eh = select.select(self._inhandles.keys(), [], [], 1)
            if verbose:
                sys.stdout.write('%s %s %s ' % (rh, wh, eh))
            if rh:
                self._update()
                for fh in rh:
                    fhandle, fproc, fdesc = self._inhandles[fh]
                    d = fhandle.readline()
                    if verbose:
                        sys.stdout.write(' %d ' % len(d))
                    if hasattr(d, 'decode'):
                        d = d.decode()
                    if d == '':
                        self._inhandles.pop(fh)
                        if fproc in self._procs:
                            self._procs.remove(fproc)
                        if len(self._procs) == 0 and [x for x in self._inhandles.keys()] == [sys.stdin.fileno()]:
                            self._inhandles.pop(sys.stdin.fileno())
                        continue
                    self._handle(d, fproc=fproc, fdesc=fdesc, verbose=verbose, update=False)
                self._ltime = self._ctime

        
if __name__ == '__main__':
    cmd = ' '.join(sys.argv[1:])
    stamper = Stamper(cmd=cmd, logfile=os.path.join(os.path.expanduser('~'), 'logs', '%s_%d' % (cmd.replace(' ', '_').replace(os.path.sep, '_'), time.time())))
    stamper.run(False)
