#! /usr/bin/env python3
'''
Small class to prepend timestamps to lines output by a command.
Typical usage is:
<command> | ts.py
'''

import os
import os.path
import sys
import subprocess
import datetime
import select
import argparse

class Stamper(object):
    '''
    The class that does all the work.
    '''
    def __init__(self, cmd=None, logfile=None, outputformat=None):
        '''
        Initialize the class
        '''
        self._inhandles = {sys.stdin.fileno(): (sys.stdin, None, 'stdin')}
        self._outhandles = {sys.stdout.fileno(): (sys.stdout, None, 'stdout')}
        self._procs = []
        self._ltime = datetime.datetime.now()
        self._ctime = datetime.datetime.now()
        self._dtime = self._ctime - self._ltime
        self._format = outputformat or '{timestamp} {deltasec:03d}.{deltamsec:06d} {pid} {cmd} - {output}'

        self._handle('Called as: %s\n' % ' '.join(sys.argv))

        if cmd:
            self.add(cmd)
        if logfile:
            self.addout(logfile)

    def add(self, cmd):
        '''
        Add another command in the background.
        '''
        if not cmd:
            return
        proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self._handle('Started: \'%s\' as pid %d\n' % (cmd, proc.pid))
        self._inhandles[proc.stderr.fileno()] = (proc.stderr, proc, 'stderr')
        self._inhandles[proc.stdout.fileno()] = (proc.stdout, proc, 'stdout')
        self._procs.append(proc)

    def addout(self, filename):
        '''
        Add an output sink.
        '''
        outfile = open(filename, 'w')
        self._outhandles[outfile.fileno()] = (outfile, None, filename)
        self._handle('Logging output to %s\n' % os.path.abspath(filename))

    def _handle(self, msg, fproc=None, fdesc='control', verbose=False, update=True):
        '''
        Handle input, sending it to the appropriate sinks.
        '''
        if update:
            self._update()
        for k in self._outhandles:
            ohandle, oproc, odesc = self._outhandles[k]
            if verbose:
                print (ohandle, oproc, odesc)
            if fproc:
                cpid = fproc.pid
            else:
                cpid = os.getpid()
            ohandle.write(self._format.format(
                timestamp=self._ctime.isoformat(),
                deltasec=self._dtime.seconds,
                deltamsec=self._dtime.microseconds,
                pid=cpid,
                cmd=fdesc,
                output=msg))
            ohandle.flush()

    def _update(self):
        '''
        Update the times.
        '''
        self._ctime = datetime.datetime.now()
        self._dtime = self._ctime - self._ltime

    def _cleanup(self, filedesc, fproc):
        '''
        Clean up a process
        '''
        retval = 0
        self._inhandles.pop(filedesc)
        if fproc in self._procs:
            if not fproc.returncode:
                # We got the signal that the child should exit, but it doesn't
                # have a return code.  Wait for the child and set our return code
                fproc.wait()
            retval = fproc.returncode
            self._procs.remove(fproc)
        if not self._procs and self._inhandles.keys() == [sys.stdin.fileno()]:
            self._inhandles.pop(sys.stdin.fileno())
        return retval

    def run(self, verbose=False):
        '''
        Run the commands and handle the output.
        '''
        retval = 0
        while self._inhandles:
            if verbose:
                print(self._inhandles, self._procs, self._outhandles)
            readhandle, writehandle, errorhandle = select.select(self._inhandles.keys(), [], [], 1)
            if verbose:
                sys.stdout.write('%s %s %s ' % (readhandle, writehandle, errorhandle))
            if readhandle:
                self._update()
                for filedesc in readhandle:
                    fhandle, fproc, fdesc = self._inhandles[filedesc]
                    indata = fhandle.readline()
                    if verbose:
                        sys.stdout.write(' %d ' % len(indata))
                    if hasattr(indata, 'decode'):
                        indata = indata.decode()
                    if indata == '':
                        retval |= self._cleanup(filedesc, fproc)
                        continue
                    self._handle(indata, fproc=fproc, fdesc=fdesc, verbose=verbose, update=False)
                self._ltime = self._ctime

        return retval

def main():
    '''
    The main program.
    '''
    parser = argparse.ArgumentParser(
        description='''Prepend timestamp and other data to lines output from a command.

Format uses standard python format() options with the following values:
    timestamp - The timestamp the output was received in ISO 8601 format
    seconds   - The number of seconds since the epoch
    deltasec  - The number of whole seconds elapsed since the last output
    deltamsec - The fractional seconds elapsed since the last output
    pid       - The process id that generated the output
    cmd       - The name of the handle that generated the output
    output    - The output from the command''',
        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-f', '--format',
                        default='{timestamp} {deltasec:03d}.{deltamsec:06d} {pid} {cmd} - {output}')
    args, cmdargs = parser.parse_known_args()

    sys.exit(Stamper(outputformat=args.format, cmd=' '.join(cmdargs)).run(False))

if __name__ == '__main__':
    main()
