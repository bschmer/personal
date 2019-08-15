#! /usr/bin/env python

'''
Simple module to provide feedback to the user that work is being done.
'''

import sys
import time
import itertools

def _output(msg, delta=None, ctime=None, term='\n'):
    '''
    Send output to stdout.
    '''
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


class IdleSpin(object):
    '''
    Simple class to show that things are happening, i.e. we're not stuck
    '''
    def __init__(self, spinchars='|/-\\', mincycle=.1, showstats=True):
        '''
        Initialize the spinner
        '''
        self._chars = itertools.cycle(spinchars)
        self._lastspin = 0
        self._prefix = ''
        self._lastout = time.time()
        self._mincycle = mincycle
        self._showstats = showstats
        self._stats = dict(msgs=0, shifts=0, spins=0, skips=0)


    def __enter__(self):
        '''
        Handler for context manager
        '''
        return self

    def __exit__(self, type, value, traceback): # pylint:disable=redefined-builtin
        '''
        Handler for context manager
        '''
        self.close()

    def spin(self):
        '''
        Display the indicator
        '''
        ctime = time.time()
        if self._lastspin + self._mincycle > ctime:
            self._stats['skips'] += 1
            return
        _output(self._prefix + next(self._chars), delta=ctime-self._lastout, term='\r')
        self._lastspin = ctime
        self._stats['spins'] += 1

    def shift(self, shiftchar='.'):
        '''
        Shift the spinner over a character.  Useful for indicating a change of processing unit,
        i.e. a new file or directory...
        '''
        self._prefix += shiftchar
        self._stats['shifts'] += 1

    def output(self, msg, *args, **kwds):
        '''
        Not really sure how this is different than spin() aside from the rate limiter.
        '''
        ctime = time.time()
        _output(msg, ctime=ctime, delta=ctime - self._lastout, *args, **kwds)
        self._lastout = ctime
        self._stats['msgs'] += 1

    def close(self):
        '''
        Show statistics at the end
        '''
        print ''
        if self._showstats:
            print self._stats
