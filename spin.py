#! /usr/bin/env python

'''
Simple module to provide feedback to the user that work is being done.
'''

import sys
import time
import itertools
import unittest

def _output(msg, delta=None, ctime=None, term='\n'):
    '''
    Send output to stderr.
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

    sys.stderr.write(start + output)
    sys.stderr.flush()


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
        self._mlen = 0

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
        msg = self._prefix + next(self._chars)
        mlen = len(msg)
        if mlen < self._mlen:
            msg += ' ' * (self._mlen - mlen)
        _output(msg, delta=ctime-self._lastout, term='\r')
        self._mlen = mlen
        self._lastspin = ctime
        self._stats['spins'] += 1

    def shift(self, shiftchar='.', groupings=(('X', 10), ('L', 50), ('C', 100), ('D', 500), ('M', 1000))):
        '''
        Shift the spinner over a character.  Useful for indicating a change of processing unit,
        i.e. a new file or directory...
        '''
        self._prefix += shiftchar
        mult = 1
        for groupchar, groupat in groupings:
            groupat /= mult
            self._prefix = self._prefix.replace(shiftchar*groupat, groupchar)
            shiftchar = groupchar
            mult *= groupat
        self._stats['shifts'] += 1

    def output(self, msg, *args, **kwds):
        '''
        Not really sure how this is different than spin() aside from the rate limiter.
        '''
        ctime = time.time()
        if self._lastout + self._mincycle > ctime:
            self._stats['skips'] += 1
            return
        _output(msg, ctime=ctime, delta=ctime - self._lastout, *args, **kwds)
        self._lastout = ctime
        self._stats['msgs'] += 1

    def close(self):
        '''
        Show statistics at the end
        '''
        sys.stderr.write('\n')
        if self._showstats:
            sys.stderr.write('%s\n' % self._stats)

    def get_stats(self):
        '''
        Return the stats to the caller
        '''
        return self._stats

class TestSpin(unittest.TestCase):
    '''
    Tests for IdleSpin class
    '''
    def test_no_spin(self):
        '''
        Test of no spinning
        '''
        spinner = IdleSpin()
        self.assertTrue(spinner.get_stats(), dict(msgs=0, shifts=0, spins=0, skips=0))

    def test_spin_collapse(self):
        '''
        Test collapsing
        '''
        spinner = IdleSpin()
        loops = 3
        for _ in range(0, loops):
            spinner.spin()
        self.assertTrue(spinner.get_stats(), dict(msgs=0, shifts=0, spins=1, skips=(loops-1)))

    def test_spin_normal(self):
        '''
        Test normal spin
        '''
        spinner = IdleSpin()
        loops = 3
        for _ in range(0, loops):
            spinner.spin()
            time.sleep(.15)
        self.assertTrue(spinner.get_stats(), dict(msgs=0, shifts=0, spins=loops, skips=0))

    def test_group(self):
        '''
        Test grouping
        '''
        spinner = IdleSpin()
        for _ in range(0, 30):
            spinner.spin()
            spinner.shift()
        self.assertTrue(spinner.get_stats(), dict(msgs=0, shifts=30, spins=0, skips=0))

def main():
    '''
    Main program
    '''
    #unittest.main()
    spinner = IdleSpin()
    for _ in range(0, 101):
        spinner.spin()
        time.sleep(.15)
        spinner.shift()

if __name__ == '__main__':
    main()
