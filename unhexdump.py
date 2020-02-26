#! /usr/bin/env python

import sys

outfile = open(sys.argv[2], 'wb')
for line in open(sys.argv[1]):
    lp = line.strip().split()
    if not lp:
        # Blank line most likely
        continue
    try:
        pos = int('0x%s' % lp.pop(0), 16)
    except:
        # Not a valid hexdump line.  Even if it's a *, we
        # can just skip to the next input line
        continue
    # Fill in the gaps
    while outfile.tell() < pos:
        outfile.write(data)
    # Hexdump bytes are encoded such that they need to be swapped before
    # decoding
    data = (''.join(['%s%s' % (x[2:], x[:2]) for x in lp])).decode('hex')
    outfile.write(data)
outfile.close()

