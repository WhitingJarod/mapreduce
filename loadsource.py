#!/usr/bin/env python

import sqlite3
import sys
import os.path

def main():
    if len(sys.argv) < 3:
        print >>sys.stderr, 'Usage: %s [dbname] [inputfile] ...' % sys.argv[0]
        sys.exit(-1)

    db = sqlite3.connect(sys.argv[1])
    db.execute(u'CREATE TABLE IF NOT EXISTS pairs (key string, value string)')

    for fname in sys.argv[2:]:
        print 'processing %s...' % os.path.basename(fname)
        fp = open(fname)
        offset = 0
        for line in fp:
            key = u'%s:%9d' % (os.path.basename(fname), offset)
            value = unicode(line.rstrip('\r\n'), 'utf8')
            offset += len(line)

            db.execute(u'INSERT INTO pairs VALUES (?, ?)', (key, value))
        fp.close()

    #db.execute(u'CREATE INDEX IF NOT EXISTS idx_pairs ON pairs (key, value)')
    db.commit()
    db.close()

if __name__ == '__main__':
    main()
