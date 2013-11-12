#!/usr/bin/env python
"""A more advanced Mapper, using Python iterators and generators."""

import sys
class Job:
    pass


{mapper}

m = {name}()

def ri(file):
    for line in file:
        yield m.read_input(line)

def emit(values):
    print '{separator}'.join(values)

def main():
    # input comes from STDIN (standard input)
    data = ri(sys.stdin)
    for line in data:
        # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # Reduce step, i.e. the input for reducer.py
        #
        # tab-delimited; the trivial word count is 1
        m.map(line)

if __name__ == "__main__":
    main()
