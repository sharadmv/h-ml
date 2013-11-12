#!/usr/bin/env python
"""A more advanced Reducer, using Python iterators and generators."""

from itertools import groupby
from operator import itemgetter
import sys

class Job:
    pass

{reducer}

r = {name}()

def emit(values):
    print '{separator}'.join(values)

def ri(file):
    for line in file:
        yield r.read_mapper(line, '{separator}')

def main():
    data = ri(sys.stdin)
    for current_word, group in groupby(data, itemgetter(0)):
        r.reduce(current_word, group)

if __name__ == "__main__":
    main()
