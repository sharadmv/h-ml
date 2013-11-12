import sys
import argparse
import numpy as np
from glob import glob
from scipy import ndimage, misc

def main(args):
    files = glob("%s/%s" % (args.dir, '*.png'))
    files.sort()
    for f in files[0:10]:
        print >>sys.stderr, f
        extract(misc.imread(f))

def extract(img):
    print >>sys.stdout, img.shape[0], img.shape[0], ' '.join(map(str, img.flatten()))

parser = argparse.ArgumentParser()
parser.add_argument("--dir")

if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
