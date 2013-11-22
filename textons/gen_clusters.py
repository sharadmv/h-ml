import argparse
import numpy as np

parser = argparse.ArgumentParser()
parser.add_argument('--k', type=int, help='number of clusters to generate')
parser.add_argument('--data', help='data to generate cluster centers for')
parser.add_argument('--output', help='cluster center output file', default='centers')

def main(args):
    k = args.k
    f = args.data
    output = args.output
    data = read_data(f)
    n, d = data.shape
    bound_idx = np.random.randint(n, size=10)
    bounds = data[bound_idx]
    lower_bound = np.amin(bounds, axis=0)
    upper_bound = np.amax(bounds, axis=0)
    centers = []
    for _ in range(k):
        centers.append(gen_sample(lower_bound, upper_bound))
    centers = np.array(centers)
    np.savetxt(output, centers, fmt='%1.4f', delimiter='\t')

def read_data(filename):
    return np.loadtxt(filename)


def gen_sample(lower_bound, upper_bound):
    sample = []
    for l, u in zip(lower_bound, upper_bound):
        sample.append((u - l) * np.random.random_sample() + l)
    return sample


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
