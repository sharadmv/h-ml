from job import Job
import subprocess as sp
class KMeansJob(Job):

    def read_input(self, line):
        return map(float, line.split('{separator}'))

    def read_mapper(self, line, separator):
        return line.strip().split(separator, 1)

    def map(self, line):
        best = float('inf')
        index = -1
        count = 0
        with open('{centers}') as f:
            for center in f:
                vec = self.read_input(center)
                d = self.distance(vec, line)
                if d < best:
                    index = count
                    best = d
                count += 1
        emit([str(index)] + map(str, line))

    def distance(self, v1, v2):
        def foo(x, y):
            return x + (y[0] - y[1]) ** 2
        return reduce(lambda x, y: foo(x,y), zip(v1, v2), 0)

    def reduce(self, key, values):
        avg = []
        length = 0
        for v in values:
            key, vec = v
            vec = vec.split('\t')
            if (len(avg) == 0):
                avg = [0] * len(vec)
            for i in range(len(vec)):
                avg[i] += float(vec[i])
            length += 1
        if len(avg):
            avg = map(lambda x : x / length, avg)
            emit(list(map(str, avg)))

def main():
    outfile = "/user/sharad/cluster-output%d"
    infile = "/user/sharad/small-output/part-00000"
    incenters = "texton-centers"
    iterations = 5
    for i in range(iterations):
        ofile = outfile % i
        k = KMeansJob(infile, ofile)
        k.run(file=incenters, centers=incenters)
        cmd = (['hdfs dfs -copyToLocal %s/part-00000 output/centers%d' % (ofile, i + 1)])
        print ' '.join(cmd)
        p = sp.Popen(' '.join(cmd), shell=True)
        (stdout, stdin) = p.communicate()
        incenters = "output/centers%i" % (i + 1)

if __name__ == "__main__":
    main()
