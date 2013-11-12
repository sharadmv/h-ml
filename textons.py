from job import Job

class TextonExtractor(Job):
    def read_input(self, line):
        return map(int, line.split())
    def read_mapper(self, line, separator):
        return line.strip().split(separator, 1)
    def map(self, line):
        [x, y] = line[0:2]
        line = line[2:]
        for i in range(0, x - 5, 5):
            for j in range(0, y - 5, 5):
                arr = []
                total = 0
                for m in range(i, i + 5):
                    for n in range(j, j + 5):
                        val = line[m*x+n]
                        total += val
                        arr.append(val)
                temp = []
                mean = total / (len(arr) + 0.0)
                for v in arr:
                    temp.append(v - mean)
                emit(['texton'] + map(str, temp))
    def reduce(self, key, values):
        map(lambda x: emit(x[1:]), values)

if __name__ == "__main__":
    job = TextonExtractor("/user/sharad/textures", "/user/sharad/t-output")
    job.run()

