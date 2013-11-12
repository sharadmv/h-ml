from job import Job
class WordCount(Job):
    def read_input(self, line):
        return line.split()
    def read_mapper(self, line, separator):
        return line.strip().split(separator, 1)
    def map(self, line):
        for word in line:
            stripped = ''.join(c for c in word if 0 < ord(c) < 127)
            if stripped != '':
                emit([stripped.lower(), '1'])
    def reduce(self, key, values):
        val = sum(int(count) for key, count in values)
        emit([key, str(val)])

if __name__ == "__main__":
    job = WordCount("/user/sharad/joyce.txt", "/user/sharad/output")
    job.run()
