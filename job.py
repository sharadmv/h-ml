import subprocess as sp
import inspect
import os

class Job:
    def __init__(self, input, output, separator='\t'):
        self.input = input
        self.output = output
        self.separator = separator

    def read_input(self, line):
        raise NotImplementedError

    def read_mapper(self, line):
        raise NotImplementedError

    def map(self, line):
        raise NotImplementedError

    def reducer(self, key, values):
        raise NotImplementedError

    def run(self):
        self.gen()

        cmd = (['hadoop jar', '$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar',
                  '-mapper ./tmp/mapper.py', '-reducer ./tmp/reducer.py',
                  '-input %s' % self.input, '-output %s' % self.output])
        print ' '.join(cmd)
        p = sp.Popen(' '.join(cmd), shell=True)
        (stdout, stdin) = p.communicate()
        print stdout

    def gen(self):
        job = inspect.getsource(self.__class__)
        name = self.__class__.__name__
        mapper = open('template/map_template.py').read().format(
            mapper=job,
            separator=self.separator,
            name=name
        )
        reducer = open('template/reduce_template.py').read().format(
            reducer=job,
            separator=self.separator,
            name=name
        )
        if not os.path.exists('tmp/'):
            os.makedirs('tmp/')
        f = open('tmp/mapper.py', 'w')
        f.write(mapper)
        f.close()
        os.chmod('tmp/mapper.py', 0777)
        f = open('tmp/reducer.py', 'w')
        f.write(reducer)
        f.close()
        os.chmod('tmp/reducer.py', 0777)

def sanitize(code):
    #code = code.replace('\t','    ')
    return '\n'.join(map(lambda x : x[4:], code.split('\n')))

