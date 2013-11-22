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

    def run(self, **kwargs):
        self.gen(**kwargs)

        cmd = (['hadoop jar', '$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar',
                  '-mapper ./tmp/mapper.py', '-reducer ./tmp/reducer.py',
                  '-input %s' % self.input, '-output %s' % self.output])
        for k,v in kwargs.items():
            if k == 'file':
                cmd.append('-%s %s' % (k, v))
        print ' '.join(cmd)
        p = sp.Popen(' '.join(cmd), shell=True)
        (stdout, stdin) = p.communicate()
        print stdout

    def gen(self, **kwargs):
        job = inspect.getsource(self.__class__)
        name = self.__class__.__name__
        mapper = open('template/map_template.py').read().format(
            mapper=job,
            name=name,
            separator=self.separator,
        )
        reducer = open('template/reduce_template.py').read().format(
            reducer=job,
            name=name,
            separator=self.separator,
        )
        mapper = mapper.format(
            separator=self.separator,
            name=name,
            **kwargs
        )
        reducer = reducer.format(
            separator=self.separator,
            name=name,
            **kwargs
        )
        #for k, v in kwargs.items():
            #mapper = mapper.replace("{%s}" % k, v)
            #reducer = reducer.replace("{%s}" % k, v)
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

