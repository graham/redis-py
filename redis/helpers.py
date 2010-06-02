from redis import Redis

class RedisHelper(Redis):
    def hgetall(self, key):
        d = {}
        values = Redis.do(self, 'hgetall', key)
        for i in range(0, len(values), 2):
            d[values[i]] = values[i+1]
        return d
    
    def info(self):
        result = Redis.do(self, 'info')
        d = {}
        for i in result.split('\r\n'):
            key, value = i.split(':', 1)
            if '=' in value:
                tempd = {}
                for i in value.split(','):
                    k, v = i.split('=')
                    tempd[k] = v
                d[key] = tempd
            elif ',' in value:
                d[key] = value.split(',')
            else:
                if value.isdigit():
                    d[key] = int(value)
                else:
                    d[key] = value
        return d

    def lgetall(self, key):
        return self.lrange(key, 0, self.llen(key))

class MockFP(object):
    def __init__(self):
        self.buf = ''
    
    def write(self, b):
        self.buf += b
    
    def read(self, l):
        h = self.buf[:l]
        t = self.buf[l:]
        self.buf = t
        
        return h
    
    def readline(self):
        for i in ('\r\n', '\n', '\r'):
            index = self.buf.find(i)
            if index != -1:
                b = self.buf[:index+(len(i))]
                self.buf = self.buf[index+(len(i)):]
                return b
        #else
        return ''
    
    def peekline(self):
        for i in ('\r\n', '\n', '\r'):
            index = self.buf.find(i)
            if index != -1:
                return True
        #else
        return ''
        
    def readlines(self):
        d = self.readline()
        while d:
            yield d
            d = self.readline()
            
class RedisSubscriber(Redis):
    def __init__(self, *args, **kwargs):
        Redis.__init__(self, *args, **kwargs)
        self.mfp = MockFP()
        self.callbacks = []
    
    def add_callback(self, cb):
        self.callbacks.append(cb)
        
    def recv(self, l=None):
        data = ''
        
        if self.mfp.buf:
            if l:
                data = self.mfp.read(l)
            else:
                data = self.mfp.readline()
        else:
            if l:
                data = self.fp.read(l)
            else:
                data = self.fp.readline()

        return data.strip()
    
    def poll(self):
        tmp = select.select( [self.sock], [], [], 0.0)
        if tmp[0]:
            self.mfp.write(self.sock.recv(512))
            return 1
        else:
            return 0
    
    def waiting(self):
        return self.mfp.peekline()
    
    def wait_for_message(self):
        return self.read_message()

    def read_message(self):
        result = self.read()
        for i in self.callbacks:
            i(self, result)
        return result

# implementation of Pipeline object, inspired by pyredis.
# It's very jquery-esk, and only somewhat useful, basically just a clean way
# to do fast multi/exec groupings. Frankly I'm liking it more every day.
class PipelineCommand(object):
    def __init__(self, command, pipeline):
        self.command = command
        self.pipeline = pipeline
        
    def __call__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        
        return self.pipeline
    
class Pipeline(object):
    def __init__(self, r):
        self.redis = r # redis connection object
        self.queue = []
        
    def __getattr__(self, key):
        if key not in self.__dict__:
            p = PipelineCommand(key, self)
            self.queue.append(p)
            return p
        else:
            return object.__getattr__(self, key)
        
    def execute(self):
        self.redis.multi()
        for i in self.queue:
            self.redis.do(i.command, *i.args, **i.kwargs)
        return self.redis.execute()


# helper functions

def unzip_list(resp, count):
    l = []
    for i in range(0, len(resp), count):
        d = resp[i:i + count]
        l.append(d)
    return l
    