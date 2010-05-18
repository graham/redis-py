__version__ = 0.1
__author__ = 'Graham Abbott'
__email__ = 'graham@grahamalot.com'

import socket
import select

connection_commands = ['quit', 'auth']
general_commands = ['exists', 'del', 'type', 'keys', 'randomkey', 
                    'rename', 'renamenx', 'dbsize', 'expire', 'expireat', 
                    'ttl', 'select', 'move', 'flushdb', 'flushall']
string_commands = ['set', 'get', 'getset', 'mget', 'setnx', 'setex', 'mset',
                   'msetnx', 'incr', 'incrby', 'decr', 'decrby', 'append', 
                   'substr']
list_commands = ['rpush', 'lpush', 'llen', 'lrange', 'ltrim', 'lindex', 
                 'lset', 'lrem', 'lpop', 'rpop', 'blpop', 'brpop', 
                 'rpoplpush']
set_commands = ['sadd', 'srem', 'spop', 'smove', 'scard', 'sismember', 
                'sinter', 'sinterstore', 'sunion', 'sunionstore', 'sdiff', 
                'sdiffstore', 'smembers', 'srandmember']
sorted_set_commands = ['zadd', 'zrem', 'zincrby', 'zrank', 'zrevrank', 
                       'zrangebyscore', 'zcard', 'zscore', 'zremrangebyrank', 
                       'zremrangebyscore', 'zunion', 'zinter']
hash_commands = ['hset', 'hget', 'hmset', 'hincrby', 'hexists', 'hdel', 
                 'hlen', 'hkeys', 'hvals', 'hgetall']
sort_commands = ['sort']
transaction_commands = ['multi', 'exec', 'discard']
pubsub_commands = ['subscribe', 'unsubscribe', 'publish']
server_commands = ['save', 'bgsave', 'lastsave', 'shutdown', 'bgrewriteaof']
remote_commands = ['info', 'monitor', 'slaveof']

DISABLED_COMMANDS = ['monitor']
SUPPORTED_BULK_COMMANDS = connection_commands + general_commands + string_commands + list_commands + set_commands + sort_commands + hash_commands + pubsub_commands + server_commands + remote_commands

UNSUPPORTED_BULK_COMMANDS = transaction_commands + sort_commands

COMMAND_ALIASES = {
    'delete':'del',
    'execute':'exec'
}

STRICT_MODE = False

ERROR = '-'
SINGLE = '+'
BULK = '$'
MULTI = '*'
INTEGER = ':'
OK = ("OK", "+ok", "+OK")

none_values = ['$-1', '*-1']

class Redis(object):
    def __init__(self, host='localhost', port=6379, db=0, timeout=None, charset='utf-8', encode_errors='strict'):
        self.host = host
        self.port = int(port)
        self.db = int(db)
        self.charset = charset
        self.encode_errors = encode_errors

        if timeout:
            socket.setdefaulttimeout(timeout)

        self.sock = None
        self.fp = None
        self.connect()
        
    def enc(self, s):
        if isinstance(s, str): # fast path
            return s
        if not isinstance(s, unicode):
            return str(s)
        try:
            return s.encode(self.charset, self.encode_errors)
        except UnicodeEncodeError, e:
            raise Exception("Error encoding unicode s '%s': %s" % (
                    s.encode(self.charset, 'replace'), e))

    def dec(self, s):
        if not self.charset:
            return s
        return s.decode(self.charset)
        
    def connect(self):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self.host, self.port))
        except socket.error, e:
            raise Exception("Error %s connecting to %s:%s. %s." %
                                  (e.args[0], self.host, self.port, e.args[1]))
        sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        self.sock = sock
        self.fp = sock.makefile('r')
    
    def send(self, command):
        try:
            self.sock.sendall(command)
        except socket.error, e:
            print 'error sending data'
    
    def len_send(self, command):
        self.send("$%i\r\n%s\r\n" % (len(command), self.enc(command)))
    
    def bulk_send(self, *args):
        self.send("*%i\r\n" % len(args))
        for i in args:
            self.len_send(i)
            
    def recv(self, l=None):
        data = ''
        if l:
            data = self.fp.read(l)
        else:
            data = self.fp.readline()

        return data.strip()
    
    def read(self):
        data = self.recv(1)
        if data == SINGLE:
            rest = self.recv().strip()
            if rest in OK:
                return 1
            return rest
        elif data == INTEGER:
            return int(self.recv().strip())
        elif data == BULK:
            result = self._read_bulk(data)
            return result
        elif data == MULTI:
            count = int(self.recv())
            result = []
            for i in range(0, count):
                result.append(self.read())
            return result
        elif data == ERROR:
            raise Exception("Error: " + self.recv())
        else:
            return data + self.recv()

    def _read_bulk(self, data=''):
        inp = data + self.recv().strip()

        content = ''
        if inp in OK:
            return True
        elif inp in none_values:
            return None
        elif inp.startswith('$'):
            length = int(inp[1:])
            if length == -1:
                return None
            content = self.recv(length)
            content = self.dec(content)
        elif inp.startswith(INTEGER):
            content = int(inp[1:])
        else:
            content = inp
        self.recv(2) # \r\n
        return content
            
    def ping(self):
        self.send("PING\r\n")
        result = self.recv()
        if result == '+PONG':
            return True
    
    def do(self, *args):
        a = []
        for i in args:
            if type(i) != str:
                a.append(self.enc(str(i)))
            else:
                a.append(self.enc(i))
        
        self.bulk_send(*a)
        
        return self.read()

    def __getattr__(self, key):
        if key in self.__dict__:
            return object.__getattr__(self, key)
        elif key in SUPPORTED_BULK_COMMANDS:
            return lambda *x: self.do(key, *x)
        elif key in COMMAND_ALIASES:
            return lambda *x: self.do(COMMAND_ALIASES[key], *x)
        else:
            if STRICT_MODE:
                return object.__getattr__(self, key)
            else:
                return lambda *x: self.do(key, *x)
    
    # I feel like this shouldn't be in the stock redis object, but i'll leave
    # it in for now.
    def pipe(self):
        from redis_helpers import Pipeline
        p = Pipeline(self)
        return p
    
if __name__ == '__main__':
    from redis_helpers import *

    db = Redis()
    dbh = RedisHelper()