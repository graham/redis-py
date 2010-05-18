import time
from nose.tools import *
import redis
import redis_helpers

def prepare_db():
    x = redis.Redis()
    x.connect()
    x.flushall()
    return x

def prepare_dbh():
    x = redis_helpers.RedisHelper()
    x.connect()
    x.flushall()
    return x

def test_ping():
    x = redis.Redis()
    x.connect()
    result = x.ping()
    assert(result)
    
# Commands operating on all the kinds of values
key = 'mykey'
key2 = 'mykey2'
key3 = 'mykey3'
ikey = 'intkey'

value = 'value'
value2 = 'newvalue'
value3 = 'foobar-nuts-blurg'
ivalue = 400
non_existant_key = 'asdfjkl'

def test_exist():
    db = prepare_db()
    assert_equal(db.exists(key), False)
    db.set(key, value)
    assert_equal(db.exists(key), True)

def test_del():
    db = prepare_db()
    db.set(key, value)
    assert(db.exists(key))
    db.delete(key)
    assert_equal(db.exists(key), False)

def test_type():
    db = prepare_db()
    db.set(key, value)
    db.set(ikey, ivalue)
    assert_equal(db.type(key), 'string')
    #assert_equal(db.type(list_key), 'list')
    #assert_equal(db.type(set_key), 'set')
    #assert_equal(db.type(zset_key), 'zset')
    #assert_equal(db.type(hash_key), 'hash')
    assert_equal(db.type(non_existant_key), 'none')

def test_keys():
    db = prepare_db()
    db.set(key, value)
    db.set(key2, value)
    db.set(key3, value)

    assert_equal(db.keys('my*'), [key, key2, key3])

def test_randomkey():
    db = prepare_db()
    db.set(key, value)
    db.set(key2, value)
    db.set(key3, value)

    assert(db.randomkey() in [key, key2, key3])

def test_rename():
    db = prepare_db()
    db.set(key, value)
    db.rename(key, key2)
    assert_equal(db.get(key2), value)
    assert_equal(db.exists(key), False)

def test_renamenx():
    db = prepare_db()
    db.set(key, value)
    result = db.renamenx(key, key2)
    assert_equal(db.exists(key), False)
    assert_equal(db.get(key2), value)
    assert_equal(result, 1)
    db.delete(key2)

    db.set(key, value)
    db.set(key2, value2)

    result = db.renamenx(key, key2)
    assert_equal(db.get(key), value)
    assert_equal(db.get(key2), value2)
    assert_equal(result, 0)

def test_dbsize():
    db = prepare_db()
    assert_equal(db.dbsize(), 0)
    db.set(key, value)
    assert_equal(db.dbsize(), 1)
    db.set(key2, value2)
    assert_equal(db.dbsize(), 2)

def test_expire():
    db = prepare_db()
    db.set(key, value)
    db.expire(key, 1)
    time.sleep(2)
    assert_equal(db.exists(key), False)

def test_expire_at():
    db = prepare_db()
    db.set(key, value)
    current_time = int(time.time())
    db.expireat(key, current_time+1)
    time.sleep(2)
    assert_equal(db.exists(key), False)

def test_ttl():
    db = prepare_db()
    db.set(key, value)
    assert_equal(db.ttl(key), -1)

    db.expire(key, 5)
    assert_equal(db.ttl(key), 5)

    current_time = int(time.time())
    db.expireat(key, current_time+5)
    assert_equal(db.ttl(key), 5)

def test_select():
    db = prepare_db()
    db.select(0)

    db.set(key, value)
    assert(db.exists(key))

    db.select(1)
    assert_equal(db.exists(key), False)
    db.set(key2, value2)
    assert(db.exists(key2))

    db.select(0)
    assert(db.exists(key))
    assert_equal(db.get(key), value)
    assert_equal(db.exists(key2), False)

def test_move():
    db = prepare_db()

    #cannot move non existant key
    assert_equal(db.move(non_existant_key, 1), 0)

    #move a key to another db
    db.select(0)
    db.set(key, value)
    assert(db.exists(key))

    db.select(1)
    assert_equal(db.exists(key), False)

    db.select(0)
    result = db.move(key, 1)
    assert_equal(db.exists(key), False)
    db.select(1)
    assert(db.exists(key))
    assert_equal(result, 1)

    #failing case for already existant key
    db = prepare_db()
    db.select(0)
    db.set(key, value)
    db.select(1)
    db.set(key, value)
    db.select(0)

    assert_equal(db.move(key, 1), 0)

def test_flushall():
    db = prepare_db()
    db.select(0)
    db.set(key, value)

    db.select(1)
    db.set(key, value)

    db.flushall()

    db.select(0)
    assert_equal(db.dbsize(), 0)
    db.select(1)
    assert_equal(db.dbsize(), 0)

def test_flushdb():
    db = prepare_db()
    db.select(0)
    db.set(key, value)
    assert_equal(db.dbsize(), 1)

    db.select(1)
    db.set(key, value)
    assert_equal(db.dbsize(), 1)

    db.select(0)
    db.flushdb()
    assert_equal(db.dbsize(), 0)
    db.select(1)
    assert_equal(db.dbsize(), 1)

#Commands operating on string values
def test_get_and_set():
    db = prepare_db()
    db.set(key, value)
    assert_equal(db.get(key), value)

def test_getset():
    db = prepare_db()
    db.set(key, value)
    assert_equal(db.get(key), value)
    last_value = db.getset(key, value2)
    assert_equal(last_value, value)
    assert_equal(db.get(key), value2)

def test_mget():
    db = prepare_db()
    db.set(key, value)
    db.set(key2, value2)

    assert_equal(db.mget(key, key2), [value, value2])

def test_setnx():
    db = prepare_db()
    assert not db.exists(key)

    result = db.setnx(key, value)
    assert result

    result = db.setnx(key, value2)
    assert not result
    assert_equal(db.get(key), value)
    
def test_setex():
    db = prepare_db()
    assert not db.exists(key)

    db.setex(key, 2, value)
    assert db.exists(key)
    time.sleep(3)
    assert not db.exists(key)

def test_mset():
    db = prepare_db()
    assert_equal( filter(bool, [db.exists(key), db.exists(key2), db.exists(key3)]), [] )
    db.mset(key, value, key2, value2, key3, value3)
    assert_equal( db.mget(key, key2, key3), [value, value2, value3] )
    
def test_msetnx():
    db = prepare_db()
    assert_equal( filter(bool, [db.exists(key), db.exists(key2), db.exists(key3)]), [] )
    db.mset(key, value)

    result = db.msetnx(key2, value2, key3, value3)
    assert result

    result = db.msetnx(key, value)
    assert not result

def test_incr_decr():
    db = prepare_db()

    db.set(ikey, ivalue)
    new_value = db.incr(ikey)
    assert new_value == (ivalue + 1)

    new_value = db.decr(ikey)
    assert new_value == ivalue

def test_incrby_decrby():
    db = prepare_db()

    db.set(ikey, ivalue)
    new_value = db.incrby(ikey, 200)
    assert new_value == (ivalue + 200)

    new_value = db.decrby(ikey, 50)
    assert new_value == ivalue + 150

def test_append():
    db = prepare_db()

    db.set(key, value)
    db.append(key, 'asdf')
    assert (value + 'asdf') == db.get(key)

def test_substr():
    # Small diff here, python is non-inclusive, redis is. So numbers are off.
    db = prepare_db()

    db.set(key, value)
    assert value[0:3] == db.substr(key, 0, 2)

# commands on lists.

def test_push_pop_len():
    db = prepare_db()

    db.rpush(key, '1')
    db.rpush(key, '2')

    db.lpush(key, 'foo')
    db.lpush(key, 'bar')

    assert 4 == db.llen(key)
    assert '2' == db.rpop(key)
    assert 'bar' == db.lpop(key)
    assert 'foo' == db.lpop(key)
    assert '1' == db.rpop(key)
    assert 0 == db.llen(key)

def test_lrange():
    db = prepare_db()

    db.rpush(key, '1')
    db.rpush(key, '2')
    db.lpush(key, '0')

    assert db.llen(key) == 3
    assert db.lrange(key, 0, 2) == ['0', '1', '2']

def test_ltrim():
    db = prepare_db()

    map(lambda x: db.rpush(key, x), ['one', 'two', 'three', 'four', 'five', 'six', 'seven'])

    db.ltrim(key, 1, 4)
    assert db.lrange(key, 0, db.llen(key)-1) == ['two', 'three', 'four', 'five']

def build_list(db):
    db.rpush(key, value)
    db.rpush(key, value2)
    db.rpush(key, value3)

def test_lindex():
    db = prepare_db()
    build_list(db)
    assert_equal(db.lindex(key, 0), value)
    assert_equal(db.lindex(key, -1), value3)

def test_lset():
    db = prepare_db()
    build_list(db)
    db.lset(key, 1, 'blurg!')
    assert_equal(db.lindex(key, 1), 'blurg!')

def test_lrem():
    dbh = prepare_dbh()

    assert_equal( 0, dbh.lrem(key, 0, value) )
    
    build_list(dbh)
    dbh.rpush(key, value)
    dbh.rpush(key, value)

    result = dbh.lrem(key, 1, value)
    assert_equal( result, 1 )
    assert_equal( dbh.lgetall(key), [value2, value3, value, value])

    result = dbh.lrem(key, 0, value)
    assert_equal( result, 2 )
    assert_equal( dbh.lgetall(key), [value2, value3] )

    dbh.rpush(key, value)
    dbh.rpush(key, value)
    dbh.rpush(key, value)

    result = dbh.lrem( key, -2, value)
    assert_equal( result, 2 )
    assert_equal( dbh.lgetall(key), [value2, value3, value] )

def test_lpop_rpop():
    dbh = prepare_dbh()
    build_list(dbh)

    assert_equal( dbh.lpop(key), value )
    assert_equal( dbh.rpop(key), value3 )
    assert_equal( dbh.rpop(key), value2 )

########### UNFINISHED TEST ##########
def test_blpop_brpop():
    db1 = prepare_db()
    db2 = prepare_db()

    assert True

def test_rpoplpush():
    dbh = prepare_dbh()
    build_list(dbh)
    assert_equal( dbh.lgetall(key), [value, value2, value3] )
    
    dbh.rpoplpush(key, key2)
    assert_equal( dbh.lgetall(key), [value, value2] )
    assert_equal( dbh.lgetall(key2), [value3] )

    dbh.rpoplpush(key, key2)
    assert_equal( dbh.lgetall(key), [value] )
    assert_equal( dbh.lgetall(key2), [value2, value3] )

########### UNFINISHED TEST ##########
def test_sort():
    dbh = prepare_dbh()
    assert True

# methods on sets.
def build_set(db):
    db.sadd(key, value)
    db.sadd(key, value2)
    db.sadd(key, value2)
    db.sadd(key, value3)
    
def test_sadd_srem():
    db = prepare_db()
    build_set(db)
    
    for i in db.smembers(key):
        assert i in [value, value2, value3]

    db.srem(key, value)

    for i in db.smembers(key):
        assert i in [value2, value3]

def test_spop():
    db = prepare_db()
    build_set(db)

    result = db.spop(key)
    assert result in [value, value2, value3]

def test_scard():
    db = prepare_db()
    build_set(db)

    assert_equal( db.scard(key), 3 )

def test_sismember():
    db = prepare_db()
    build_set(db)

    assert db.sismember( key, value )
    assert not db.sismember( key, 'non-existant-value' )

def test_sinter():
    db = prepare_db()
    build_set(db)
    db.sadd(key2, value)
    assert_equal( db.sinter(key, key2), [value] )

def test_sinterstore():
    db = prepare_db()
    build_set(db)
    db.sadd(key2, value)
    db.sinterstore( key3, key, key2 )
    assert_equal( db.smembers(key3), [value] )

def test_union():
    pass

