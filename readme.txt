Redis-py
--------

Attribution:
    First off this wouldn't be possible without the work of Ludovico Magnocavallo and Andy McCurdy. Having their code to look over while developing made it easy to stand on the shoulders of giants.

Reasoning:
    I had a personal project which I really wanted to use Redis on, however, the python package at the time didn't support subscribe/unsubscribe or the new hash functions. It seemed like a wonderful opportunity to help out as well as learn the ins and outs of the Redis comms protocol. In the end it also really helped me learn about the Redis database.
    
Design:
    In most ways the Redis object works just like you would expect to. It does however, differ from the actual Redis command reference in a couple ways. First it has aliases for "del" -> "delete" and "exec" -> "execute". This is due to the fact that 'del' and 'exec' are reserved words in python.
    In addition to the normal Redis class, which provides normal interaction with the Redis Database, there is also a set of helper classes designed to be used with some of Redis' newer features. First is a more general RedisHelper class that makes some things a little easier (such as turning hgetall into a native dictionary).
    I had considered doing some sort of Object mapping, but I think this causes developers to treat a database more like an Object Database and less like a kickass kvs.
    Another helper class included is one specifically designed to handle channel subscription. It provides a couple simple methods for dealing with receiving and queueing messages. This should make it easy to integrate into a application in a non-blocking way. This method probably isn't foolproof and might not be the most efficient way, but it will make the pub/sub experimentation a little easier.
    
Tests:
    I've tried to create a pretty comprehensive set of tests, mostly just to practice TDD but also to make the module as robust as possible. More tests and helper classes should make it into 1.1
    
Examples:
    # the examples are pretty simple
    import redis
    
    db = redis.Redis()
    db.set('key', 'value')

    x = db.get('key')
    
    #x == 'value'
    
