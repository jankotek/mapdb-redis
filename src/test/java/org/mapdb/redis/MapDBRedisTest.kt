package org.mapdb.redis

import org.junit.Test
import org.junit.Assert.assertEquals
import org.mapdb.*


/**
 * Basic functional test for redis commands
 *
 * @author Jan Kotek
 */
public class MapDBRedisTest() {

    val d = MapDBRedis.init();

    fun eq(a:Any, b:Any){
        assertEquals(a,b);
    }


    [Test] public fun get() {
        d.set("aa", "bb")
        assertEquals("bb", d.get("aa"))
    }

    [Test] public fun setrange() {
        d.set("a", "Hello World")
        d.setrange("a", 6, "Redis")
        assertEquals("Hello Redis", d.get("a"))
    }

    [Test] public fun getrange() {
        d.set("a","This is a string")
        assertEquals("This",d.getrange("a",0,3));
        assertEquals("ing",d.getrange("a",-3,-1));
        assertEquals("This is a string",d.getrange("a",0,-1));
        assertEquals("string",d.getrange("a",10,100));
    }

    [Test] public fun setnx() {
        assertEquals(1L,d.setnx("a","h"))
        assertEquals(0L,d.setnx("a","b"))
        assertEquals("h",d.get("a"))
    }

    [Test] public fun decrby() {
        assertEquals("OK",d.set("a","11"))
        assertEquals(6L,d.decrBy("a",5))
    }

    [Test] public fun append() {
        assertEquals(false, d.exists("a"))
        assertEquals(5L, d.append("a","hello"))
        assertEquals(11L, d.append("a"," world"))
        assertEquals("hello world", d.get("a"))
    }

    [Test] public fun hset_hget(){
        assertEquals(1L,d.hset("a","b","hello"))
        assertEquals("hello",d.hget("a","b"))
    }

    [Test] public fun hsetnx() {
        assertEquals(1L,d.hsetnx("a","b","h"))
        assertEquals(0L,d.hsetnx("a","b","b"))
        assertEquals("h",d.hget("a","b"))
    }


    [Test] public fun hincrby() {
        assertEquals(1L, d.hset("a","f","5"))
        assertEquals(6L, d.hincrBy("a","f",1))
        assertEquals(5L, d.hincrBy("a","f",-1))
        assertEquals(-5L, d.hincrBy("a","f",-10))
    }

    [Test] public fun sets(){
        assertEquals(1L, d.sadd("a","5"))
        assertEquals(2L, d.sadd("a","6","7"))
        assertEquals(true, d.sismember("a","6"))
        assertEquals(false, d.sismember("a","1"))
        assertEquals(3L, d.scard("a"))
        assertEquals(2L, d.srem("a","6","7"))
        assertEquals("5", d.spop("a"))
        assertEquals(0L, d.scard("a"))
        assertEquals(1L, d.sadd("a","5"))
        assertEquals("5", d.srandmember("a"))
        assertEquals(1L, d.scard("a"))

    }


    [Test] public fun strlen(){
        assertEquals("OK",d.set("aa", "abcd"));
        assertEquals(4L, d.strlen("aa"))
        assertEquals(0L, d.strlen("nonexistent"))
    }


    [Test] public fun zadd(){
        assertEquals(1L, d.zadd("a", 1.0,"one"))
        assertEquals(1L, d.zadd("a", 1.0,"uno"))
        assertEquals(1L, d.zadd("a", 2.0,"two"))
        assertEquals(0L, d.zadd("a", 3.0,"two"))

    }
}



