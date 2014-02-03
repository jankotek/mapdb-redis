package org.mapdb.redis

import org.mapdb.DB
import org.mapdb.DBMaker
import redis.clients.jedis.*
import org.mapdb.HTreeMap
import java.util.Collections
import java.util.ArrayList

/**
 * Created by jan on 1/11/14.
 */
public open class MapDBRedis(db: DB) : JedisCommands {

    protected val db: DB
    protected val keys: HTreeMap<String, String>

    protected val OK: String = "OK"

    {
        this.db = db
        this.keys = db.getHashMap("keys")
    }

    class object {
        public open fun init(): MapDBRedis {
            //TODO open bug ticket in Kotlin for generics inheritance
            return MapDBRedis(
                    (DBMaker.newDirectMemoryDB()
                    .transactionDisable() as DBMaker<*>)
                    .make()
                )
        }
    }



    public override fun set(key: String, value: String): String {
        keys.put(key, value)
        return OK
    }

    public override fun get(key: String): String? {
        return keys.get(key)
    }

    public override fun exists(key: String): Boolean {
        return keys.containsKey(key)!!
    }

    public override fun persist(key: String): Long {
        //TODO key expiry
        throw UnsupportedOperationException("no expiration yet")
    }

    public override fun `type`(key: String): String {
        val v = keys.get(key)
        if (v == null)
            return "none"

        if (v is String)
            return "string"

        //TODO types
        throw Error("Unknown type: " + v)
    }

    public override fun expire(key: String, seconds: Int): Long {
        //TODO key expiry
        throw UnsupportedOperationException("no expiration yet")
    }

    public override fun expireAt(key: String, unixTime: Long): Long {
        //TODO key expiry
        throw UnsupportedOperationException("no expiration yet")
    }

    public override fun ttl(key: String): Long {
        //TODO key expiry
        throw UnsupportedOperationException("no expiration yet")
    }

    public override fun setbit(key: String, offset: Long, value: Boolean): Boolean {
        //TODO bit wise
        throw UnsupportedOperationException()
    }

    public override fun setbit(key: String, offset: Long, value: String): Boolean {
        //TODO bit wise
        throw UnsupportedOperationException()
    }

    public override fun getbit(key: String, offset: Long): Boolean {
        //TODO bit wise
        throw UnsupportedOperationException()
    }

    public override fun setrange(key: String, offset: Long, value: String): Long {
        val s = keys.get(key)
        if(s==null)
            return 0;

        val s2 = s.substring(0, offset.toInt()) + value
        keys.put(key, s2)
        return s2.length().toLong();
    }


    override fun getrange(key: String, startOffset: Long, endOffset: Long): String? {
        val s = keys.get(key);
        if(s==null)
            return null;
        var start = startOffset.toInt()
        if(start<0)
            start = s.length+start;
        start = Math.max(0,start);

        var end = endOffset.toInt();
        if(end<0)
            end = s.length+end;
        end = Math.min(s.length,end+1);

        return s.substring(start,end);
    }

    override fun getSet(key: String, value: String): String? {
        return keys.put(key,value);
    }

    override fun setnx(key: String, value: String): Long? {
        return if(keys.putIfAbsent(key,value)==null) 1 else 0;
    }
    override fun setex(key: String, seconds: Int, value: String): String? {
        //TODO timeout
        throw UnsupportedOperationException("no expiration yet")
    }

    override fun decrBy(key: String, integer: Long): Long? {
        val n = keys.get(key)!!.toLong()-integer
        keys.put(key, n.toString())
        return n
    }

    override fun decr(key: String): Long? {
        return decrBy(key,1)
    }

    override fun incrBy(key: String, integer: Long): Long? {
        return decrBy(key,-integer);
    }

    override fun incr(key: String): Long? {
        return incrBy(key,1)
    }

    override fun append(key: String, value: String): Long? {
        val n = keys.getOrElse(key,{""})+value;
        keys.put(key,n);
        return n.length.toLong();
    }

    override fun substr(key: String, start: Int, end: Int): String? {
        //TODO unknown command
        throw UnsupportedOperationException()
    }

    override fun hset(key: String, field: String, value: String): Long? {
        val m:HTreeMap<String,String> = db.getHashMap(key)
        val old = m.put(field,value);
        return if(old==null) 1 else 0;
    }

    override fun hget(key: String, field: String): String? {
        val m:HTreeMap<String,String> = db.getHashMap(key)
        return m.get(field);
    }

    override fun hsetnx(key: String, field: String, value: String): Long? {
        val m:HTreeMap<String,String> = db.getHashMap(key)
        return if(m.putIfAbsent(field,value)==null) 1 else 0;
    }

    override fun hmset(key: String, hash: Map<String, String>): String? {
        val m:HTreeMap<String,String> = db.getHashMap(key)
        for((hkey,hval) in hash){
            m.put(hkey,hval)
        }
        return OK
    }

    override fun hincrBy(key: String, field: String, value: Long): Long? {
        val m:HTreeMap<String,String> = db.getHashMap(key)
        val n = m.get(field)!!.toLong()+value
        m.put(field, n.toString())
        return n
    }

    override fun hexists(key: String, field: String): Boolean? {
        val m:HTreeMap<String,String> = db.getHashMap(key)
        return m.containsKey(field);
    }

    override fun hdel(key: String, vararg field: String?): Long? {
        val m:HTreeMap<String,String> = db.getHashMap(key)
        var c = 0L;
        for(f in field){
            if(m.remove(f)!=null)
                c++
        }
        return c;
    }
    override fun hlen(key: String): Long? {
        val m:HTreeMap<String,String> = db.getHashMap(key)
        return m.size().toLong();
    }
    override fun hkeys(key: String): MutableSet<String>? {
        val m:HTreeMap<String,String> = db.getHashMap(key)
        return m.keySet() //TODO defensive copy?
    }

    override fun hvals(key: String): MutableList<String>? {
        val m:HTreeMap<String,String> = db.getHashMap(key)
        return ArrayList(m.values())
    }

    override fun hgetAll(key: String): MutableMap<String, String>? {
        val m:HTreeMap<String,String> = db.getHashMap(key)
        return m //TODO defensive copy?
    }

    override fun rpush(key: String, vararg string: String?): Long? {
        //TODO lists
        throw UnsupportedOperationException("Lists not implemented yet")
    }
    override fun lpush(key: String, vararg string: String?): Long? {
        //TODO lists
        throw UnsupportedOperationException("Lists not implemented yet")
    }
    override fun llen(key: String): Long? {
        //TODO lists
        throw UnsupportedOperationException("Lists not implemented yet")
    }
    override fun lrange(key: String, start: Long, end: Long): MutableList<String>? {
        //TODO lists
        throw UnsupportedOperationException("Lists not implemented yet")
    }
    override fun ltrim(key: String, start: Long, end: Long): String? {
        //TODO lists
        throw UnsupportedOperationException("Lists not implemented yet")
    }
    override fun lindex(key: String, index: Long): String? {
        //TODO lists
        throw UnsupportedOperationException("Lists not implemented yet")
    }
    override fun lset(key: String, index: Long, value: String): String? {
        //TODO lists
        throw UnsupportedOperationException("Lists not implemented yet")
    }
    override fun lrem(key: String, count: Long, value: String): Long? {
        //TODO lists
        throw UnsupportedOperationException("Lists not implemented yet")
    }
    override fun lpop(key: String): String? {
        //TODO lists
        throw UnsupportedOperationException("Lists not implemented yet")
    }
    override fun rpop(key: String): String? {
        //TODO lists
        throw UnsupportedOperationException()
    }
    override fun sadd(key: String, vararg member: String?): Long? {
        val s:MutableSet<String> = db.getHashSet(key)
        var ret = 0L;
        for(m in member)
            if(m!=null && s.add(m))
                ret++;
        return ret;
    }
    override fun smembers(key: String): MutableSet<String> {
        val s:MutableSet<String> = db.getHashSet(key)
        return s; //TODO defensive copy?
    }
    override fun srem(key: String, vararg member: String?): Long? {
        val s:MutableSet<String> = db.getHashSet(key)
        var ret = 0L;
        for(m in member)
            if(m!=null && s.remove(m))
                ret++;
        return ret;
    }

    override fun spop(key: String): String? {
        val s:MutableSet<String> = db.getHashSet(key)
        val iter = s.iterator();
        if(!iter.hasNext())
            return null;
        val ret = iter.next();
        iter.remove();
        return ret;
    }
    override fun scard(key: String): Long? {
        val s:MutableSet<String> = db.getHashSet(key);
        return s.size().toLong()
    }

    override fun sismember(key: String, member: String): Boolean? {
        val s:MutableSet<String> = db.getHashSet(key);
        return s.contains(member);
    }

    override fun srandmember(key: String): String? {
        val s:MutableSet<String> = db.getHashSet(key)
        val iter = s.iterator();
        return if(iter.hasNext()) iter.next() else null;
    }
    override fun strlen(key: String): Long? {
        val s = keys.get(key);
        return if(s is String)
            s.length.toLong()
        else
            0;
    }
    override fun zadd(key: String, score: Double, member: String): Long? {
        //TODO counter BTree
        throw UnsupportedOperationException("sorted set supported yet")
    }
    override fun zadd(key: String, scoreMembers: Map<Double, String>): Long? {
        //TODO counter BTree
        throw UnsupportedOperationException("sorted set supported yet")

    }
    override fun zrange(key: String, start: Long, end: Long): MutableSet<String> {
        //TODO counter BTree
        throw UnsupportedOperationException("sorted set supported yet")

    }
    override fun zrem(key: String, vararg member: String?): Long? {
        //TODO counter BTree
        throw UnsupportedOperationException("sorted set supported yet")

    }
    override fun zincrby(key: String, score: Double, member: String): Double? {
        //TODO counter BTree
        throw UnsupportedOperationException("sorted set supported yet")
    }

    override fun zrank(key: String, member: String): Long? {
        //TODO counter BTree
        throw UnsupportedOperationException("sorted set supported yet")
    }
    override fun zrevrank(key: String, member: String): Long? {
        //TODO counter BTree
        throw UnsupportedOperationException("sorted set supported yet")
    }

    override fun zrevrange(key: String, start: Long, end: Long): MutableSet<String> {
        //TODO counter BTree
        throw UnsupportedOperationException("sorted set supported yet")
    }
    override fun zrangeWithScores(key: String, start: Long, end: Long): MutableSet<Tuple> {
        //TODO counter BTree
        throw UnsupportedOperationException("sorted set supported yet")
    }

    override fun zrevrangeWithScores(key: String, start: Long, end: Long): MutableSet<Tuple> {
        //TODO counter BTree
        throw UnsupportedOperationException("sorted set supported yet")
    }
    override fun zcard(key: String): Long? {
        //TODO counter BTree
        throw UnsupportedOperationException("sorted set supported yet")
    }
    override fun zscore(key: String, member: String): Double? {
        //TODO counter BTree
        throw UnsupportedOperationException("sorted set supported yet")
    }

    override fun zcount(key: String, min: Double, max: Double): Long? {
        //TODO counter BTree
        throw UnsupportedOperationException("sorted set supported yet")

    }
    override fun zcount(key: String, min: String, max: String): Long? {
        //TODO counter BTree
        throw UnsupportedOperationException("sorted set supported yet")

    }
    override fun zrangeByScore(key: String, min: Double, max: Double): MutableSet<String> {
        //TODO counter BTree
        throw UnsupportedOperationException("sorted set supported yet")

    }
    override fun zrangeByScore(key: String, min: String, max: String): MutableSet<String> {
        //TODO counter BTree
        throw UnsupportedOperationException("sorted set supported yet")

    }
    override fun zrevrangeByScore(key: String, max: Double, min: Double): MutableSet<String> {
        //TODO counter BTree
        throw UnsupportedOperationException("sorted set supported yet")

    }
    override fun zrangeByScore(key: String, min: Double, max: Double, offset: Int, count: Int): MutableSet<String> {
        //TODO counter BTree
        throw UnsupportedOperationException("sorted set supported yet")

    }
    override fun zrevrangeByScore(key: String, max: String, min: String): MutableSet<String> {
        //TODO counter BTree
        throw UnsupportedOperationException("sorted set supported yet")

    }
    override fun zrangeByScore(key: String, min: String, max: String, offset: Int, count: Int): MutableSet<String> {
        //TODO counter BTree
        throw UnsupportedOperationException("sorted set supported yet")

    }
    override fun zrevrangeByScore(key: String, max: Double, min: Double, offset: Int, count: Int): MutableSet<String> {
        //TODO counter BTree
        throw UnsupportedOperationException("sorted set supported yet")

    }
    override fun zrangeByScoreWithScores(key: String, min: Double, max: Double): MutableSet<Tuple> {
        //TODO counter BTree
        throw UnsupportedOperationException("sorted set supported yet")

    }
    override fun zrevrangeByScoreWithScores(key: String, max: Double, min: Double): MutableSet<Tuple> {
        //TODO counter BTree
        throw UnsupportedOperationException("sorted set supported yet")

    }
    override fun zrangeByScoreWithScores(key: String, min: Double, max: Double, offset: Int, count: Int): MutableSet<Tuple> {
        //TODO counter BTree
        throw UnsupportedOperationException("sorted set supported yet")

    }
    override fun zrevrangeByScore(key: String, max: String, min: String, offset: Int, count: Int): MutableSet<String> {
        //TODO counter BTree
        throw UnsupportedOperationException("sorted set supported yet")

    }
    override fun zrangeByScoreWithScores(key: String, min: String, max: String): MutableSet<Tuple> {
        //TODO counter BTree
        throw UnsupportedOperationException("sorted set supported yet")

    }
    override fun zrevrangeByScoreWithScores(key: String, max: String, min: String): MutableSet<Tuple> {
        //TODO counter BTree
        throw UnsupportedOperationException("sorted set supported yet")

    }
    override fun zrangeByScoreWithScores(key: String, min: String, max: String, offset: Int, count: Int): MutableSet<Tuple> {
        //TODO counter BTree
        throw UnsupportedOperationException("sorted set supported yet")

    }
    override fun zrevrangeByScoreWithScores(key: String, max: Double, min: Double, offset: Int, count: Int): MutableSet<Tuple> {
        //TODO counter BTree
        throw UnsupportedOperationException("sorted set supported yet")

    }
    override fun zrevrangeByScoreWithScores(key: String, max: String, min: String, offset: Int, count: Int): MutableSet<Tuple> {
        //TODO counter BTree
        throw UnsupportedOperationException("sorted set supported yet")

    }
    override fun zremrangeByRank(key: String, start: Long, end: Long): Long? {
        //TODO counter BTree
        throw UnsupportedOperationException("sorted set supported yet")

    }
    override fun zremrangeByScore(key: String, start: Double, end: Double): Long? {
        //TODO counter BTree
        throw UnsupportedOperationException("sorted set supported yet")

    }
    override fun zremrangeByScore(key: String, start: String, end: String): Long? {
        //TODO counter BTree
        throw UnsupportedOperationException("sorted set supported yet")

    }

    override fun sort(key: String): MutableList<String>? {
        throw UnsupportedOperationException()
    }
    override fun sort(key: String, sortingParameters: SortingParams?): MutableList<String>? {
        throw UnsupportedOperationException()
    }

    override fun linsert(key: String, where: BinaryClient.LIST_POSITION?, pivot: String, value: String): Long? {
        throw UnsupportedOperationException()
    }
    override fun lpushx(key: String, vararg string: String?): Long? {
        throw UnsupportedOperationException()
    }
    override fun rpushx(key: String, vararg string: String?): Long? {
        throw UnsupportedOperationException()
    }
    override fun blpop(arg: String?): MutableList<String>? {
        throw UnsupportedOperationException()
    }
    override fun brpop(arg: String?): MutableList<String>? {
        throw UnsupportedOperationException()
    }
    override fun del(key: String?): Long? {
        throw UnsupportedOperationException()
    }
    override fun echo(string: String): String? {
        throw UnsupportedOperationException()
    }
    override fun move(key: String, dbIndex: Int): Long? {
        throw UnsupportedOperationException()
    }
    override fun bitcount(key: String): Long? {
        throw UnsupportedOperationException()
    }
    override fun bitcount(key: String, start: Long, end: Long): Long? {
        throw UnsupportedOperationException()
    }
    override fun hmget(key: String, vararg fields: String?): MutableList<String>? {
        throw UnsupportedOperationException()
    }
}
