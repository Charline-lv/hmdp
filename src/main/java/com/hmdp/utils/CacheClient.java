package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Component
@Slf4j
public class CacheClient {
    /*这里用的构造器注入stringRedisTemplate
    * 也可以自动注入@Resource*/
    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    /**设置缓存
     * 将任意对象序列化为为json字符串，并存储在String类型的key中，且可以设置TTL过期时间
     * 写入redis*/
    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    /**设置缓存，逻辑过期
     * 将任意对象序列化为为json字符串，并存储在String类型的key中，且可以设置逻辑过期时间，用于处理缓存击穿问题
     * 写入redis*/
    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        //设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        //写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    /**根据指定key查询缓存并反序列化为指定类型，利用缓存空值的方式解决缓存穿透问题*/
       public <R, ID> R queryWithPassThrough(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {

           String key = keyPrefix + id;
            //1.从redis查询商铺缓存
            String json = stringRedisTemplate.opsForValue().get(key);

            //2.判断缓存是否存在
            if(StrUtil.isNotBlank(json)){
                //3.将缓存中的json字符串转换为对象并返回
                return JSONUtil.toBean(json, type, true);
            }
            //判断命中的是否是空值
            if(json != null){//是空字符串
                return null;
            }
            //4.缓存不存在（json为null），根据id查询数据库---通过函数式接口实现
            R r = dbFallback.apply(id);
            //5.数据库中也不存在
            if(r == null){
                //将空值写入redis，过期时间2分钟
                stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);

                return null;
            }
            //6.数据库中存在，把对象转成json字符串，写入redis，并设置过期时间
            this.set(key, r, time, unit);

            //7.返回结果

            return r;
    }

    /**根据指定key查询缓存并反序列化为指定类型，利用缓存逻辑过期问题，解决缓存击穿问题*/
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public <R,ID> R queryWithLogicalExpire(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        //1.从redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);

        //2.判断缓存是否存在
        if(StrUtil.isBlank(json)){
            //3.未命中
            return null;
        }

        //4.命中，需要先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        JSONObject data = (JSONObject)redisData.getData();
        R r = JSONUtil.toBean(data, type);
        LocalDateTime expireTime = redisData.getExpireTime();

        //5.判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())) {
            //5.1未过期，直接返回结果
            return r;
        }
        //5.2已过期，需要缓存重建
        //6.缓存重建
        //6.1获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);

        //6.2判断是否获取锁成功
        if(isLock){
            //6.3成功，再次检测redis缓存是否过期，做DoubleCheck，如果未过期则直接返回结果
            json = stringRedisTemplate.opsForValue().get(key);
            // 反序列化，拿到新的过期时间
            RedisData redisData2 = JSONUtil.toBean(json, RedisData.class);
            LocalDateTime expireTime2 = redisData2.getExpireTime();

            // DoubleCheck 判断：缓存已被重建 → 未过期 → 直接返回
            if(expireTime2.isAfter(LocalDateTime.now())){
                JSONObject data2 = (JSONObject) redisData2.getData();
                return JSONUtil.toBean(data2, type);  // 提前 return，不走后面的重建
            }

            // 若过期则需要开启独立线程，查询数据库，重建缓存
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    //查询数据库
                    R r1 = dbFallback.apply(id);
                    //写入redis
                    this.setWithLogicalExpire(key, r1, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    //释放锁
                    unlock(lockKey);
                }
            });

        }

        //6.4返回过期的商铺信息
        return r;
    }

    /** 带互斥锁查询商铺-解决缓存击穿问题,之前的缓存穿透解决也存在
     * @param id 商铺id
     * @return 商铺对象
     */

    @Transactional(rollbackFor = Exception.class)
    public <R,ID> R queryWithMutex(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit){
        String key = keyPrefix + id;
        //1.从redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);

        //2.判断缓存是否存在
        if(StrUtil.isNotBlank(json)){
            //3.将缓存中的json字符串转换为对象并返回
            R r = JSONUtil.toBean(json, type, true);
        }
        //判断命中的是否是空值
        if(json != null){//是空字符串
            return null;
        }
        //4.缓存不存在（json为null），实现缓存重建
        //4.1获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        R r = null;
        try {
            boolean isLock = tryLock(lockKey);
            //4.2判断是否获取到锁
            if(!isLock){
                //4.3失败，则休眠并重试
                Thread.sleep(50);
                return queryWithMutex(keyPrefix, id, type, dbFallback, time, unit);
            }

            //4.4成功，根据id查询数据库
            //获取锁成功，再次检测redis缓存是否存在，做DoubleCheck，如果存在则无需重建缓存
            json = stringRedisTemplate.opsForValue().get(key);
            if(StrUtil.isNotBlank(json)){
                //缓存中存在，直接返回

                return JSONUtil.toBean(json, type, true);
            }
            //判断命中的是否是空值
            if(json != null){//是空字符串
                return null;
            }
            r = dbFallback.apply(id);
            //模拟重建的延时
            Thread.sleep(200);
            //5.数据库中也不存在
            if(r == null){
                //将空值写入redis，过期时间2分钟
                stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            //6.数据库中存在，把对象转成json字符串，写入redis，并设置过期时间
            this.set(key, r, time, unit);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            //7释放互斥锁
            unlock(lockKey);
        }

        //8.返回结果
        return r;
    }

    /**
     * 尝试获取锁
     * @param key 锁的key
     * @return 是否获取成功
     */
    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);//拆箱后返回boolean值
    }
    /**
     * 释放锁
     * @param key 锁的key
     */
    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }
}
