package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements ILock {

    private  String name;
    private StringRedisTemplate stringRedisTemplate;

    public SimpleRedisLock(String name,StringRedisTemplate stringRedisTemplate) {
        this.name = name;
        this.stringRedisTemplate = stringRedisTemplate;
    }

    private static final String KEY_PREFIX = "lock:";
    private static final String ID_PREFIX = UUID.randomUUID().toString(true) + "-";

    @Override
    public boolean tryLock(long timeoutSec) {

        String key = KEY_PREFIX + name;
        //获取线程标识id-用UUID拼上线程id
        String threadId = ID_PREFIX + Thread.currentThread().getId();

        //尝试获取锁
        Boolean result = stringRedisTemplate.opsForValue()
                .setIfAbsent(key, threadId, timeoutSec, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(result);
    }

    @Override
    public void unlock() {
        //获取线程标识
        String threadId = ID_PREFIX + Thread.currentThread().getId();
        //获取锁中的标识
        String key = KEY_PREFIX + name;
        String id = stringRedisTemplate.opsForValue().get(key);

        //判断标识是否一致
        if (id.equals(threadId)) {
            //释放锁
            stringRedisTemplate.delete(key);
        }


    }
}
