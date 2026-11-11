package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        //缓存穿透查询商铺--工具类方法调用
       /* Shop shop = cacheClient
                .queryWithPassThrough(RedisConstants.CACHE_SHOP_KEY, id, Shop.class, this::getById, RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
*/
        //互斥锁解决缓存击穿
        Shop shop = cacheClient.queryWithMutex(RedisConstants.CACHE_SHOP_KEY, id, Shop.class, this::getById, RedisConstants.CACHE_SHOP_TTL, TimeUnit.SECONDS);

        //逻辑过期解决缓存击穿
        /*Shop shop = cacheClient.
                queryWithLogicalExpire(RedisConstants.CACHE_SHOP_KEY, id, Shop.class, this::getById, RedisConstants.CACHE_SHOP_TTL, TimeUnit.SECONDS);
*/
        if (shop == null) {
            return Result.fail("商铺不存在");
        }
        return Result.ok(shop);
    }

    /**逻辑过期解决缓存击穿*/
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    /*public Shop queryWithLogicalExpire(Long id){
        //1.从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);

        //2.判断缓存是否存在
        if(StrUtil.isBlank(shopJson)){
            //3.未命中
            return null;
        }

        //4.命中，需要先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        JSONObject data = (JSONObject)redisData.getData();
        Shop shop = JSONUtil.toBean(data, Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();

        //5.判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())) {
            //5.1未过期，直接返回结果
            return shop;
        }
        //5.2已过期，需要缓存重建
        //6.缓存重建
        //6.1获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);

        //6.2判断是否获取锁成功
        if(isLock){
            //6.3成功，再次检测redis缓存是否过期，做DoubleCheck，如果未过期则直接返回结果
            shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
            // 反序列化，拿到新的过期时间
            RedisData redisData2 = JSONUtil.toBean(shopJson, RedisData.class);
            LocalDateTime expireTime2 = redisData2.getExpireTime();

            // DoubleCheck 判断：缓存已被重建 → 未过期 → 直接返回
            if(expireTime2.isAfter(LocalDateTime.now())){
                JSONObject data2 = (JSONObject) redisData2.getData();
                return JSONUtil.toBean(data2, Shop.class);  // 提前 return，不走后面的重建
            }
            
            // 若过期则需要开启独立线程，查询数据库，重建缓存
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    //重建缓存
                    this.saveShop2Redis(id, 30L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    //释放锁
                    unlock(lockKey);
                }
            });

        }

        //6.4返回过期的商铺信息
        return shop;
    }*/

    /** 带互斥锁查询商铺-解决缓存击穿问题,之前的缓存穿透解决也存在
     * @param id 商铺id
     * @return 商铺对象
     */

    /*@Transactional(rollbackFor = Exception.class)
    public Shop queryWithMutex(Long id){
        //1.从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);

        //2.判断缓存是否存在
        if(StrUtil.isNotBlank(shopJson)){
            //3.将缓存中的json字符串转换为对象并返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class, true);
            return shop;
        }
        //判断命中的是否是空值
        if(shopJson != null){//是空字符串
            return null;
        }
        //4.缓存不存在（shopJson为null），实现缓存重建
        //4.1获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            //4.2判断是否获取到锁
            if(!isLock){
                //4.3失败，则休眠并重试
                Thread.sleep(50);
                return queryWithMutex(id);
            }

            //4.4成功，根据id查询数据库
            //获取锁成功，再次检测redis缓存是否存在，做DoubleCheck，如果存在则无需重建缓存
            shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
            if(StrUtil.isNotBlank(shopJson)){
                //缓存中存在，直接返回

                return JSONUtil.toBean(shopJson, Shop.class, true);
            }
            //判断命中的是否是空值
            if(shopJson != null){//是空字符串
                return null;
            }
            shop = getById(id);
            //模拟重建的延时
            Thread.sleep(200);
            //5.数据库中也不存在
            if(shop == null){
                //将空值写入redis，过期时间2分钟
                stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);

                return null;
            }
            //6.数据库中存在，把对象转成json字符串，写入redis，并设置过期时间
            stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            //7释放互斥锁
            unlock(lockKey);
        }

        //8.返回结果
        return shop;
    }*/

/*    *//**
     * 带穿透查询商铺-解决缓存穿透问题
     * @param id 商铺id
     * @return 商铺对象
     *//*
    public Shop queryWithPassThrough(Long id){
        //1.从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);

        //2.判断缓存是否存在
        if(StrUtil.isNotBlank(shopJson)){
            //3.将缓存中的json字符串转换为对象并返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class, true);
            return shop;
        }
        //判断命中的是否是空值
        if(shopJson != null){//是空字符串
            return null;
        }
        //4.缓存不存在（shopJson为null），根据id查询数据库
        Shop shop = getById(id);
        //5.数据库中也不存在
        if(shop == null){
            //将空值写入redis，过期时间2分钟
            stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);

            return null;
        }
        //6.数据库中存在，把对象转成json字符串，写入redis，并设置过期时间
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);

        //7.返回结果

        return shop;
    }*/

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

    /**
     * 保存店铺到redis-带逻辑过期时间
     * @param id 商铺id
     * @param expiredSeconds 过期时间，单位秒
     */
    public void saveShop2Redis(Long id,Long expiredSeconds){
        //1.查询店铺数据
        Shop shop=getById(id);

        //2.封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expiredSeconds));

        //3.写入redis
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(redisData));
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        //判断商铺是否存在
        if(id == null){
            return Result.fail("店铺id不能为空");
        }
        //1.更新数据库
        updateById(shop);
        //2.删除缓存
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY + id);
        //3.返回结果
        return Result.ok();
    }
}
