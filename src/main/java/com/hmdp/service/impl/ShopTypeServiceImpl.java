package com.hmdp.service.impl;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryTypeList() {
        String key = RedisConstants.CACHE_SHOP_TYPE_KEY;
        //1.从redis查询商铺类型缓存 , end:-1 表示取全部数据
        List<String> shopTypeList =  stringRedisTemplate.opsForList().range(key, 0, -1);

        //2.判断缓存是否存在
        if(CollectionUtil.isNotEmpty(shopTypeList)){
            //3.将缓存中的json字符串转换为对象,排序后返回
            List<ShopType> shopTypes = JSONUtil.toList(shopTypeList.toString(), ShopType.class);
            Collections.sort(shopTypes, (o1, o2) -> o1.getSort()-(o2.getSort()));
            return Result.ok(shopTypes);
        }
        //4.缓存不存在，查询数据库
        List<ShopType> shopTypes = query().orderByAsc("sort").list();
        //5.数据库中也不存在
        if(CollectionUtil.isEmpty(shopTypes)){
            return Result.fail("商铺类型不存在");
        }
        //6.数据库中存在，写入redis
        // 这里使用Redis的List类型，String类型，就是直接所有都写在一起，对内存开销比较大。
        // 要将List中的每个元素(元素类型ShopType) ，每个元素都要单独转成JSON，使用stream流的map映射
        // Hutools里的 BeanUtil.copyToList 本来想模仿UserService中的写法，
        // 传入一个CopyOptions的，但是setFieldValueEditor貌似只对beanToMap有效
        // 改用流的形式转换每个list元素
        List<String> shopTypeJsonList = shopTypes.stream()
                .map(shopType -> JSONUtil.toJsonStr(shopType))
                .collect(Collectors.toList());

        // 写入redis
        // 因为从数据库读出来的时候已经是按照顺序读出来的，这里想要维持顺序必须从右边push，类似队列的效果
        stringRedisTemplate.opsForList().rightPushAll(key, shopTypeJsonList);

        //7.返回结果
        return Result.ok(shopTypes);
    }
}
