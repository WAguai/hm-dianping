package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
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
import com.hmdp.utils.RedisData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private CacheClient cacheClient;

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    @Override
    public Result queryById(Long id) {
        // 1.缓存穿透
        Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY,id,Shop.class,this::getById,CACHE_SHOP_TTL,TimeUnit.MINUTES);

        // 2.1互斥锁解决缓存击穿
//        Shop shop = queryWithMutex(id);

        // 2.2用逻辑过期解决缓存击穿问题
//        Shop shop1 = cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY,id,Shop.class,this::getById,CACHE_SHOP_TTL,TimeUnit.SECONDS);

        if(shop == null){
            return Result.fail("店铺不存在");
        }
        // 7.返回
        return Result.ok(shop);
    }

//    public Shop queryWithMutex(Long id){
//        // 1.从redis查询商铺缓存
//        String key = CACHE_SHOP_KEY + id;
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        // 2.判断是否存在
//        if(StrUtil.isNotBlank(shopJson)){
//            // 3.存在，直接返回
//            Shop shop = JSONUtil.toBean(shopJson,Shop.class);
//            return shop;
//        }
//        // 命中的是否是空值
//        if(shopJson != null){
//            // 返回错误信息
//            return null;
//        }
//        // 4.不存在，获取互斥锁
//        String lockKey = "lock:shop:" + id;
//        Shop shop = null;
//        try {
//            boolean flag = tryLock(lockKey);
//            while(!flag){
//                flag = tryLock(key);
//                Thread.sleep(50);
//            }
//            // 4.1如果获取到互斥锁，则查数据库，更新到redis
//            shop = getById(id);
//            // 5.数据库不存在，返回错误
//            if(shop == null){
//                // 将空值写入redis
//                stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
//                return null;
//            }
//            // 6.数据库存在，写入redis
//            stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        } finally {
//            unlock(lockKey);
//        }
//        // 7.返回
//        return shop;
//    }
//
//    public Shop queryWithLogicalExpire(Long id){
//        // 1.从redis查询商铺缓存
//        String key = CACHE_SHOP_KEY + id;
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        // 2.判断是否存在
//        if(StrUtil.isBlank(shopJson)){
//            // 3.存在，直接返回
//            return null;
//        }
//        // 4.命中,把json反序列化为对象
//        RedisData redisData = JSONUtil.toBean(shopJson,RedisData.class);
//        JSONObject data = (JSONObject)redisData.getData();
//        Shop shop = JSONUtil.toBean(data,Shop.class);
//        LocalDateTime expireTime = redisData.getExpireTime();
//        // 5 判断是否过期
//        if(expireTime.isAfter(LocalDateTime.now())){
//            // 5.1未过期，返回
//            return shop;
//        }
//        // 5.2已经过期，缓存重建
//        // 6.缓存重建
//        // 6.1获取互斥锁
//        String lockKey = LOCK_SHOP_KEY + id;
//        boolean isLock = tryLock(lockKey);
//        // 6.2判断是否获取锁成功
//        if(isLock){
//            // 6.3成功，开启独立线程，实现缓存重建
//            CACHE_REBUILD_EXECUTOR.submit(()->{
//                try {
//                    // 重建缓存
//                    this.saveShop2Redis(id,20L);
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                } finally {
//                    // 释放锁
//                    unlock(lockKey);
//                }
//            });
//        }
//        // 6.4.失败，返回原始数据
//        return shop;
//    }




    public void saveShop2Redis(Long id,Long expireSeconds) throws InterruptedException {
        // 1.查询店铺数据
        Shop shop = getById(id);
        Thread.sleep(200);
        // 2.封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        // 3.写入Redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,JSONUtil.toJsonStr(redisData));
    }


    @Override
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空");
        }
        // 1.更新数据库
        updateById(shop);
        // 2.删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        return Result.ok();
    }
}
