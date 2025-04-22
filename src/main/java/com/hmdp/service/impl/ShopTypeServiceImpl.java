package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.util.List;

import static com.hmdp.utils.RedisConstants.CACHE_SHOPTYPES_KEY;

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
    @Autowired
    private ShopTypeMapper shopTypeMapper;

    @Override
    public Result queryTypeList() {

        String shopTypesJson = stringRedisTemplate.opsForValue().get(CACHE_SHOPTYPES_KEY);
        if(StrUtil.isNotBlank(shopTypesJson)){
            List<ShopType> shopTypes = JSONUtil.toList(shopTypesJson, ShopType.class);
            return Result.ok(shopTypes);
        }
        List<ShopType> shopTypes = shopTypeMapper.selectList(null);
        // 5.数据库不存在，返回错误
        if(shopTypes == null){
            return Result.fail("无店铺类型");
        }
        stringRedisTemplate.opsForValue().set(CACHE_SHOPTYPES_KEY,JSONUtil.toJsonStr(shopTypes));
        return Result.ok(shopTypes);

    }
}
