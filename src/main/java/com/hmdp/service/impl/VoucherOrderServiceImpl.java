package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Autowired
    private IVoucherOrderService voucherOrderService;
    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result seckillVoucher(Long voucherId) {
        // 1.查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);

        // 2.判断秒杀是否开始
        if(voucher.getBeginTime().isAfter(LocalDateTime.now())){
            return Result.fail("秒杀还没开始");
        }
        // 3. 判断秒杀是否结束
        if(voucher.getEndTime().isBefore(LocalDateTime.now())){
            return Result.fail("秒杀已经结束");
        }
        // 4.判断库存是否充足
        if(voucher.getStock() <1){
            return Result.fail("库存不足");
        }

        Long userId = UserHolder.getUser().getId();
        // 在事物之外添加锁，保证事物提交

        // 创建锁对象
        SimpleRedisLock lock = new SimpleRedisLock("order:"+userId,stringRedisTemplate);
        // 获取锁
        boolean isLock = lock.tryLock(1200);
        if(!isLock){
            // 获取锁失败
            return Result.fail("不允许重复下单");
        }
        try {
            // createVoucherOrder 方法拥有事务的能力是因为 spring aop 生成代理了对象，但是这种方法直接调用了 this 对象的方法，所以 createVoucherOrder 方法不会生成事务。
            //3.3 通过 AopContent 类
            //在该 Service 类中使用 AopContext.currentProxy() 获取代理对象。
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            // 或者通过注入自己的方式
            return voucherOrderService.createVoucherOrder(voucherId);
        }finally {
            lock.unlock();
        }
    }

    @Transactional
    public Result createVoucherOrder(Long voucherId) {
        // 5.一人一单
        Long userId = UserHolder.getUser().getId();
        // intern返回一个字符串的规范表示，会先去字符串常量池中查询
        int count = query().eq("voucher_id", voucherId).eq("user_id", userId).count();
        if (count > 0) {
            return Result.fail("用户已经购买过一次");
        }
        // 6.扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")  // set stock = stock - 1
                .eq("voucher_id", voucherId)      // where voucher_id = ? and stock > 0
                .gt("stock", 0)
                .update();
        if (!success) {
            return Result.fail("库存不足");
        }
        // 7.创建订单
        // 订单id
        VoucherOrder voucherOrder = new VoucherOrder();
        Long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        // 用户id
        voucherOrder.setUserId(userId);
        // 代金券id
        voucherOrder.setVoucherId(voucherId);
        save(voucherOrder);
        return Result.ok(orderId);
    }
}
