package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
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
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }
    @Autowired
    private IVoucherOrderService voucherOrderService;
    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;


    // 在类初始化之后
    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }


    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024*1024);
    private class VoucherOrderHandler implements Runnable {
        @Override
        public void run() {
            while(true){
                try {
                    // 1.获取队列中的订单信息
                    VoucherOrder voucherOrder = orderTasks.take();
                    // 2.创建订单
                    handleVoucherOrder(voucherOrder);
                } catch (InterruptedException e) {
                    log.error("处理订单异常",e);
                }
            }
        }
    }

//    String queueName = "stream.orders";
//    private class VoucherOrderHandler implements Runnable {
//        @Override
//        public void run() {
//            while(true){
//                try {
//                    // 1.获取redis消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.orders >
//                    List<MapRecord<String,Object,Object>> list = stringRedisTemplate.opsForStream().read(
//                            Consumer.from("g1","c1"),
//                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
//                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
//                    );
//                    // 2.判断消息获取是否成功
//                    if(list == null || list.isEmpty()){
//                        // 2.1.如果获取失败，说明没有消息，继续下一次循环
//                        continue;
//                    }
//                    // 3.解析消息中的订单
//                    MapRecord<String,Object,Object> record = list.get(0);
//                    Map<Object,Object> values = record.getValue();
//                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values,new VoucherOrder(),true);
//                    // 4.如果获取成功，可以下单
//                    handleVoucherOrder(voucherOrder);
//                    // 5.ACK确认
//                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
//                }
//                catch (Exception e) {
//                    log.error("处理消息队列异常",e);
//                    handlePendingList();
//                }
//            }
//        }
//    }

//    private void handlePendingList() {
//        while (true) {
//            try {
//                // 1.获取redis pending-list中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 STREAMS streams.order 0
//                List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
//                        Consumer.from("g1", "c1"),
//                        StreamReadOptions.empty().count(1),
//                        StreamOffset.create(queueName, ReadOffset.from("0"))
//                );
//                // 2.判断消息获取是否成功
//                if (list == null || list.isEmpty()) {
//                    // 2.1.如果获取失败，说明pending-list没有消息，继续下一次循环
//                    break;
//                }
//                // 3.解析消息中的订单
//                MapRecord<String, Object, Object> record = list.get(0);
//                Map<Object, Object> values = record.getValue();
//                VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
//                // 4.如果获取成功，可以下单
//                handleVoucherOrder(voucherOrder);
//                // 5.ACK确认
//                stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
//            }
//            catch (Exception e) {
//                log.error("处理pending-list异常",e);
//                try {
//                    Thread.sleep(20);
//                } catch (InterruptedException ex) {
//                    throw new RuntimeException(ex);
//                }
//            }
//        }
//    }


    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        // 在事物之外添加锁，保证事物提交
        // 创建锁对象
        RLock lock = redissonClient.getLock("lock:order:"+userId);
        // 获取锁
        boolean isLock = lock.tryLock();
        if(!isLock){
            // 获取锁失败
            log.error("不允许重复下单");
            return;
        }
        try {
            // createVoucherOrder 方法拥有事务的能力是因为 spring aop 生成代理了对象，但是这种方法直接调用了 this 对象的方法，所以 createVoucherOrder 方法不会生成事务。
            //3.3 通过 AopContent 类
            //在该 Service 类中使用 AopContext.currentProxy() 获取代理对象。
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            // 或者通过注入自己的方式
            voucherOrderService.createVoucherOrder(voucherOrder);
        }finally {
            lock.unlock();
        }
    }

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        // 5.一人一单
        Long voucherId = voucherOrder.getVoucherId();
        Long userId = voucherOrder.getUserId();

        // intern返回一个字符串的规范表示，会先去字符串常量池中查询
        int count = query().eq("voucher_id", voucherId).eq("user_id", userId).count();
        if (count > 0) {
            log.error("用户已经购买过一次");
            return;
        }
        // 6.扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")  // set stock = stock - 1
                .eq("voucher_id", voucherId)      // where voucher_id = ? and stock > 0
                .gt("stock", 0)
                .update();
        if (!success) {
            log.error("库存不足");
        }
        // 7.插入订单
        save(voucherOrder);
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        long orderId = redisIdWorker.nextId("order");
        Long userId = UserHolder.getUser().getId();
        // 1.执行lua脚本(判断用户是否有购买资格)
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId)
        );

        // 2.判断结果是否为0
        int r = result.intValue();
        if(r != 0){
            // 2.1不为0，代表没有购买资格
            return Result.fail(r == 1?"库存不足":"不能重复下单");
        }
        return Result.ok(orderId);
    }


//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        long orderId = redisIdWorker.nextId("order");
//        Long userId = UserHolder.getUser().getId();
//        // 1.执行lua脚本(判断用户是否有购买资格)
//        Long result = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(), userId.toString(), String.valueOf(orderId)
//        );
//
//        // 2.判断结果是否为0
//        int r = result.intValue();
//        if(r != 0){
//            // 2.1不为0，代表没有购买资格
//            return Result.fail(r == 1?"库存不足":"不能重复下单");
//        }
//        // 2.2为0，有购买资格，把下单信息保存到阻塞队列
//        // 订单id
//        VoucherOrder voucherOrder = new VoucherOrder();
//        voucherOrder.setId(orderId);
//        // 用户id
//        voucherOrder.setUserId(userId);
//        // 代金券id
//        voucherOrder.setVoucherId(voucherId);
//        orderTasks.add(voucherOrder);
//
//        return Result.ok(orderId);
//    }



    //    @Override
//    public Result seckillVoucher(Long voucherId) {
//        // 1.查询优惠券
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//
//        // 2.判断秒杀是否开始
//        if(voucher.getBeginTime().isAfter(LocalDateTime.now())){
//            return Result.fail("秒杀还没开始");
//        }
//        // 3. 判断秒杀是否结束
//        if(voucher.getEndTime().isBefore(LocalDateTime.now())){
//            return Result.fail("秒杀已经结束");
//        }
//        // 4.判断库存是否充足
//        if(voucher.getStock() <1){
//            return Result.fail("库存不足");
//        }
//
//        Long userId = UserHolder.getUser().getId();
//        // 在事物之外添加锁，保证事物提交
//
//        // 创建锁对象
//        SimpleRedisLock lock = new SimpleRedisLock("order:"+userId,stringRedisTemplate);
////        RLock lock = redissonClient.getLock("lock:order:"+userId);
//
//        // 获取锁
//        boolean isLock = lock.tryLock(1200);
////        boolean isLock = lock.tryLock();
//        if(!isLock){
//            // 获取锁失败
//            return Result.fail("不允许重复下单");
//        }
//        try {
//            // createVoucherOrder 方法拥有事务的能力是因为 spring aop 生成代理了对象，但是这种方法直接调用了 this 对象的方法，所以 createVoucherOrder 方法不会生成事务。
//            //3.3 通过 AopContent 类
//            //在该 Service 类中使用 AopContext.currentProxy() 获取代理对象。
////            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            // 或者通过注入自己的方式
//            return voucherOrderService.createVoucherOrder(voucherId);
//        }finally {
//            lock.unlock();
//        }
//    }
//    @Transactional
//    public Result createVoucherOrder(Long voucherId) {
//        // 5.一人一单
//        Long userId = UserHolder.getUser().getId();
//
//        // 创建锁对象
//        RLock redisLock = redissonClient.getLock("lock:order:" + userId);
//        // 尝试获取锁
//        boolean isLock = redisLock.tryLock();
//        // 判断
//        if(!isLock){
//            // 获取锁失败，直接返回失败或者重试
//            return Result.fail("不允许重复下单！");
//        }
//
//        try {
//            // 5.1.查询订单
//            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
//            // 5.2.判断是否存在
//            if (count > 0) {
//                // 用户已经购买过了
//                return Result.fail("用户已经购买过一次！");
//            }
//
//            // 6.扣减库存
//            boolean success = seckillVoucherService.update()
//                    .setSql("stock = stock - 1") // set stock = stock - 1
//                    .eq("voucher_id", voucherId).gt("stock", 0) // where id = ? and stock > 0
//                    .update();
//            if (!success) {
//                // 扣减失败
//                return Result.fail("库存不足！");
//            }
//
//            // 7.创建订单
//            VoucherOrder voucherOrder = new VoucherOrder();
//            // 7.1.订单id
//            long orderId = redisIdWorker.nextId("order");
//            voucherOrder.setId(orderId);
//            // 7.2.用户id
//            voucherOrder.setUserId(userId);
//            // 7.3.代金券id
//            voucherOrder.setVoucherId(voucherId);
//            save(voucherOrder);
//
//            // 7.返回订单id
//            return Result.ok(orderId);
//        } finally {
//            // 释放锁
//            redisLock.unlock();
//        }
//
//    }

}
