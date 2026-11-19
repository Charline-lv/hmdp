package com.hmdp;

import cn.hutool.core.util.RandomUtil;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.service.impl.UserServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisIdWoker;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class HmDianPingApplicationTests {

    @Resource
    private ShopServiceImpl shopService;

    @Resource
    private CacheClient cacheClient;

    @Resource
    private RedisIdWoker redisIdWoker;

    @Resource
    private UserMapper userMapper;

    @Resource
    private UserServiceImpl userServiceImpl;

    @Resource
    private StringRedisTemplate stringRedisTemplate;


    /**
     * 为用户表中所有用户批量登录，token 由 login() 自动存入 Redis（login:token:{token}），
     * 并将 token 逐行写入项目根目录下的 tokens.txt 文件
     */
    @Test
    void testLoginAllUsers() throws Exception {
        // 1. 查询所有用户
        List<User> users = userMapper.selectList(null);
        System.out.println("共查询到 " + users.size() + " 个用户");

        // 2. 输出到项目根目录 tokens.txt
        String filePath = Paths.get(System.getProperty("user.dir"), "tokens.txt").toString();

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, StandardCharsets.UTF_8))) {
            int success = 0;
            int fail = 0;

            for (User user : users) {
                String phone = user.getPhone();
                // 3. 生成 6 位验证码，预先写入 Redis（绕过 sendCode 发短信步骤）
                String code = RandomUtil.randomNumbers(6);
                stringRedisTemplate.opsForValue().set(
                        RedisConstants.LOGIN_CODE_KEY + phone,
                        code,
                        RedisConstants.LOGIN_CODE_TTL,
                        TimeUnit.MINUTES
                );

                // 4. 构造登录表单并执行登录
                LoginFormDTO loginForm = new LoginFormDTO();
                loginForm.setPhone(phone);
                loginForm.setCode(code);

                Result result = userServiceImpl.login(loginForm, null);

                // 5. 登录成功则写入 token（login 内部已自动存入 Redis）
                if (result.getSuccess() && result.getData() != null) {
                    writer.write(result.getData().toString());
                    writer.newLine();
                    success++;
                } else {
                    System.err.println("登录失败 user=" + user.getId());
                    fail++;
                }
            }
            System.out.println("登录完成: 成功=" + success + ", 失败=" + fail);
            System.out.println("tokens 已保存至: " + filePath);
        }
    }

    @Test
    void testSaveShop(){
        Shop shop = shopService.getById(1L);
        cacheClient.setWithLogicalExpire(RedisConstants.CACHE_SHOP_KEY + 1L, shop, 10L, TimeUnit.SECONDS);
    }
    /**线程池，用于测试id工作类*/
    private ExecutorService es = Executors.newFixedThreadPool(500);
    @Test
    void testIdWoker() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(300);

        Runnable task = () -> {
            for (int i = 0; i < 100; i++) {
                long id = redisIdWoker.nextId("order");
                System.out.println("id: " + id);
            }
            latch.countDown();
        };
        long begin = System.currentTimeMillis();
        for (int i = 0; i < 300; i++) {
            es.execute(task);
        }
        latch.await();
        long end = System.currentTimeMillis();
        System.out.println("time: " + (end - begin));
    }

}

