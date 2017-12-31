package com.github.pig.common.bean.config;

import com.alibaba.fastjson.JSONObject;
import com.sohu.tv.builder.ClientBuilder;
import com.sohu.tv.builder.RedisStandaloneBuilder;
import com.sohu.tv.cachecloud.client.basic.util.HttpUtils;
import com.sohu.tv.cachecloud.client.basic.util.StringUtil;
import com.sohu.tv.cachecloud.client.jedis.stat.ClientDataCollectReportExecutor;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.CachingConfigurerSupport;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import redis.clients.jedis.JedisPool;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.netflix.config.DeploymentContext.ContextKey.appId;

/**
 * @author lengleng
 * @date 2017-12-31 22:59:37
 * 增强redis
 */
@Configuration
@EnableCaching
public class RedisCacheConfig extends CachingConfigurerSupport {
    private Logger logger = LoggerFactory.getLogger(RedisCacheConfig.class);

    private static final Lock LOCK = new ReentrantLock();
    @Value("${redis.cache.expiration:3600}")
    private Long expiration;
    @Value("${spring.redis.remote:false}")
    private boolean isRemoteRedis;

    @Bean
    public CacheManager cacheManager(RedisTemplate redisTemplate) {
        RedisCacheManager rcm = new RedisCacheManager(redisTemplate);
        rcm.setDefaultExpiration(expiration);
        return rcm;
    }

    @Bean
    public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory factory) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(factory);
        template.setKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(new JdkSerializationRedisSerializer());
        return template;
    }

    /**
     * 根据缓存策略的不同，RedisConnectionFactory不同
     *
     * @return
     */
    @Bean
    public RedisConnectionFactory redisConnectionFactory() {
        if (!isRemoteRedis) {
            return new JedisConnectionFactory();
        } else {
            return buildRemoteCache();
        }
    }

    private RedisConnectionFactory buildRemoteCache() {
        while (true) {
            try {
                LOCK.tryLock(100, TimeUnit.MILLISECONDS);
                /**
                 * 心跳返回的请求为空；
                 */
                String response = HttpUtils.doGet("http://localhost:5005/cache/client/redis/standalone/10000.json?clientVersion=1.0-SNAPSHOT");
                if (response == null || response.isEmpty()) {
                    continue;
                }
                JSONObject jsonObject = null;
                try {
                    jsonObject = JSONObject.parseObject(response);
                } catch (Exception e) {
                }
                if (jsonObject == null) {
                    continue;
                }
                /**
                 * 从心跳中提取HostAndPort，构造JedisPool实例；
                 */
                String instance = jsonObject.getString("standalone");
                String[] instanceArr = instance.split(":");
                if (instanceArr.length != 2) {
                    continue;
                }

                //收集上报数据
                ClientDataCollectReportExecutor.getInstance("http://localhost:5005/cachecloud/client/reportData.json");

                String password = jsonObject.getString("password");
                String host = instanceArr[0];
                String port = instanceArr[1];

                JedisConnectionFactory jedisConnectionFactory = new JedisConnectionFactory();
                jedisConnectionFactory.setPassword(password);
                jedisConnectionFactory.setHostName(host);
                jedisConnectionFactory.setPort(Integer.parseInt(port));
                return jedisConnectionFactory;
            } catch (InterruptedException e) {
                logger.error("error in build().", e);
            }

        }
    }
}
