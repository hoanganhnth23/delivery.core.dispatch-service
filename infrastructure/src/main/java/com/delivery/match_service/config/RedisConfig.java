package com.delivery.match_service.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import lombok.extern.slf4j.Slf4j;

/**
 * ✅ Redis configuration cho Match Service
 * Dùng Redis để lưu cancellation flag theo deliveryId.
 *
 * Dựa theo RedisConfig cũ của delivery-service để đồng bộ cách serialize.
 */
@Configuration
@Slf4j
public class RedisConfig {

    @Bean
    public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory connectionFactory) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);

        template.setKeySerializer(new StringRedisSerializer());
        template.setHashKeySerializer(new StringRedisSerializer());

        template.setValueSerializer(new GenericJackson2JsonRedisSerializer());
        template.setHashValueSerializer(new GenericJackson2JsonRedisSerializer());

        template.afterPropertiesSet();
        return template;
    }

    /**
     * Optional: container cho keyspace notifications.
     * (Match service hiện chỉ cần RedisTemplate để check cancel flag; container giữ style đồng nhất.)
     */
    @Bean
    public RedisMessageListenerContainer redisMessageListenerContainer(RedisConnectionFactory connectionFactory) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);

        try {
            connectionFactory.getConnection().serverCommands().setConfig("notify-keyspace-events", "Ex");
        } catch (Exception e) {
            log.warn("Could not set Redis notify-keyspace-events=Ex; configure it on the Redis server", e);
        }

        return container;
    }
}
