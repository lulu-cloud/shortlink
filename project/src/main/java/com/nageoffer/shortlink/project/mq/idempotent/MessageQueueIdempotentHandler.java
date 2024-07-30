/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nageoffer.shortlink.project.mq.idempotent;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * 消息队列幂等处理器

 */
@Component
@RequiredArgsConstructor
public class MessageQueueIdempotentHandler {

    // @RequiredArgsConstructor 它会自动生成一个包含所有 final 字段的构造函数。因此这里是构造器注入
    private final StringRedisTemplate stringRedisTemplate;

    private static final String IDEMPOTENT_KEY_PREFIX = "short-link:idempotent:";

    /**
     * 判断当前消息是否消费过
     *
     * isMessageBeingConsumed：判断当前消息是否已经被消费过。
     * 参数：messageId 消息的唯一标识。
     * 逻辑：使用 setIfAbsent 方法尝试设置一个键，如果键不存在则设置成功并返回 true，否则返回 false。这里设置的值为 "0"，过期时间为 2 分钟。
     * 返回值：如果返回 false，表示消息已经被消费过；否则表示消息未被消费。
     *
     * @param messageId 消息唯一标识
     * @return 消息是否消费过
     */
    public boolean isMessageBeingConsumed(String messageId) {
        String key = IDEMPOTENT_KEY_PREFIX + messageId;
        return Boolean.FALSE.equals(stringRedisTemplate.opsForValue().setIfAbsent(key, "0", 2, TimeUnit.MINUTES));
    }

    /**
     * 判断消息消费流程是否执行完成
     *
     * 参数：messageId 消息的唯一标识。
     * 逻辑：获取键的值并判断是否等于 "1"。
     * 返回值：如果值为 "1"，表示消息处理完成；否则表示未完成。
     *
     * @param messageId 消息唯一标识
     * @return 消息是否执行完成
     */
    public boolean isAccomplish(String messageId) {
        String key = IDEMPOTENT_KEY_PREFIX + messageId;
        return Objects.equals(stringRedisTemplate.opsForValue().get(key), "1");
    }

    /**
     * 设置消息流程执行完成
     *
     * @param messageId 消息唯一标识
     */
    public void setAccomplish(String messageId) {
        String key = IDEMPOTENT_KEY_PREFIX + messageId;
        stringRedisTemplate.opsForValue().set(key, "1", 2, TimeUnit.MINUTES);
    }

    /**
     * 如果消息处理遇到异常情况，删除幂等标识
     *
     * @param messageId 消息唯一标识
     */
    public void delMessageProcessed(String messageId) {
        String key = IDEMPOTENT_KEY_PREFIX + messageId;
        stringRedisTemplate.delete(key);
    }
}
