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

package com.nageoffer.shortlink.project.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.text.StrBuilder;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson2.JSON;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.nageoffer.shortlink.project.common.convention.exception.ClientException;
import com.nageoffer.shortlink.project.common.convention.exception.ServiceException;
import com.nageoffer.shortlink.project.common.enums.VailDateTypeEnum;
import com.nageoffer.shortlink.project.config.GotoDomainWhiteListConfiguration;
import com.nageoffer.shortlink.project.dao.entity.ShortLinkDO;
import com.nageoffer.shortlink.project.dao.entity.ShortLinkGotoDO;
import com.nageoffer.shortlink.project.dao.mapper.ShortLinkGotoMapper;
import com.nageoffer.shortlink.project.dao.mapper.ShortLinkMapper;
import com.nageoffer.shortlink.project.dto.biz.ShortLinkStatsRecordDTO;
import com.nageoffer.shortlink.project.dto.req.ShortLinkBatchCreateReqDTO;
import com.nageoffer.shortlink.project.dto.req.ShortLinkCreateReqDTO;
import com.nageoffer.shortlink.project.dto.req.ShortLinkPageReqDTO;
import com.nageoffer.shortlink.project.dto.req.ShortLinkUpdateReqDTO;
import com.nageoffer.shortlink.project.dto.resp.ShortLinkBaseInfoRespDTO;
import com.nageoffer.shortlink.project.dto.resp.ShortLinkBatchCreateRespDTO;
import com.nageoffer.shortlink.project.dto.resp.ShortLinkCreateRespDTO;
import com.nageoffer.shortlink.project.dto.resp.ShortLinkGroupCountQueryRespDTO;
import com.nageoffer.shortlink.project.dto.resp.ShortLinkPageRespDTO;
import com.nageoffer.shortlink.project.mq.producer.ShortLinkStatsSaveProducer;
import com.nageoffer.shortlink.project.service.ShortLinkService;
import com.nageoffer.shortlink.project.toolkit.HashUtil;
import com.nageoffer.shortlink.project.toolkit.LinkUtil;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RLock;
import org.redisson.api.RReadWriteLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.GOTO_IS_NULL_SHORT_LINK_KEY;
import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.GOTO_SHORT_LINK_KEY;
import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.LOCK_GID_UPDATE_KEY;
import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.LOCK_GOTO_SHORT_LINK_KEY;
import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.SHORT_LINK_CREATE_LOCK_KEY;
import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.SHORT_LINK_STATS_UIP_KEY;
import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.SHORT_LINK_STATS_UV_KEY;

/**
 * 短链接接口实现层
 * 公众号：马丁玩编程，回复：加群，添加马哥微信（备注：link）获取项目资料
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ShortLinkServiceImpl extends ServiceImpl<ShortLinkMapper, ShortLinkDO> implements ShortLinkService {

    private final RBloomFilter<String> shortUriCreateCachePenetrationBloomFilter;
    private final ShortLinkGotoMapper shortLinkGotoMapper;
    private final StringRedisTemplate stringRedisTemplate;
    private final RedissonClient redissonClient;
    private final ShortLinkStatsSaveProducer shortLinkStatsSaveProducer;
    private final GotoDomainWhiteListConfiguration gotoDomainWhiteListConfiguration;

    @Value("${short-link.domain.default}")
    private String createShortLinkDefaultDomain;

    /**
     * 通过事务管理、布隆过滤器、缓存预热等技术，实现了高效、可靠的短链接生成功能。
     * 通过合理的数据库分片和缓存策略，可以有效地解决海量数据存储和高并发访问的问题。
     * @param requestParam 创建短链接请求参数
     * @return
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public ShortLinkCreateRespDTO createShortLink(ShortLinkCreateReqDTO requestParam) {
        // 验证白名单
        verificationWhitelist(requestParam.getOriginUrl());
        // 生成短链接后缀和完整短链接
        String shortLinkSuffix = generateSuffix(requestParam);
        String fullShortUrl = StrBuilder.create(createShortLinkDefaultDomain)
                .append("/")
                .append(shortLinkSuffix)
                .toString();
        // 构建短链接实体对象与跳转对象
        ShortLinkDO shortLinkDO = ShortLinkDO.builder()
                .domain(createShortLinkDefaultDomain)
                .originUrl(requestParam.getOriginUrl())
                .gid(requestParam.getGid())
                .createdType(requestParam.getCreatedType())
                .validDateType(requestParam.getValidDateType())
                .validDate(requestParam.getValidDate())
                .describe(requestParam.getDescribe())
                .shortUri(shortLinkSuffix)
                .enableStatus(0)
                .totalPv(0)
                .totalUv(0)
                .totalUip(0)
                .delTime(0L)
                .fullShortUrl(fullShortUrl)
                .favicon(getFavicon(requestParam.getOriginUrl()))
                .build();
        ShortLinkGotoDO linkGotoDO = ShortLinkGotoDO.builder()
                .fullShortUrl(fullShortUrl)
                .gid(requestParam.getGid())
                .build();
        // 插入数据库并处理重复键异常
        try {
            // 短链接项目有多少数据？如何解决海量数据存储？详情查看：https://nageoffer.com/shortlink/question
            baseMapper.insert(shortLinkDO);
            // 短链接数据库分片键是如何考虑的？详情查看：https://nageoffer.com/shortlink/question
            shortLinkGotoMapper.insert(linkGotoDO);
        } catch (DuplicateKeyException ex) {
            // 首先判断是否存在布隆过滤器，如果不存在直接新增
            if (!shortUriCreateCachePenetrationBloomFilter.contains(fullShortUrl)) {
                shortUriCreateCachePenetrationBloomFilter.add(fullShortUrl);
            }
            throw new ServiceException(String.format("短链接：%s 生成重复", fullShortUrl));
        }
        // 项目中短链接缓存预热是怎么做的？详情查看：https://nageoffer.com/shortlink/question
        // 缓存预热，将生成的短链接和原始URL缓存到Redis中，设置缓存的有效期。
        stringRedisTemplate.opsForValue().set(
                String.format(GOTO_SHORT_LINK_KEY, fullShortUrl),
                requestParam.getOriginUrl(),
                LinkUtil.getLinkCacheValidTime(requestParam.getValidDate()), TimeUnit.MILLISECONDS
        );
        // 删除短链接后，布隆过滤器如何删除
        /**
         * 布隆过滤器不支持删除的原因
         * 布隆过滤器使用多个哈希函数将元素映射到位数组中的多个位置。当你想删除一个元素时，你无法确定这些位置是否只属于这个元素，还是也属于其他元素。如果你将这些位置的位清零，可能会导致其他元素的存在判断出错。
         *
         * 解决方案
         * 虽然布隆过滤器不支持删除操作，但我们可以通过其他方法来处理短链接删除后的问题。以下是几种常见的解决方案：
         *
         * 1. 使用计数布隆过滤器（Counting Bloom Filter）
         * 计数布隆过滤器是布隆过滤器的一种变体，它在位数组的每个位置上使用计数器而不是单个位。这样，当插入一个元素时，增加相应位置的计数器；当删除一个元素时，减少相应位置的计数器。如果计数器减到零，表示该位置不再有任何元素。
         *
         * 优点：
         *
         * 支持删除操作。
         * 缺点：
         *
         * 需要更多的存储空间，因为每个位置需要存储一个计数器。
         * 2. 逻辑删除
         * 在删除短链接时，不实际删除数据库中的记录，而是将其标记为已删除（例如，设置一个 delFlag 字段）。在查询时，忽略已标记为删除的记录。
         *
         * 优点：
         *
         * 简单易实现，不需要修改布隆过滤器。
         * 缺点：
         *
         * 数据库中会保留大量已删除的记录，可能会影响查询性能。
         * 3. 定期重建布隆过滤器
         * 定期重建布隆过滤器，从数据库中重新加载所有有效的短链接数据，构建新的布隆过滤器。
         *
         * 优点：
         *
         * 保持布隆过滤器的准确性。
         * 缺点：
         *
         * 重建过程可能比较耗时，适合在系统负载较低时进行。
         */
        shortUriCreateCachePenetrationBloomFilter.add(fullShortUrl);
        return ShortLinkCreateRespDTO.builder()
                .fullShortUrl("http://" + shortLinkDO.getFullShortUrl())
                .originUrl(requestParam.getOriginUrl())
                .gid(requestParam.getGid())
                .build();
    }

    /**
     * 在这段代码中，分布式锁已经确保了短链接生成过程的唯一性和一致性，因此不需要引入双重检查锁。
     * @param requestParam 创建短链接请求参数
     * @return
     */
    @Override
    public ShortLinkCreateRespDTO createShortLinkByLock(ShortLinkCreateReqDTO requestParam) {
        // 验证白名单
        verificationWhitelist(requestParam.getOriginUrl());
        String fullShortUrl;
        // 为什么说布隆过滤器性能远胜于分布式锁？详情查看：https://nageoffer.com/shortlink/question
        /**
         * 为什么说布隆过滤器性能远胜于分布式锁？
         * 布隆过滤器：布隆过滤器是一种高效的概率型数据结构，用于判断一个元素是否存在于集合中。它的查询和插入操作都非常快速，时间复杂度为O(1)，并且占用空间较小。
         * 分布式锁：分布式锁需要进行网络通信和锁的获取与释放操作，性能相对较低。在高并发场景下，分布式锁可能会成为瓶颈。
         * 对比：布隆过滤器适用于快速判断短链接是否存在，减少对数据库的访问。而分布式锁适用于确保数据的一致性和唯一性，但性能不如布隆过滤器。
         */
        RLock lock = redissonClient.getLock(SHORT_LINK_CREATE_LOCK_KEY);
        lock.lock();
        try {
            // 思考，这里为什么不向查询那边又不需要双重检查呢？
            /**
             * 分布式锁的保护：
             *
             * 分布式锁已经确保了在同一时间只有一个线程能够执行短链接的生成逻辑。获取锁之后，其他线程必须等待锁释放才能继续执行。
             * 这种机制本身就提供了对共享资源的独占访问，确保了操作的原子性和唯一性。
             * 数据库的唯一性约束：
             *
             * 数据库表中通常会对短链接的字段设置唯一性约束（如唯一索引）。即使在极少数情况下，多个线程同时生成了相同的短链接，数据库的唯一性约束也会抛出 DuplicateKeyException 异常。
             * 代码中已经处理了 DuplicateKeyException 异常，确保在发生重复时能够正确处理。
             * 操作的原子性：
             *
             * 在获取锁之后，生成短链接、插入数据库、更新缓存等操作是一个完整的事务，确保了操作的原子性。
             * 这种设计避免了在操作过程中出现不一致的情况。
             * 双重检查的适用场景
             * 双重检查锁定模式通常适用于以下场景：
             *
             * 懒加载单例模式：确保在多线程环境下只创建一个实例，并且只有在第一次需要实例时才进行实例化。
             * 高频率读取、低频率写入的场景：如缓存的读取和更新，减少不必要的同步开销。
             */
            String shortLinkSuffix = generateSuffixByLock(requestParam);
            fullShortUrl = StrBuilder.create(createShortLinkDefaultDomain)
                    .append("/")
                    .append(shortLinkSuffix)
                    .toString();
            ShortLinkDO shortLinkDO = ShortLinkDO.builder()
                    .domain(createShortLinkDefaultDomain)
                    .originUrl(requestParam.getOriginUrl())
                    .gid(requestParam.getGid())
                    .createdType(requestParam.getCreatedType())
                    .validDateType(requestParam.getValidDateType())
                    .validDate(requestParam.getValidDate())
                    .describe(requestParam.getDescribe())
                    .shortUri(shortLinkSuffix)
                    .enableStatus(0)
                    .totalPv(0)
                    .totalUv(0)
                    .totalUip(0)
                    .delTime(0L)
                    .fullShortUrl(fullShortUrl)
                    .favicon(getFavicon(requestParam.getOriginUrl()))
                    .build();
            ShortLinkGotoDO linkGotoDO = ShortLinkGotoDO.builder()
                    .fullShortUrl(fullShortUrl)
                    .gid(requestParam.getGid())
                    .build();
            try {
                baseMapper.insert(shortLinkDO);
                shortLinkGotoMapper.insert(linkGotoDO);
            } catch (DuplicateKeyException ex) {
                throw new ServiceException(String.format("短链接：%s 生成重复", fullShortUrl));
            }
            stringRedisTemplate.opsForValue().set(
                    String.format(GOTO_SHORT_LINK_KEY, fullShortUrl),
                    requestParam.getOriginUrl(),
                    LinkUtil.getLinkCacheValidTime(requestParam.getValidDate()), TimeUnit.MILLISECONDS
            );
        } finally {
            lock.unlock();
        }
        return ShortLinkCreateRespDTO.builder()
                .fullShortUrl("http://" + fullShortUrl)
                .originUrl(requestParam.getOriginUrl())
                .gid(requestParam.getGid())
                .build();
    }

    @Override
    public ShortLinkBatchCreateRespDTO batchCreateShortLink(ShortLinkBatchCreateReqDTO requestParam) {
        List<String> originUrls = requestParam.getOriginUrls();
        List<String> describes = requestParam.getDescribes();
        List<ShortLinkBaseInfoRespDTO> result = new ArrayList<>();
        for (int i = 0; i < originUrls.size(); i++) {
            ShortLinkCreateReqDTO shortLinkCreateReqDTO = BeanUtil.toBean(requestParam, ShortLinkCreateReqDTO.class);
            shortLinkCreateReqDTO.setOriginUrl(originUrls.get(i));
            shortLinkCreateReqDTO.setDescribe(describes.get(i));
            try {
                ShortLinkCreateRespDTO shortLink = createShortLink(shortLinkCreateReqDTO);
                ShortLinkBaseInfoRespDTO linkBaseInfoRespDTO = ShortLinkBaseInfoRespDTO.builder()
                        .fullShortUrl(shortLink.getFullShortUrl())
                        .originUrl(shortLink.getOriginUrl())
                        .describe(describes.get(i))
                        .build();
                result.add(linkBaseInfoRespDTO);
            } catch (Throwable ex) {
                log.error("批量创建短链接失败，原始参数：{}", originUrls.get(i));
            }
        }
        return ShortLinkBatchCreateRespDTO.builder()
                .total(result.size())
                .baseLinkInfos(result)
                .build();
    }

    /**
     * 验证原始URL是否在白名单中。
     * 查询现有短链接记录，如果记录不存在，抛出异常。
     * 更新短链接记录：
     * 如果GID相同，直接更新记录。
     * 如果GID不同，使用分布式锁确保并发安全，标记旧记录为删除，并插入新记录。
     * 缓存一致性处理：根据记录的变化情况，删除对应的缓存键，确保缓存和数据库的一致性。
     * @param requestParam 修改短链接请求参数
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public void updateShortLink(ShortLinkUpdateReqDTO requestParam) {
        verificationWhitelist(requestParam.getOriginUrl());
        LambdaQueryWrapper<ShortLinkDO> queryWrapper = Wrappers.lambdaQuery(ShortLinkDO.class)
                .eq(ShortLinkDO::getGid, requestParam.getOriginGid())
                .eq(ShortLinkDO::getFullShortUrl, requestParam.getFullShortUrl())
                .eq(ShortLinkDO::getDelFlag, 0)
                .eq(ShortLinkDO::getEnableStatus, 0);
        ShortLinkDO hasShortLinkDO = baseMapper.selectOne(queryWrapper);
        if (hasShortLinkDO == null) {
            throw new ClientException("短链接记录不存在");
        }
        // 更新操作： 如果现有记录的GID与请求参数中的GID相同，直接更新短链接记录
        /**
         * 为什么要考虑gid
         * 数据隔离：gid 确保不同分组的数据隔离，避免数据混淆。
         * 权限控制：通过 gid 实现对短链接的权限控制，不同的 gid 可能对应不同的访问权限。
         * 并发安全：在处理 gid 不同的情况下，使用分布式锁确保并发安全，避免数据不一致。
         * 业务逻辑：gid 可能代表不同的业务逻辑分组，通过 gid 可以实现更灵活的业务逻辑处理。
         *
         * 分组管理：gid 用于将短链接分组管理，不同的 gid 可以代表不同的用户、项目或业务逻辑分组。
         * 权限控制：通过 gid 可以实现对短链接的权限控制，不同的 gid 可能对应不同的访问权限。
         * 数据隔离：gid 可以用于数据隔离，确保不同分组的数据不会互相干扰。
         *
         */
        if (Objects.equals(hasShortLinkDO.getGid(), requestParam.getGid())) {
            LambdaUpdateWrapper<ShortLinkDO> updateWrapper = Wrappers.lambdaUpdate(ShortLinkDO.class)
                    .eq(ShortLinkDO::getFullShortUrl, requestParam.getFullShortUrl())
                    .eq(ShortLinkDO::getGid, requestParam.getGid())
                    .eq(ShortLinkDO::getDelFlag, 0)
                    .eq(ShortLinkDO::getEnableStatus, 0)
                    .set(Objects.equals(requestParam.getValidDateType(), VailDateTypeEnum.PERMANENT.getType()), ShortLinkDO::getValidDate, null);
            ShortLinkDO shortLinkDO = ShortLinkDO.builder()
                    .domain(hasShortLinkDO.getDomain())
                    .shortUri(hasShortLinkDO.getShortUri())
                    .favicon(hasShortLinkDO.getFavicon())
                    .createdType(hasShortLinkDO.getCreatedType())
                    .gid(requestParam.getGid())
                    .originUrl(requestParam.getOriginUrl())
                    .describe(requestParam.getDescribe())
                    .validDateType(requestParam.getValidDateType())
                    .validDate(requestParam.getValidDate())
                    .build();
            baseMapper.update(shortLinkDO, updateWrapper);
        } else {
            // 为什么监控表要加上Gid？不加的话是否就不存在读写锁？详情查看：https://nageoffer.com/shortlink/question
            // 分布式写锁
            // gid 与分布式锁的关系
            //锁的粒度：gid 可以帮助我们细化锁的粒度。如果不加 gid，锁的粒度会变得更粗，可能会导致更多的并发冲突。例如，如果所有的短链接更新操作都使用同一个锁，那么即使不同分组的短链接更新操作也会互相阻塞。
            //锁的范围：通过 gid 可以将锁的范围限定在特定的分组内，减少不必要的锁争用，提高系统的并发性能。
            RReadWriteLock readWriteLock = redissonClient.getReadWriteLock(String.format(LOCK_GID_UPDATE_KEY, requestParam.getFullShortUrl()));
            RLock rLock = readWriteLock.writeLock();
            rLock.lock();
            try {
                // 更新旧记录， 将旧记录标记为已经删除
                LambdaUpdateWrapper<ShortLinkDO> linkUpdateWrapper = Wrappers.lambdaUpdate(ShortLinkDO.class)
                        .eq(ShortLinkDO::getFullShortUrl, requestParam.getFullShortUrl())
                        .eq(ShortLinkDO::getGid, hasShortLinkDO.getGid())
                        .eq(ShortLinkDO::getDelFlag, 0)
                        .eq(ShortLinkDO::getDelTime, 0L)
                        .eq(ShortLinkDO::getEnableStatus, 0);
                ShortLinkDO delShortLinkDO = ShortLinkDO.builder()
                        .delTime(System.currentTimeMillis())
                        .build();
                delShortLinkDO.setDelFlag(1);
                baseMapper.update(delShortLinkDO, linkUpdateWrapper);
                // 插入新纪录
                ShortLinkDO shortLinkDO = ShortLinkDO.builder()
                        .domain(createShortLinkDefaultDomain)
                        .originUrl(requestParam.getOriginUrl())
                        .gid(requestParam.getGid())
                        .createdType(hasShortLinkDO.getCreatedType())
                        .validDateType(requestParam.getValidDateType())
                        .validDate(requestParam.getValidDate())
                        .describe(requestParam.getDescribe())
                        .shortUri(hasShortLinkDO.getShortUri())
                        .enableStatus(hasShortLinkDO.getEnableStatus())
                        .totalPv(hasShortLinkDO.getTotalPv())
                        .totalUv(hasShortLinkDO.getTotalUv())
                        .totalUip(hasShortLinkDO.getTotalUip())
                        .fullShortUrl(hasShortLinkDO.getFullShortUrl())
                        .favicon(getFavicon(requestParam.getOriginUrl()))
                        .delTime(0L)
                        .build();
                baseMapper.insert(shortLinkDO);
                // 更新关联表
                LambdaQueryWrapper<ShortLinkGotoDO> linkGotoQueryWrapper = Wrappers.lambdaQuery(ShortLinkGotoDO.class)
                        .eq(ShortLinkGotoDO::getFullShortUrl, requestParam.getFullShortUrl())
                        .eq(ShortLinkGotoDO::getGid, hasShortLinkDO.getGid());
                ShortLinkGotoDO shortLinkGotoDO = shortLinkGotoMapper.selectOne(linkGotoQueryWrapper);
                shortLinkGotoMapper.delete(linkGotoQueryWrapper);
                shortLinkGotoDO.setGid(requestParam.getGid());
                shortLinkGotoMapper.insert(shortLinkGotoDO);
            } finally {
                rLock.unlock();
            }
        }
        // 短链接如何保障缓存和数据库一致性？详情查看：https://nageoffer.com/shortlink/question
        // 删除缓存：如果有效期类型、有效期或原始URL发生变化，删除对应的缓存键。
        if (!Objects.equals(hasShortLinkDO.getValidDateType(), requestParam.getValidDateType())
                || !Objects.equals(hasShortLinkDO.getValidDate(), requestParam.getValidDate())
                || !Objects.equals(hasShortLinkDO.getOriginUrl(), requestParam.getOriginUrl())) {
            stringRedisTemplate.delete(String.format(GOTO_SHORT_LINK_KEY, requestParam.getFullShortUrl()));
            Date currentDate = new Date();
            // 检查有效期：如果旧记录的有效期已过期，并且新记录的有效期类型为永久或有效期在当前时间之后，删除对应的缓存键。
            if (hasShortLinkDO.getValidDate() != null && hasShortLinkDO.getValidDate().before(currentDate)) {
                if (Objects.equals(requestParam.getValidDateType(), VailDateTypeEnum.PERMANENT.getType()) || requestParam.getValidDate().after(currentDate)) {
                    stringRedisTemplate.delete(String.format(GOTO_IS_NULL_SHORT_LINK_KEY, requestParam.getFullShortUrl()));
                }
            }
        }
    }

    @Override
    public IPage<ShortLinkPageRespDTO> pageShortLink(ShortLinkPageReqDTO requestParam) {
        IPage<ShortLinkDO> resultPage = baseMapper.pageLink(requestParam);
        return resultPage.convert(each -> {
            ShortLinkPageRespDTO result = BeanUtil.toBean(each, ShortLinkPageRespDTO.class);
            result.setDomain("http://" + result.getDomain());
            return result;
        });
    }

    @Override
    public List<ShortLinkGroupCountQueryRespDTO> listGroupShortLinkCount(List<String> requestParam) {
        QueryWrapper<ShortLinkDO> queryWrapper = Wrappers.query(new ShortLinkDO())
                .select("gid as gid, count(*) as shortLinkCount")
                .in("gid", requestParam)
                .eq("enable_status", 0)
                .eq("del_flag", 0)
                .eq("del_time", 0L)
                .groupBy("gid");
        List<Map<String, Object>> shortLinkDOList = baseMapper.selectMaps(queryWrapper);
        return BeanUtil.copyToList(shortLinkDOList, ShortLinkGroupCountQueryRespDTO.class);
    }

    @SneakyThrows
    @Override
    public void restoreUrl(String shortUri, ServletRequest request, ServletResponse response) {
        // 短链接接口的并发量有多少？如何测试？详情查看：https://nageoffer.com/shortlink/question
        // 面试中如何回答短链接是如何跳转长链接？详情查看：https://nageoffer.com/shortlink/question
        String serverName = request.getServerName();
        String serverPort = Optional.of(request.getServerPort())
                .filter(each -> !Objects.equals(each, 80))
                .map(String::valueOf)
                .map(each -> ":" + each)
                .orElse("");
        // 完整短链接
        String fullShortUrl = serverName + serverPort + "/" + shortUri;
        // 从redis找对应的长链接
        String originalLink = stringRedisTemplate.opsForValue().get(String.format(GOTO_SHORT_LINK_KEY, fullShortUrl));
        // 如果能找到， 重定向到长链接地址
        if (StrUtil.isNotBlank(originalLink)) {
            // 对完整对短链接进行一些业务操作， 获取统计信息， 丢到mq
            shortLinkStats(buildLinkStatsRecordAndSetUser(fullShortUrl, request, response));
            // 重定向
            ((HttpServletResponse) response).sendRedirect(originalLink);
            return;
        }
        // 处理缓存穿透
        // 1、布隆过滤器判定
        boolean contains = shortUriCreateCachePenetrationBloomFilter.contains(fullShortUrl);
        if (!contains) {
            ((HttpServletResponse) response).sendRedirect("/page/notfound");
            return;
        }
        // 2、检查空值缓存
        String gotoIsNullShortLink = stringRedisTemplate.opsForValue().get(String.format(GOTO_IS_NULL_SHORT_LINK_KEY, fullShortUrl));
        if (StrUtil.isNotBlank(gotoIsNullShortLink)) {
            ((HttpServletResponse) response).sendRedirect("/page/notfound");
            return;
        }
        // 获取分布式锁， 解决并发
        RLock lock = redissonClient.getLock(String.format(LOCK_GOTO_SHORT_LINK_KEY, fullShortUrl));
        lock.lock();
        try {
            /**
             * 在获取分布式锁之后，这段代码的主要目的是再次检查缓存，以确保在获取锁的过程中没有其他线程已经更新了缓存。
             * 这是一种双重检查锁定（Double-Checked Locking）模式，确保在高并发环境下的正确性和效率。
             */
            // 重新从Redis中查找长链接
            // 原因：在获取锁之前，可能有其他线程已经更新了缓存，所以需要重新检查。
            originalLink = stringRedisTemplate.opsForValue().get(String.format(GOTO_SHORT_LINK_KEY, fullShortUrl));
            if (StrUtil.isNotBlank(originalLink)) {
                shortLinkStats(buildLinkStatsRecordAndSetUser(fullShortUrl, request, response));
                ((HttpServletResponse) response).sendRedirect(originalLink);
                return;
            }
            // 再次检查空值缓存
            // 在获取锁之前，可能有其他线程已经更新了空值缓存，所以需要重新检查。
            gotoIsNullShortLink = stringRedisTemplate.opsForValue().get(String.format(GOTO_IS_NULL_SHORT_LINK_KEY, fullShortUrl));
            if (StrUtil.isNotBlank(gotoIsNullShortLink)) {
                ((HttpServletResponse) response).sendRedirect("/page/notfound");
                return;
            }
            //从数据库中查找短链接对应的长链接，并检查其有效性。
            //将长链接缓存到Redis中，记录统计信息并进行重定向。

            // 查短链对应的目标信息
            LambdaQueryWrapper<ShortLinkGotoDO> linkGotoQueryWrapper = Wrappers.lambdaQuery(ShortLinkGotoDO.class)
                    .eq(ShortLinkGotoDO::getFullShortUrl, fullShortUrl);
            // 短链接跳转实体
            ShortLinkGotoDO shortLinkGotoDO = shortLinkGotoMapper.selectOne(linkGotoQueryWrapper);
            // 如果查询结果为空，缓存空值并返回404页面
            if (shortLinkGotoDO == null) {
                stringRedisTemplate.opsForValue().set(String.format(GOTO_IS_NULL_SHORT_LINK_KEY, fullShortUrl), "-", 30, TimeUnit.MINUTES);
                ((HttpServletResponse) response).sendRedirect("/page/notfound");
                return;
            }
            // 从数据库中查询短链接的详细信息
            LambdaQueryWrapper<ShortLinkDO> queryWrapper = Wrappers.lambdaQuery(ShortLinkDO.class)
                    .eq(ShortLinkDO::getGid, shortLinkGotoDO.getGid())
                    .eq(ShortLinkDO::getFullShortUrl, fullShortUrl)
                    .eq(ShortLinkDO::getDelFlag, 0)
                    .eq(ShortLinkDO::getEnableStatus, 0);
            // 短链接实体
            ShortLinkDO shortLinkDO = baseMapper.selectOne(queryWrapper);
            // 如果查询结果为空或链接已失效或者已经失效，缓存空值并返回404页面
            if (shortLinkDO == null || (shortLinkDO.getValidDate() != null && shortLinkDO.getValidDate().before(new Date()))) {
                stringRedisTemplate.opsForValue().set(String.format(GOTO_IS_NULL_SHORT_LINK_KEY, fullShortUrl), "-", 30, TimeUnit.MINUTES);
                ((HttpServletResponse) response).sendRedirect("/page/notfound");
                return;
            }
            // 重定向原始链接
            stringRedisTemplate.opsForValue().set(
                    String.format(GOTO_SHORT_LINK_KEY, fullShortUrl),
                    shortLinkDO.getOriginUrl(),
                    LinkUtil.getLinkCacheValidTime(shortLinkDO.getValidDate()), TimeUnit.MILLISECONDS
            );
            shortLinkStats(buildLinkStatsRecordAndSetUser(fullShortUrl, request, response));
            ((HttpServletResponse) response).sendRedirect(shortLinkDO.getOriginUrl());
        } finally {
            lock.unlock();
        }
    }

    /**
     * 返回短链接的统计信息
     * @param fullShortUrl
     * @param request
     * @param response
     * @return
     */
    private ShortLinkStatsRecordDTO buildLinkStatsRecordAndSetUser(String fullShortUrl, ServletRequest request, ServletResponse response) {
        // 标记是否是第一次访问uv
        AtomicBoolean uvFirstFlag = new AtomicBoolean();
        // 存请求的所有cookies
        Cookie[] cookies = ((HttpServletRequest) request).getCookies();
        // 存uv
        AtomicReference<String> uv = new AtomicReference<>();
        // 定时任务， 将新生产uv存到响应的cookie中， 更新redis的uv统计
        Runnable addResponseCookieTask = () -> {
            uv.set(UUID.fastUUID().toString());
            Cookie uvCookie = new Cookie("uv", uv.get());
            uvCookie.setMaxAge(60 * 60 * 24 * 30);
            uvCookie.setPath(StrUtil.sub(fullShortUrl, fullShortUrl.indexOf("/"), fullShortUrl.length()));
            ((HttpServletResponse) response).addCookie(uvCookie);
            uvFirstFlag.set(Boolean.TRUE);
            stringRedisTemplate.opsForSet().add(SHORT_LINK_STATS_UV_KEY + fullShortUrl, uv.get());
        };
        // 处理请求的cookies
        if (ArrayUtil.isNotEmpty(cookies)) {
            // 是否存在“uv”的cookie， 获取值， 存入redis，并更新uvFirstFlag
            Arrays.stream(cookies)
                    .filter(each -> Objects.equals(each.getName(), "uv"))
                    .findFirst()
                    .map(Cookie::getValue)
                    .ifPresentOrElse(each -> {
                        uv.set(each);
                        Long uvAdded = stringRedisTemplate.opsForSet().add(SHORT_LINK_STATS_UV_KEY + fullShortUrl, each);
                        uvFirstFlag.set(uvAdded != null && uvAdded > 0L);
                    }, addResponseCookieTask);
        } else {
            // 如果不存在，继续执行任务
            addResponseCookieTask.run();
        }
        // 获取用户信息
        String remoteAddr = LinkUtil.getActualIp(((HttpServletRequest) request));
        String os = LinkUtil.getOs(((HttpServletRequest) request));
        String browser = LinkUtil.getBrowser(((HttpServletRequest) request));
        String device = LinkUtil.getDevice(((HttpServletRequest) request));
        String network = LinkUtil.getNetwork(((HttpServletRequest) request));
        // 将用户的ip地址添加到redis集合中
        Long uipAdded = stringRedisTemplate.opsForSet().add(SHORT_LINK_STATS_UIP_KEY + fullShortUrl, remoteAddr);
        boolean uipFirstFlag = uipAdded != null && uipAdded > 0L;
        return ShortLinkStatsRecordDTO.builder()
                .fullShortUrl(fullShortUrl)
                .uv(uv.get())
                .uvFirstFlag(uvFirstFlag.get())
                .uipFirstFlag(uipFirstFlag)
                .remoteAddr(remoteAddr)
                .os(os)
                .browser(browser)
                .device(device)
                .network(network)
                .currentDate(new Date())
                .build();
    }

    /**
     * 将短链接记录丢到mq中
     * @param statsRecord 短链接统计实体参数
     */
    @Override
    public void shortLinkStats(ShortLinkStatsRecordDTO statsRecord) {
        Map<String, String> producerMap = new HashMap<>();
        producerMap.put("statsRecord", JSON.toJSONString(statsRecord));
        // 消息队列为什么选用RocketMQ？详情查看：https://nageoffer.com/shortlink/question
        shortLinkStatsSaveProducer.send(producerMap);
    }

    /**
     * 两种方案：
     * 1. 布隆过滤器
     * 布隆过滤器有一定的误判率，即可能会误判一个不存在的短链接为存在。
     * 不支持删除操作：
     *
     * 布隆过滤器不支持删除操作，删除一个元素可能会影响其他元素的判断。
     * 适合只需要插入和查询的场景，不适合需要频繁删除的场景。
     * 2. 数据库唯一索引
     */


    /**
     * 生成一个短链接的后缀（shortUri），并确保生成的短链接是唯一的。
     * 代码中使用了一个布隆过滤器来检查生成的短链接是否已经存在，并且在生成过程中使用了一个循环来处理可能的哈希冲突
     * @param requestParam
     * @return
     */
    private String generateSuffix(ShortLinkCreateReqDTO requestParam) {
        int customGenerateCount = 0;
        String shorUri;
        while (true) {
            if (customGenerateCount > 10) {
                throw new ServiceException("短链接频繁生成，请稍后再试");
            }
            // 生成哈希值：将原始URL加上一个随机UUID，使用 HashUtil.hashToBase62 方法生成一个Base62编码的哈希值作为短链接后缀
            String originUrl = requestParam.getOriginUrl();
            originUrl += UUID.randomUUID().toString();
            // 短链接哈希算法生成冲突问题如何解决？
            /**
             * 1. 短链接哈希算法生成冲突问题如何解决？
             * 哈希冲突：哈希算法生成的短链接可能会发生冲突，即不同的原始URL生成相同的短链接。
             * 解决方法：通过在原始URL后面加上一个随机的UUID，增加哈希输入的随机性，从而减少冲突的概率。如果发生冲突，通过循环和布隆过滤器检查，确保生成的短链接是唯一的。
             */
            shorUri = HashUtil.hashToBase62(originUrl);
            // 判断短链接是否存在为什么不使用Set结构？
            // 2. 判断短链接是否存在为什么不使用Set结构？
            /**
             * Set结构：Set结构可以用来存储唯一的短链接，但在大规模数据场景下，Set的内存占用可能会非常大。
             * 布隆过滤器：布隆过滤器是一种空间效率高的概率型数据结构，可以用来快速判断一个元素是否在集合中。虽然布隆过滤器有一定的误判率，但它的空间效率和查询速度非常适合大规模数据场景。
             * 选择布隆过滤器的原因：布隆过滤器相比Set结构更节省内存，适合大规模短链接的存储和查询。
             */

            // 如果布隆过滤器挂了，里边存的数据全丢失了，怎么恢复呢？
            /**
             * 3. 如果布隆过滤器挂了，里边存的数据全丢失了，怎么恢复呢？
             * 布隆过滤器挂了：如果布隆过滤器挂了，存储在其中的数据会丢失，导致无法判断短链接是否存在。
             * 恢复方法：
             * 持久化存储：可以将布隆过滤器的数据定期持久化到磁盘或数据库中，定期备份。
             * 重建布隆过滤器：从持久化存储中恢复数据，重建布隆过滤器。
             * 双重检查：在布隆过滤器失效的情况下，可以通过数据库或其他持久化存储进行二次检查，确保短链接的唯一性。
             *
             * 进一步的解决方案
             * 1. 持久化布隆过滤器
             * 定期备份：定期将布隆过滤器的数据持久化到磁盘或数据库中，确保在布隆过滤器挂掉时可以快速恢复。
             * 恢复机制：在系统启动时，从持久化存储中加载布隆过滤器的数据，重建布隆过滤器。
             * 2. 双重检查机制
             * 布隆过滤器+数据库：在布隆过滤器检查的基础上，增加数据库的二次检查。即使布隆过滤器失效，也可以通过数据库确保短链接的唯一性。
             * 缓存+持久化存储：使用缓存（如Redis）和持久化存储（如MySQL）结合，确保数据的高可用性和一致性。
             * 3. 分布式锁
             * 分布式锁：在生成短链接时使用分布式锁，确保在高并发场景下的唯一性和一致性。
             * 锁的粒度：通过 gid 等字段细化锁的粒度，减少锁争用，提高系统性能。
             */
            if (!shortUriCreateCachePenetrationBloomFilter.contains(createShortLinkDefaultDomain + "/" + shorUri)) {
                break;
            }
            customGenerateCount++;
        }
        return shorUri;
    }

    private String generateSuffixByLock(ShortLinkCreateReqDTO requestParam) {
        int customGenerateCount = 0;
        String shorUri;
        while (true) {
            if (customGenerateCount > 10) {
                throw new ServiceException("短链接频繁生成，请稍后再试");
            }
            String originUrl = requestParam.getOriginUrl();
            originUrl += UUID.randomUUID().toString();
            shorUri = HashUtil.hashToBase62(originUrl);
            /**
             * 使用数据库的唯一索引去解决问题
             */
            LambdaQueryWrapper<ShortLinkDO> queryWrapper = Wrappers.lambdaQuery(ShortLinkDO.class)
                    .eq(ShortLinkDO::getGid, requestParam.getGid())
                    .eq(ShortLinkDO::getFullShortUrl, createShortLinkDefaultDomain + "/" + shorUri)
                    .eq(ShortLinkDO::getDelFlag, 0);
            ShortLinkDO shortLinkDO = baseMapper.selectOne(queryWrapper);
            if (shortLinkDO == null) {
                break;
            }
            customGenerateCount++;
        }
        return shorUri;
    }

    @SneakyThrows
    private String getFavicon(String url) {
        URL targetUrl = new URL(url);
        HttpURLConnection connection = (HttpURLConnection) targetUrl.openConnection();
        connection.setRequestMethod("GET");
        connection.connect();
        int responseCode = connection.getResponseCode();
        if (HttpURLConnection.HTTP_OK == responseCode) {
            Document document = Jsoup.connect(url).get();
            Element faviconLink = document.select("link[rel~=(?i)^(shortcut )?icon]").first();
            if (faviconLink != null) {
                return faviconLink.attr("abs:href");
            }
        }
        return null;
    }

    /**
     * 验证白名单， 不在白名单的直接抛异常
     * @param originUrl
     */
    private void verificationWhitelist(String originUrl) {
        Boolean enable = gotoDomainWhiteListConfiguration.getEnable();
        if (enable == null || !enable) {
            return;
        }
        String domain = LinkUtil.extractDomain(originUrl);
        if (StrUtil.isBlank(domain)) {
            throw new ClientException("跳转链接填写错误");
        }
        List<String> details = gotoDomainWhiteListConfiguration.getDetails();
        if (!details.contains(domain)) {
            throw new ClientException("演示环境为避免恶意攻击，请生成以下网站跳转链接：" + gotoDomainWhiteListConfiguration.getNames());
        }
    }
}
