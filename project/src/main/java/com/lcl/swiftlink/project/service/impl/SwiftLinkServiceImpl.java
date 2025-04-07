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

package com.lcl.swiftlink.project.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.date.DateUnit;
import cn.hutool.core.date.DateUtil;
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
import com.google.common.collect.Lists;
import com.lcl.swiftlink.project.common.convention.exception.ClientException;
import com.lcl.swiftlink.project.common.convention.exception.ServiceException;
import com.lcl.swiftlink.project.common.convention.result.Results;
import com.lcl.swiftlink.project.common.enums.VailDateTypeEnum;
import com.lcl.swiftlink.project.config.GotoDomainWhiteListConfiguration;
import com.lcl.swiftlink.project.dao.entity.ShortLinkDO;
import com.lcl.swiftlink.project.dao.entity.ShortLinkGotoDO;
import com.lcl.swiftlink.project.dao.entity.SwiftLinkDO;
import com.lcl.swiftlink.project.dao.entity.SwiftLinkJumpDO;
import com.lcl.swiftlink.project.dao.mapper.ShortLinkGotoMapper;
import com.lcl.swiftlink.project.dao.mapper.ShortLinkMapper;
import com.lcl.swiftlink.project.dao.mapper.SwiftLinkJumpMapper;
import com.lcl.swiftlink.project.dto.biz.ShortLinkStatsRecordDTO;
import com.lcl.swiftlink.project.dto.biz.SwiftLinkVisitDTO;
import com.lcl.swiftlink.project.dto.req.ShortLinkBatchCreateReqDTO;
import com.lcl.swiftlink.project.dto.req.ShortLinkCreateReqDTO;
import com.lcl.swiftlink.project.dto.req.ShortLinkPageReqDTO;
import com.lcl.swiftlink.project.dto.req.ShortLinkUpdateReqDTO;
import com.lcl.swiftlink.project.dto.resp.*;
import com.lcl.swiftlink.project.mq.producer.ShortLinkStatsSaveProducer;
import com.lcl.swiftlink.project.service.ShortLinkService;
import com.lcl.swiftlink.project.toolkit.HashUtil;
import com.lcl.swiftlink.project.toolkit.LinkUtil;
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
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.lcl.swiftlink.project.common.constant.RedisKeyConstant.*;

/**
 * 短链接接口实现层
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SwiftLinkServiceImpl extends ServiceImpl<ShortLinkMapper, ShortLinkDO> implements ShortLinkService {

    private static final String VISIT_DATA_SET_LUA_SCRIPT_PATH = "lua/visit_data_set.lua";
    private final RBloomFilter<String> shortUriCreateCachePenetrationBloomFilter;
    private final RBloomFilter<String> swiftUrlRedisBloomFilter;
    private final ShortLinkGotoMapper shortLinkGotoMapper;
    private final SwiftLinkJumpMapper swiftLinkJumpMapper;
    private final StringRedisTemplate stringRedisTemplate;
    private final StringRedisTemplate swiftUrlStringRedisTemplate;
    private final RedissonClient redissonClient;
    private final RedissonClient swiftLinkRedissonClient;
    private final ShortLinkStatsSaveProducer shortLinkStatsSaveProducer;
    private final GotoDomainWhiteListConfiguration gotoDomainWhiteListConfiguration;

    @Value("${short-link.domain.default}")
    private String createShortLinkDefaultDomain;

    @Transactional(rollbackFor = Exception.class)
    @Override
    public ShortLinkCreateRespDTO createShortLink(ShortLinkCreateReqDTO requestParam) {
        verificationWhitelist(requestParam.getOriginUrl());
        String shortLinkSuffix = generateSuffix(requestParam);
        String fullShortUrl = StrBuilder.create(createShortLinkDefaultDomain)
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
//                .favicon(null)
                .build();
        ShortLinkGotoDO linkGotoDO = ShortLinkGotoDO.builder()
                .fullShortUrl(fullShortUrl)
                .gid(requestParam.getGid())
                .build();
        try {
            baseMapper.insert(shortLinkDO);
            shortLinkGotoMapper.insert(linkGotoDO);
        } catch (DuplicateKeyException ex) {
            // 首先判断是否存在布隆过滤器，如果不存在直接新增
            if (!shortUriCreateCachePenetrationBloomFilter.contains(fullShortUrl)) {
                shortUriCreateCachePenetrationBloomFilter.add(fullShortUrl);
            }
            throw new ServiceException(String.format("短链接：%s 生成重复", fullShortUrl));
        }
        stringRedisTemplate.opsForValue().set(
                String.format(GOTO_SHORT_LINK_KEY, fullShortUrl),
                requestParam.getOriginUrl(),
                LinkUtil.getLinkCacheValidTime(requestParam.getValidDate()), TimeUnit.MILLISECONDS
        );
        shortUriCreateCachePenetrationBloomFilter.add(fullShortUrl);
        return ShortLinkCreateRespDTO.builder()
                .fullShortUrl("http://" + shortLinkDO.getFullShortUrl())
                .originUrl(requestParam.getOriginUrl())
                .gid(requestParam.getGid())
                .build();
    }

    @Override
    public ShortLinkCreateRespDTO createShortLinkByLock(ShortLinkCreateReqDTO requestParam) {
        verificationWhitelist(requestParam.getOriginUrl());
        String fullShortUrl;
        RLock lock = redissonClient.getLock(SHORT_LINK_CREATE_LOCK_KEY);
        lock.lock();
        try {
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
//                    .fullShortUrl(fullShortUrl)
                    .favicon(getFavicon(requestParam.getOriginUrl()))
                    .favicon(null)
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
                    .favicon(Objects.equals(requestParam.getOriginUrl(), hasShortLinkDO.getOriginUrl()) ? hasShortLinkDO.getFavicon() : getFavicon(requestParam.getOriginUrl()))
                    .createdType(hasShortLinkDO.getCreatedType())
                    .gid(requestParam.getGid())
                    .originUrl(requestParam.getOriginUrl())
                    .describe(requestParam.getDescribe())
                    .validDateType(requestParam.getValidDateType())
                    .validDate(requestParam.getValidDate())
                    .build();
            baseMapper.update(shortLinkDO, updateWrapper);
        } else {
            RReadWriteLock readWriteLock = redissonClient.getReadWriteLock(String.format(LOCK_GID_UPDATE_KEY, requestParam.getFullShortUrl()));
            RLock rLock = readWriteLock.writeLock();
            rLock.lock();
            try {
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
                        .favicon(Objects.equals(requestParam.getOriginUrl(), hasShortLinkDO.getOriginUrl()) ? hasShortLinkDO.getFavicon() : getFavicon(requestParam.getOriginUrl()))
                        .delTime(0L)
                        .build();
                baseMapper.insert(shortLinkDO);
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
        if (!Objects.equals(hasShortLinkDO.getValidDateType(), requestParam.getValidDateType())
                || !Objects.equals(hasShortLinkDO.getValidDate(), requestParam.getValidDate())
                || !Objects.equals(hasShortLinkDO.getOriginUrl(), requestParam.getOriginUrl())) {
            stringRedisTemplate.delete(String.format(GOTO_SHORT_LINK_KEY, requestParam.getFullShortUrl()));
            Date currentDate = new Date();
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
        String serverName = request.getServerName();
        String serverPort = Optional.of(request.getServerPort())
                .filter(each -> !Objects.equals(each, 80))
                .map(String::valueOf)
                .map(each -> ":" + each)
                .orElse("");
        String fullShortUrl = serverName + serverPort + "/" + shortUri;
        String originalLink = stringRedisTemplate.opsForValue().get(String.format(GOTO_SHORT_LINK_KEY, fullShortUrl));
        if (StrUtil.isNotBlank(originalLink)) {
            shortLinkStats(buildLinkStatsRecordAndSetUser(fullShortUrl, request, response));
             ((HttpServletResponse) response).sendRedirect(originalLink);
            return;
        }
        boolean contains = shortUriCreateCachePenetrationBloomFilter.contains(fullShortUrl);
        if (!contains) {
            ((HttpServletResponse) response).sendRedirect("/page/notfound");
            return;
        }
        String gotoIsNullShortLink = stringRedisTemplate.opsForValue().get(String.format(GOTO_IS_NULL_SHORT_LINK_KEY, fullShortUrl));
        if (StrUtil.isNotBlank(gotoIsNullShortLink)) {
            ((HttpServletResponse) response).sendRedirect("/page/notfound");
            return;
        }
        RLock lock = redissonClient.getLock(String.format(LOCK_GOTO_SHORT_LINK_KEY, fullShortUrl));
        lock.lock();
        try {
            originalLink = stringRedisTemplate.opsForValue().get(String.format(GOTO_SHORT_LINK_KEY, fullShortUrl));
            if (StrUtil.isNotBlank(originalLink)) {
                shortLinkStats(buildLinkStatsRecordAndSetUser(fullShortUrl, request, response));
                ((HttpServletResponse) response).sendRedirect(originalLink);
                return;
            }
            gotoIsNullShortLink = stringRedisTemplate.opsForValue().get(String.format(GOTO_IS_NULL_SHORT_LINK_KEY, fullShortUrl));
            if (StrUtil.isNotBlank(gotoIsNullShortLink)) {
                ((HttpServletResponse) response).sendRedirect("/page/notfound");
                return;
            }
            LambdaQueryWrapper<ShortLinkGotoDO> linkGotoQueryWrapper = Wrappers.lambdaQuery(ShortLinkGotoDO.class)
                    .eq(ShortLinkGotoDO::getFullShortUrl, fullShortUrl);
            ShortLinkGotoDO shortLinkGotoDO = shortLinkGotoMapper.selectOne(linkGotoQueryWrapper);
            if (shortLinkGotoDO == null) {
                stringRedisTemplate.opsForValue().set(String.format(GOTO_IS_NULL_SHORT_LINK_KEY, fullShortUrl), "-", 30, TimeUnit.MINUTES);
                ((HttpServletResponse) response).sendRedirect("/page/notfound");
                return;
            }
            LambdaQueryWrapper<ShortLinkDO> queryWrapper = Wrappers.lambdaQuery(ShortLinkDO.class)
                    .eq(ShortLinkDO::getGid, shortLinkGotoDO.getGid())
                    .eq(ShortLinkDO::getFullShortUrl, fullShortUrl)
                    .eq(ShortLinkDO::getDelFlag, 0)
                    .eq(ShortLinkDO::getEnableStatus, 0);
            ShortLinkDO shortLinkDO = baseMapper.selectOne(queryWrapper);
            if (shortLinkDO == null || (shortLinkDO.getValidDate() != null && shortLinkDO.getValidDate().before(new Date()))) {
                stringRedisTemplate.opsForValue().set(String.format(GOTO_IS_NULL_SHORT_LINK_KEY, fullShortUrl), "-", 30, TimeUnit.MINUTES);
                ((HttpServletResponse) response).sendRedirect("/page/notfound");
                return;
            }
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
     * 短网址跳转原始网址
     * @param swiftUri
     * @param servletRequest
     * @param servletResponse
     * @throws IOException
     */
    @SneakyThrows
    public void jumpOriginalLink(String swiftUri,ServletRequest servletRequest,ServletResponse servletResponse) throws Exception {
        // 1. 构造完整短链接
        String fullSwiftUrl = constructFullSwiftUrl(swiftUri, servletRequest);
        // 2. 先查询缓存是否存在
        String sourceUrl = swiftUrlStringRedisTemplate.opsForValue().get("swiftLink:jump:" + fullSwiftUrl);
        if (hitDestiCache(servletRequest, servletResponse, sourceUrl,fullSwiftUrl)) return;
        // 4. 若没查到，防止缓存穿透
        // 4.1. 先查布隆过滤器
        if (!swiftUrlRedisBloomFilter.contains(fullSwiftUrl)) {
            // 4.1.1. 布隆过滤器判断不存在，则一定不存在
            ((HttpServletResponse) servletResponse).sendRedirect("/page/missing");
            return;
        }
        // 4.1.2. 布隆过滤器判断存在，未必真存在，可能存在误判，先查缓存是否有空值
        String jumpNull = swiftUrlStringRedisTemplate.opsForValue().get("swiftLink:null:jump_to_" + fullSwiftUrl);
        if (jumpNull!=null && !jumpNull.trim().isEmpty()){
            // 查到缓存的空值
            ((HttpServletResponse) servletResponse).sendRedirect("/page/missing");
            return;
        }
        // 4.2. 查不到空对象，说明可能是存在的，进行缓存的构建
        // 4.1.1. 获取分布式锁，且采用 dcl
        RLock jumpLock = swiftLinkRedissonClient.getLock("swiftLink:jumpLock:" + sourceUrl);
        jumpLock.lock();
        // dcl，包括缓存击穿和缓存穿透
        sourceUrl = swiftUrlStringRedisTemplate.opsForValue().get("swiftLink:jump:" + fullSwiftUrl);
        if (hitDestiCache(servletRequest, servletResponse, sourceUrl,fullSwiftUrl)) return;
        jumpNull = swiftUrlStringRedisTemplate.opsForValue().get("swiftLink:null:jump_to_" + fullSwiftUrl);
        if (jumpNull!=null && !jumpNull.trim().isEmpty()){
            // 查到缓存的空值
            ((HttpServletResponse) servletResponse).sendRedirect("/page/missing");
            return;
        }
        try {
            if (processDatabaseQueryAndCache((HttpServletResponse) servletResponse, fullSwiftUrl)) return;
            // 5.5. 进行监控
            trackSwiftLinkAccess(fullSwiftUrl,servletRequest,servletResponse);
            // 5.6. 跳转
            ((HttpServletResponse) servletResponse).sendRedirect(sourceUrl);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            jumpLock.unlock();
        }
    }

    /**
     * 查询两个相关的数据库表并构建缓存
     * @param servletResponse
     * @param fullSwiftUrl
     * @return
     * @throws IOException
     */
    private boolean processDatabaseQueryAndCache(HttpServletResponse servletResponse, String fullSwiftUrl) throws IOException {
        //  4.1.2. 查询数据库
        QueryWrapper<SwiftLinkJumpDO> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("full_swift_url", fullSwiftUrl);
        SwiftLinkJumpDO swiftLinkJumpDO = swiftLinkJumpMapper.selectOne(queryWrapper);
        if (swiftLinkJumpDO==null) {
            // 5.1. 数据库查不到，是缓存穿透问题，设置空对象然后跳转404
            swiftUrlStringRedisTemplate.opsForValue().
                    set("swiftLink:null:jump_to_" + fullSwiftUrl,"_",30,TimeUnit.MINUTES);
            servletResponse.sendRedirect("/page/missing");
            return true;
        }
        // 5.2. 数据库查到链接跳转表，还得查询链接表
        QueryWrapper<ShortLinkDO> queryWrapperAnother = new QueryWrapper<>();
        queryWrapperAnother.eq("gid",swiftLinkJumpDO.getGid())
                        .eq("full_swift_url",swiftLinkJumpDO.getFullSwiftUrl())
                                .eq("del_flag",0)
                                        .eq("enable_status",0);
        ShortLinkDO swiftLinkDO = baseMapper.selectOne(queryWrapperAnother);
//        SwiftLinkDO swiftLinkDO = baseMapper.selectOne(queryWrapperAnother);
        if (swiftLinkDO == null || (swiftLinkDO.getValidDate() != null && swiftLinkDO.getValidDate().before(new Date()))) {
            // 5.3. 数据库查不到，是缓存穿透问题，或者已经过期，设置空对象然后跳转404
            swiftUrlStringRedisTemplate.opsForValue().
                    set("swiftLink:null:jump_to_" + fullSwiftUrl,"_",30,TimeUnit.MINUTES);
            servletResponse.sendRedirect("/page/missing");
            return true;
        }
        // 5.4. 数据库两个表都查得到，是缓存击穿，重建缓存
        long leftTime = swiftLinkDO.getValidDate()==null? 2626560000L: DateUtil.between(new Date(),swiftLinkDO.getValidDate(), DateUnit.MS);
        swiftUrlStringRedisTemplate.opsForValue()
                .set("swiftLink:jump:" + fullSwiftUrl,
                        swiftLinkDO.getSourceUrl(),
                        leftTime,
                        TimeUnit.MILLISECONDS);
        return false;
    }

    /**
     * 击中缓存数据
     * @param servletRequest
     * @param servletResponse
     * @param sourceUrl
     * @return
     * @throws IOException
     */
    private boolean hitDestiCache(ServletRequest servletRequest, ServletResponse servletResponse, String sourceUrl,String fullSwiftUrl) throws Exception {
        if (sourceUrl !=null && !sourceUrl.trim().isEmpty()) {
            // 3. 若查到了，则统计访问数据，然后跳转
            trackSwiftLinkAccess(fullSwiftUrl, servletRequest, servletResponse);
            ((HttpServletResponse) servletResponse).sendRedirect(sourceUrl);
            return true;
        }
        return false;
    }

    /**
     * 构建完整短网址
     * @param swiftUri
     * @param servletRequest
     * @return
     */
    private String constructFullSwiftUrl(String swiftUri,ServletRequest servletRequest){
        // 1.1. 获取地址栏中输入的端口，即服务端口
        int port = servletRequest.getServerPort();
        String serverPort = port != 80 ? ":" + Integer.toString(port) : "";
        // 1.2. 获取服务域名
        String serverName = servletRequest.getServerName();
        // 1.3. 拼接后返回
        return serverName+serverPort+"/"+swiftUri;
    }

    /**
     * 监控链接的访问信息
     * @param fullSwiftUrl
     * @param servletRequest
     * @param servletResponse
     * @throws Exception
     */
    private void trackSwiftLinkAccess(String fullSwiftUrl,ServletRequest servletRequest,ServletResponse servletResponse) throws Exception {
        SwiftLinkVisitDTO swiftLinkVisitDTO = generateSwiftLinkVisitDTO(fullSwiftUrl, servletRequest, servletResponse);
        // ... 发消息等逻辑
    }
    // --------------------------------------------------------------------------------

    /**
     * 构建访问统计的DTO，用于后续给其他数据库实体赋值。
     * @param fullSwiftUrl
     * @param servletRequest
     * @param servletResponse
     * @return
     * @throws Exception
     */
    private SwiftLinkVisitDTO generateSwiftLinkVisitDTO(String fullSwiftUrl,ServletRequest servletRequest,ServletResponse servletResponse) throws Exception {
        // 1. 准备各个变量
        // 1.1. 强转为 HttpServletRequest 以获取请求头中的信息
        HttpServletRequest httpReq = (HttpServletRequest) servletRequest;
        // 1.2. 标记是有史以来第一次访问该链接还是历史访问过的用户，用于统计历史访问 uv
        AtomicBoolean historyVisitorFirstFlag = new AtomicBoolean();
        // 1.3. 标记是今日第一次访问的还是今日访问过的用户，用于统计今日访问 uv
        AtomicBoolean todayVisitorFirstFlag = new AtomicBoolean();
        // 1.4. 用于放在在请求头中的 cookie 中的值
        AtomicReference<String> visitorIdForUV = new AtomicReference<>();
        // 1.5. 用于执行 lua 脚本的变量，因为添加到集合并设置有效期是两个操作，为防止出现 GC 等造成程序卡顿，带来时间间隔大，需要
        // 使用 lua 脚本使其作为一个原子性操作
        DefaultRedisScript<Long> redisScript = new DefaultRedisScript<>();
        redisScript.setScriptSource(new ResourceScriptSource(new ClassPathResource(VISIT_DATA_SET_LUA_SCRIPT_PATH)));
        redisScript.setResultType(Long.class);
        // 2. 构造 cookie 并得到 cookie 中的值，用于后续返回给前端
        String uvUUID = processVisitorCookie(redisScript,fullSwiftUrl, httpReq, (HttpServletResponse) servletResponse, visitorIdForUV, historyVisitorFirstFlag,todayVisitorFirstFlag);
        // 3. 获取用户的 IP
        String clientIp = getClientIP(httpReq);
        // 4. 获取用户的访问浏览器
        String clientBrowser = getClientBrowser(httpReq);
        // 5. 获取用户的访问设备
        String clientDevice = getClientDevice(httpReq);
        // 6. 获取用户的访问网络
        String clientNetwork = getClientNetwork(httpReq);
        // 7.1 记录uip历史访问信息添加到redis集合中的情况
        Long uipHistoryAdded=null;
        // 8.1 记录uip今日访问信息添加到redis集合中的情况
        Long uipTodayAdded=null;
        try {
            // 7.2 将用户ip放到redis的历史uip集合中，利用集合的唯一性判断是否已经重复。注意要设置每个的有效期，所以键需要精确到IP。
            uipHistoryAdded = swiftUrlStringRedisTemplate.execute(redisScript,
                    Lists.newArrayList("SwiftLink:monitorHistoryUIP:" + fullSwiftUrl + clientIp),
                    clientIp,
                    String.valueOf(0));
            // 8.2 将用户ip放到redis的今日uip集合中，利用集合的唯一性判断是否已经重复。注意要设置每个的有效期，所以键需要精确到IP。
            uipTodayAdded = swiftUrlStringRedisTemplate.execute(redisScript,
                    Lists.newArrayList("SwiftLink:monitorTodayUIP:" + fullSwiftUrl + clientIp),
                    clientIp,
                    String.valueOf(2));
        } catch (Throwable ex) {
            log.error("设置为统计访问记录的uv的LUA脚本出错", ex);
        }
        // 7.3. >0L说明添加成功，否则添加失败，说明集合中已经存在，不是新用户
        boolean uipHistoryFirst = uipHistoryAdded!=null && uipHistoryAdded>0L;
        // 8.3. >0L说明添加成功，否则添加失败，说明集合中已经存在，不是今日新用户
        boolean uipTodayFirst = uipTodayAdded!=null && uipTodayAdded>0L;
        // 9. 构建好访问统计的DTO，用于后续给其他数据库实体赋值。
        SwiftLinkVisitDTO swiftLinkVisitDTO = SwiftLinkVisitDTO.builder().fullSwiftUrl(fullSwiftUrl)
                .uv(uvUUID)
                .currentDate(new Date())
                .uvHistoryFirstFlag(historyVisitorFirstFlag.get())
                .uvTodayFirstFlag(historyVisitorFirstFlag.get())
                .uipHistoryFirstFlag(uipHistoryFirst)
                .uipTodayFirstFlag(uipTodayFirst)
                .clientIP(clientIp)
                .clientBrowser(clientBrowser)
                .clientDevice(clientDevice)
                .clientNetwork(clientNetwork)
                .build();
        return swiftLinkVisitDTO;
    }

    /**
     * 处理 uv 信息，包括历史范围和今日范围
     * @param redisScript
     * @param url
     * @param req
     * @param resp
     * @param visitorIdForUV
     * @param historyVisitorFirstFlag
     * @param todayVisitorFirstFlag
     * @return
     * @throws Exception
     */
    private String processVisitorCookie(DefaultRedisScript<Long> redisScript,String url, HttpServletRequest req, HttpServletResponse resp,
                                      AtomicReference<String> visitorIdForUV, AtomicBoolean historyVisitorFirstFlag,AtomicBoolean todayVisitorFirstFlag) throws Exception {
        // 给新用户构建 cookie 并记录和统计信息的任务
        Callable<String> cookieInitializer = () -> {
            String newCookieString = UUID.fastUUID().toString();
            Cookie trackingCookie = buildCookie(newCookieString, url);
            resp.addCookie(trackingCookie);
            visitorIdForUV.set(newCookieString);
            historyVisitorFirstFlag.set(Boolean.TRUE);
            todayVisitorFirstFlag.set(Boolean.TRUE);
            try {
                swiftUrlStringRedisTemplate.execute(redisScript,
                        Lists.newArrayList("SwiftLink:monitorHistoryUV:" + url + newCookieString),
                        newCookieString,
                        String.valueOf(1));
                swiftUrlStringRedisTemplate.execute(redisScript,
                        Lists.newArrayList("SwiftLink:monitorTodayUV:" + url + newCookieString),
                        newCookieString,
                        String.valueOf(2));
            } catch (Throwable ex) {
                log.error("设置为统计访问记录的uv的LUA脚本出错", ex);
            }
            return newCookieString;
        };
        String uvUUID = null;
        // 2.1. 找到用户请求头的键为 uv 即我们存进入的 cookie
        Optional<Cookie> optionalCookie = Optional.ofNullable(req.getCookies())
                .flatMap(cookies -> Arrays.stream(cookies)
                        .filter(c -> "uv".equals(c.getName()))
                        .findFirst());
        if (optionalCookie.isPresent()) {
            // 2.2.1. 若找到，则获取其中的 uv cookie对应的值 uvUUID，作为用户的标记
            Cookie existingCookie = optionalCookie.get();
            uvUUID = existingCookie.getValue();
            // 2.3.1 将其存入对应的redis集合中，判断是否是新用户
            handleExistingCookie(redisScript, url,uvUUID , visitorIdForUV, historyVisitorFirstFlag, todayVisitorFirstFlag);
        } else {
            try {
                // 2.2.2 若没找到，说明是新用户，构造新的 cookie 并放入响应体中。
                uvUUID = cookieInitializer.call();
            } catch (Exception e) {
                log.error("Failed to initialize cookie", e);
            }
        }
        return uvUUID;
    }

    /**
     * 构建 cookie
     * @param value
     * @param path
     * @return
     */
    private Cookie buildCookie(String value, String path) {
        Cookie cookie = new Cookie("uv", value);
        // 设置有效期为1个月，即间隔一个月访问的用户则认为是新用户
        cookie.setMaxAge((int)TimeUnit.DAYS.toSeconds(30));
        // 作用路径设置在当前短链接上，防止被其他短链访问到uv
        cookie.setPath(path.substring(path.indexOf("/")));
        return cookie;
    }

    /**
     * 处理已经存在的 cookie，用于历史uv和今日uv的统计
     * @param redisScript
     * @param url
     * @param value
     * @param visitorIdForUV
     * @param historyVisitorFirstFlag
     * @param todayVisitorFirstFlag
     */
    private void handleExistingCookie(DefaultRedisScript<Long> redisScript,String url, String value, AtomicReference<String> visitorIdForUV, AtomicBoolean historyVisitorFirstFlag,AtomicBoolean todayVisitorFirstFlag) {
        visitorIdForUV.set(value);
        Long uvHistoryAdded=null;
        Long uvTodayAdded=null;
        try {
            uvHistoryAdded = swiftUrlStringRedisTemplate.execute(redisScript,
                    Lists.newArrayList("SwiftLink:monitorHistoryUV:" + url + value),
                    value,
                    String.valueOf(1));
            uvTodayAdded = swiftUrlStringRedisTemplate.execute(redisScript,
                    Lists.newArrayList("SwiftLink:monitorTodayUV:" + url + value),
                    value,
                    String.valueOf(2));
        } catch (Throwable ex) {
            log.error("设置为统计访问记录的uv的LUA脚本出错", ex);
        }
        historyVisitorFirstFlag.set(uvHistoryAdded!=null && uvHistoryAdded>0L);
        todayVisitorFirstFlag.set(uvTodayAdded!=null && uvTodayAdded>0L);
    }

    /**
     * 获取用户请求的IP
     * @param request
     * @return
     */
    private static String getClientIP(HttpServletRequest request) {
        String clientIP = request.getHeader("X-Forwarded-For");
        if (clientIP == null || clientIP.isEmpty() || "unknown".equalsIgnoreCase(clientIP)) {
            clientIP = request.getHeader("Proxy-Client-IP");
        }
        if (clientIP == null || clientIP.isEmpty() || "unknown".equalsIgnoreCase(clientIP)) {
            clientIP = request.getHeader("WL-Proxy-Client-IP");
        }
        if (clientIP == null || clientIP.isEmpty() || "unknown".equalsIgnoreCase(clientIP)) {
            clientIP = request.getHeader("HTTP_CLIENT_IP");
        }
        if (clientIP == null || clientIP.isEmpty() || "unknown".equalsIgnoreCase(clientIP)) {
            clientIP = request.getHeader("HTTP_X_FORWARDED_FOR");
        }
        if (clientIP == null || clientIP.isEmpty() || "unknown".equalsIgnoreCase(clientIP)) {
            clientIP = request.getRemoteAddr();
        }
        return clientIP;
    }

    /**
     * 获取用户请求的浏览器
     * @param request
     * @return
     */
    private static String getClientBrowser(HttpServletRequest request) {
        String userAgent = request.getHeader("User-Agent");
        if (userAgent.toLowerCase().contains("edg")) {
            return "Microsoft Edge";
        } else if (userAgent.toLowerCase().contains("chrome")) {
            return "Google Chrome";
        } else if (userAgent.toLowerCase().contains("firefox")) {
            return "Mozilla Firefox";
        } else if (userAgent.toLowerCase().contains("safari")) {
            return "Apple Safari";
        } else if (userAgent.toLowerCase().contains("opera")) {
            return "Opera";
        } else if (userAgent.toLowerCase().contains("msie") || userAgent.toLowerCase().contains("trident")) {
            return "Internet Explorer";
        } else {
            return "Unknown";
        }
    }

    /**
     * 获取用户请求的浏览器设备
     * @param request
     * @return
     */
    private static String getClientDevice(HttpServletRequest request) {
        String userAgent = request.getHeader("User-Agent");
        if (userAgent.toLowerCase().contains("mobile")) {
            return "Mobile";
        }
        return "PC";
    }

    /**
     * 获取用户请求的网络信息
     * @param request
     * @return
     */
    private static String getClientNetwork(HttpServletRequest request) {
        String actualIp = getClientIP(request);
        // 通过简单判断IP地址范围，确定是 WIFI 还是 移动网络
        return actualIp.startsWith("192.168.") || actualIp.startsWith("10.") ? "WIFI" : "Mobile";
    }

    // --------------------------------------------------------------------------------
    
    private ShortLinkStatsRecordDTO buildLinkStatsRecordAndSetUser(String fullShortUrl, ServletRequest request, ServletResponse response) {
        AtomicBoolean uvFirstFlag = new AtomicBoolean();
        Cookie[] cookies = ((HttpServletRequest) request).getCookies();
        AtomicReference<String> uv = new AtomicReference<>();
        Runnable addResponseCookieTask = () -> {
            uv.set(UUID.fastUUID().toString());
            Cookie uvCookie = new Cookie("uv", uv.get());
            uvCookie.setMaxAge(60 * 60 * 24 * 30);
            uvCookie.setPath(StrUtil.sub(fullShortUrl, fullShortUrl.indexOf("/"), fullShortUrl.length()));
            ((HttpServletResponse) response).addCookie(uvCookie);
            uvFirstFlag.set(Boolean.TRUE);
            stringRedisTemplate.opsForSet().add(SHORT_LINK_STATS_UV_KEY + fullShortUrl, uv.get());
        };
        if (ArrayUtil.isNotEmpty(cookies)) {
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
            addResponseCookieTask.run();
        }
        String remoteAddr = LinkUtil.getActualIp(((HttpServletRequest) request));
        String os = LinkUtil.getOs(((HttpServletRequest) request));
        String browser = LinkUtil.getBrowser(((HttpServletRequest) request));
        String device = LinkUtil.getDevice(((HttpServletRequest) request));
        String network = LinkUtil.getNetwork(((HttpServletRequest) request));
        Long uipAdded = stringRedisTemplate.opsForSet().add(SHORT_LINK_STATS_UIP_KEY + fullShortUrl, remoteAddr);
        boolean uipFirstFlag = uipAdded != null && uipAdded > 0L;
//        if (uipFirstFlag){ // 添加成功，需要配合今日情况访问uip统计修改有效期
//            stringRedisTemplate.expire(SHORT_LINK_STATS_UIP_KEY + fullShortUrl,
//                    Duration.between(LocalDateTime.now(),
//                            LocalDateTime.now().plusDays(1).
//                                    withHour(0).withMinute(0).withSecond(0).
//                                    withNano(0)).getSeconds(), TimeUnit.SECONDS);
//        }
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

    @Override
    public void shortLinkStats(ShortLinkStatsRecordDTO statsRecord) {
        Map<String, String> producerMap = new HashMap<>();
        producerMap.put("statsRecord", JSON.toJSONString(statsRecord));
        shortLinkStatsSaveProducer.send(producerMap);
    }

    private String generateSuffix(ShortLinkCreateReqDTO requestParam) {
        int customGenerateCount = 0;
        String shorUri;
        String originUrl = requestParam.getOriginUrl();
        String tmpUrl = originUrl;
        while (true) {
            if (customGenerateCount > 10) {
                throw new ServiceException("短链接频繁生成，请稍后再试");
            }
            shorUri = HashUtil.hashToBase62(tmpUrl);
            if (!shortUriCreateCachePenetrationBloomFilter.contains(createShortLinkDefaultDomain + "/" + shorUri)) {
                break;
            }
            customGenerateCount++;
            tmpUrl = UUID.randomUUID()+originUrl;
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
            LambdaQueryWrapper<ShortLinkGotoDO> queryWrapper = Wrappers.lambdaQuery(ShortLinkGotoDO.class)
                    .eq(ShortLinkGotoDO::getGid, requestParam.getGid())
                    .eq(ShortLinkGotoDO::getFullShortUrl, createShortLinkDefaultDomain + "/" + shorUri);
            ShortLinkGotoDO shortLinkGotoDO = shortLinkGotoMapper.selectOne(queryWrapper);
            if (shortLinkGotoDO == null) {
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
