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

package com.lcl.swiftlink.admin.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.lang.UUID;
import com.alibaba.fastjson2.JSON;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.lcl.swiftlink.admin.common.biz.user.UserContext;
import com.lcl.swiftlink.admin.common.convention.exception.ClientException;
import com.lcl.swiftlink.admin.common.convention.exception.ServiceException;
import com.lcl.swiftlink.admin.common.enums.UserErrorCodeEnum;
import com.lcl.swiftlink.admin.dao.entity.UserDO;
import com.lcl.swiftlink.admin.dao.mapper.UserMapper;
import com.lcl.swiftlink.admin.dto.req.RegisterReqDTO;
import com.lcl.swiftlink.admin.dto.req.UserLoginReqDTO;
import com.lcl.swiftlink.admin.dto.req.UserRegisterReqDTO;
import com.lcl.swiftlink.admin.dto.req.UserUpdateReqDTO;
import com.lcl.swiftlink.admin.dto.resp.UserLoginRespDTO;
import com.lcl.swiftlink.admin.dto.resp.UserRespDTO;
import com.lcl.swiftlink.admin.service.GroupService;
import com.lcl.swiftlink.admin.service.LinkGroupService;
import com.lcl.swiftlink.admin.service.UserService;
import io.micrometer.core.instrument.Clock;
import lombok.RequiredArgsConstructor;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.BeanUtils;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static com.lcl.swiftlink.admin.common.constant.RedisCacheConstant.LOCK_USER_REGISTER_KEY;
import static com.lcl.swiftlink.admin.common.constant.RedisCacheConstant.USER_LOGIN_KEY;
import static com.lcl.swiftlink.admin.common.enums.UserErrorCodeEnum.USER_EXIST;
import static com.lcl.swiftlink.admin.common.enums.UserErrorCodeEnum.USER_NAME_EXIST;
import static com.lcl.swiftlink.admin.common.enums.UserErrorCodeEnum.USER_SAVE_ERROR;

/**
 * 用户接口实现层
 */
@Service
@RequiredArgsConstructor
public class UserServiceImpl extends ServiceImpl<UserMapper, UserDO> implements UserService {

    private final RBloomFilter<String> userRegisterCachePenetrationBloomFilter;
    private final RBloomFilter<String> registerRedisBloomFilter;
    private final RedissonClient redissonClient;
    private final StringRedisTemplate stringRedisTemplate;
    private final GroupService groupService;
    private final LinkGroupService linkGroupService;

    @Override
    public UserRespDTO getUserByUsername(String username) {
        LambdaQueryWrapper<UserDO> queryWrapper = Wrappers.lambdaQuery(UserDO.class)
                .eq(UserDO::getUsername, username);
        UserDO userDO = baseMapper.selectOne(queryWrapper);
        if (userDO == null) {
            throw new ServiceException(UserErrorCodeEnum.USER_NULL);
        }
        UserRespDTO result = new UserRespDTO();
        BeanUtils.copyProperties(userDO, result);
        return result;
    }

    @Override
    public Boolean hasUsername(String username) {
        return !userRegisterCachePenetrationBloomFilter.contains(username);
    }

    @Transactional(rollbackFor = Exception.class)
    @Override
    public void register(UserRegisterReqDTO requestParam) {
        if (!hasUsername(requestParam.getUsername())) {
            throw new ClientException(USER_NAME_EXIST);
        }
        RLock lock = redissonClient.getLock(LOCK_USER_REGISTER_KEY + requestParam.getUsername());
        if (!lock.tryLock()) {
            throw new ClientException(USER_NAME_EXIST);
        }
        try {
            int inserted = baseMapper.insert(BeanUtil.toBean(requestParam, UserDO.class));
            if (inserted < 1) {
                throw new ClientException(USER_SAVE_ERROR);
            }
            groupService.saveGroup(requestParam.getUsername(), "默认分组");
            userRegisterCachePenetrationBloomFilter.add(requestParam.getUsername());
        } catch (DuplicateKeyException ex) {
            throw new ClientException(USER_EXIST);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 用户注册
     * @param registerReqDTO
     */
    // 涉及到用户表和链接表两个表的操作，要求事务的原子性
    @Transactional(rollbackFor = Exception.class)
    public void userRegister(RegisterReqDTO registerReqDTO){
        // 1. 首先检查布隆过滤器中是否已有该用户名
        if (registerRedisBloomFilter.contains(registerReqDTO.getUsername())) {
            throw new ClientException(USER_NAME_EXIST);
        }
        // 2. 获取分布式锁，避免对数据库压力过大，在多线程下布隆过滤器查询不到，但可能此时已有数据插入数据库但还
        // 没来得及加入布隆，此时用户名也是已存在的情况，为避免用户名已存在进行多次插入导致索引冲突，也需要使用分布式锁
        // 锁粒度设置为用户名，也就是不同用户名，仍然可以持有锁，不互斥。
        RLock registerUserNameLock = redissonClient.getLock("admin:lock_register:" + registerReqDTO.getUsername());
        // 3. 若获取锁失败，直接抛异常，采取快失败方法，可以容忍
        if (!registerUserNameLock.tryLock()){
            throw new ClientException(USER_NAME_EXIST);
        }
        // 4. 构建实体类对象
        UserDO user = UserDO.builder().username(registerReqDTO.getUsername())
                .phone(registerReqDTO.getPhone())
                .password(registerReqDTO.getPassword())
                .mail(registerReqDTO.getMail())
                .realName(registerReqDTO.getRealName()).build();
        try {
            // 5. 将用户信息插入数据库
            if (baseMapper.insert(user)<1){
                throw new ClientException(USER_SAVE_ERROR);
            }
            // 6. 进行后续业务处理，添加默认分组到该用户下
            initializeUserData(user.getUsername());
            // 7.1 将用户名加入布隆过滤器
            registerRedisBloomFilter.add(user.getUsername());
        } catch (Exception e) {
            // 7.2 可能是用户名唯一索引冲突，因为虽然有布隆过滤器和分布式锁，但在分布式下还是可能会有小概率冲突发生
            throw new ClientException(USER_SAVE_ERROR);
        } finally {
            // 8. 注意放在 finally，不管有没有异常都会释放分布式锁
            registerUserNameLock.unlock();
        }
    }

    /**
     * 用户注册后的后续业务处理
     * @param username
     */
    private void initializeUserData(String username) {
        linkGroupService.saveLinkGroup(username, "默认分组");
    }

    @Override
    public void update(UserUpdateReqDTO requestParam) {
        if (!Objects.equals(requestParam.getUsername(), UserContext.getUsername())) {
            throw new ClientException("当前登录用户修改请求异常");
        }
        LambdaUpdateWrapper<UserDO> updateWrapper = Wrappers.lambdaUpdate(UserDO.class)
                .eq(UserDO::getUsername, requestParam.getUsername());
        baseMapper.update(BeanUtil.toBean(requestParam, UserDO.class), updateWrapper);
        stringRedisTemplate.delete(USER_LOGIN_KEY + requestParam.getUsername());
    }

    @Override
    public UserLoginRespDTO login(UserLoginReqDTO requestParam) {
        LambdaQueryWrapper<UserDO> queryWrapper = Wrappers.lambdaQuery(UserDO.class)
                .eq(UserDO::getUsername, requestParam.getUsername())
                .eq(UserDO::getPassword, requestParam.getPassword())
                .eq(UserDO::getDelFlag, 0);
        UserDO userDO = baseMapper.selectOne(queryWrapper);
        if (userDO == null) {
            throw new ClientException("用户不存在");
        }
        Map<Object, Object> hasLoginMap = stringRedisTemplate.opsForHash().entries(USER_LOGIN_KEY + requestParam.getUsername());
        if (CollUtil.isNotEmpty(hasLoginMap)) {
            stringRedisTemplate.expire(USER_LOGIN_KEY + requestParam.getUsername(), 30L, TimeUnit.MINUTES);
            String token = hasLoginMap.keySet().stream()
                    .findFirst()
                    .map(Object::toString)
                    .orElseThrow(() -> new ClientException("用户登录错误"));
            return new UserLoginRespDTO(token);
        }
        /**
         * Hash
         * Key：login_用户名
         * Value：
         *  Key：token标识
         *  Val：JSON 字符串（用户信息）
         */
        String uuid = UUID.randomUUID().toString();
        stringRedisTemplate.opsForHash().put(USER_LOGIN_KEY + requestParam.getUsername(), uuid, JSON.toJSONString(userDO));
        stringRedisTemplate.expire(USER_LOGIN_KEY + requestParam.getUsername(), 30L, TimeUnit.MINUTES);
        return new UserLoginRespDTO(uuid);
    }

    @Override
    public Boolean checkLogin(String username, String token) {
        return stringRedisTemplate.opsForHash().get(USER_LOGIN_KEY + username, token) != null;
    }

    @Override
    public void logout(String username, String token) {
        if (checkLogin(username, token)) {
            stringRedisTemplate.delete(USER_LOGIN_KEY + username);
            return;
        }
        throw new ClientException("用户Token不存在或用户未登录");
    }
}
