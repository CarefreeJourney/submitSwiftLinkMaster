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

import cn.hutool.core.collection.CollUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.lcl.swiftlink.admin.common.biz.user.UserContext;
import com.lcl.swiftlink.admin.common.convention.exception.ServiceException;
import com.lcl.swiftlink.admin.common.convention.result.Result;
import com.lcl.swiftlink.admin.dao.entity.GroupDO;
import com.lcl.swiftlink.admin.remote.ShortLinkActualRemoteService;
import com.lcl.swiftlink.admin.remote.dto.req.ShortLinkRecycleBinPageReqDTO;
import com.lcl.swiftlink.admin.remote.dto.resp.ShortLinkPageRespDTO;
import com.lcl.swiftlink.admin.service.GarbageCanService;
import com.lcl.swiftlink.project.dao.entity.LinkGroupDO;
import com.lcl.swiftlink.project.dao.mapper.LinkGroupMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * URL 回收站接口实现层
 */
@Service(value = "recycleBinServiceImplByAdmin")
@RequiredArgsConstructor
public class GarbageCanServiceImpl implements GarbageCanService {

    private final ShortLinkActualRemoteService shortLinkActualRemoteService;
    private final LinkGroupMapper linkGroupMapper;

    @Override
    public Result<Page<ShortLinkPageRespDTO>> pageRecycleBinShortLink(ShortLinkRecycleBinPageReqDTO shortLinkRecycleBinPageReqDTO) {
        LambdaQueryWrapper<LinkGroupDO> queryWrapper = Wrappers.lambdaQuery(LinkGroupDO.class)
                .eq(LinkGroupDO::getUsername, UserContext.getUsername())
                .eq(LinkGroupDO::getDelFlag, 0);
        List<LinkGroupDO> groupDOList = linkGroupMapper.selectList(queryWrapper);
        if (CollUtil.isEmpty(groupDOList)) {
            throw new ServiceException("用户无分组信息");
        }
        shortLinkRecycleBinPageReqDTO.setGidList(groupDOList.stream().map(LinkGroupDO::getGid).toList());
        return shortLinkActualRemoteService.pageRecycleBinShortLink(shortLinkRecycleBinPageReqDTO.getGidList(), shortLinkRecycleBinPageReqDTO.getCurrent(), shortLinkRecycleBinPageReqDTO.getSize());
    }
}
