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

package com.lcl.swiftlink.project.dto.biz;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * 短链接统计实体
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SwiftLinkVisitDTO {

    /**
     * 完整短链接
     */
    private String fullSwiftUrl;

    /**
     * 访问用户IP
     */
    private String clientIP;

    /**
     * 浏览器
     */
    private String clientBrowser;

    /**
     * 操作设备
     */
    private String clientDevice;

    /**
     * 网络
     */
    private String clientNetwork;

    /**
     * UV
     */
    private String uv;

    /**
     * UV历史访问标识
     */
    private Boolean uvHistoryFirstFlag;

    /**
     * UIP历史访问标识
     */
    private Boolean uipHistoryFirstFlag;

    /**
     * UV今日访问标识
     */
    private Boolean uvTodayFirstFlag;

    /**
     * UIP今日访问标识
     */
    private Boolean uipTodayFirstFlag;

    /**
     * 消息队列唯一标识
     */
    private String keys;

    /**
     * 当前时间
     */
    private Date currentDate;
}
