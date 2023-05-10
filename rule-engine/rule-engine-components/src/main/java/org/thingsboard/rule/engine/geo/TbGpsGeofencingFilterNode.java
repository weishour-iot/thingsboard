/**
 * Copyright © 2016-2023 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.rule.engine.geo;

import lombok.extern.slf4j.Slf4j;
import org.thingsboard.rule.engine.api.RuleNode;
import org.thingsboard.rule.engine.api.TbContext;
import org.thingsboard.rule.engine.api.TbNodeException;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.msg.TbMsg;

/**
 * Created by ashvayka on 19.01.18.
 */
@Slf4j
@RuleNode(
        type = ComponentType.FILTER,
        name = "GPS地理围栏过滤器",
        configClazz = TbGpsGeofencingFilterNodeConfiguration.class,
        relationTypes = {"True", "False"},
        nodeDescription = "通过基于GPS的地理围栏过滤传入消息",
        nodeDetails = "从传入的信息中提取经纬度参数，并根据配置的周长进行检查。</br>" +
                "配置:</br></br>" +
                "<ul>" +
                "<li>纬度键名-包含位置纬度的消息字段的名称;</li>" +
                "<li>经度键名-包含位置经度的消息字段的名称;</li>" +
                "<li>周长型-多边形或圆形;</li>" +
                "<li>从消息元数据中获取周长-复选框用于从消息元数据中加载周长;" +
                "   如果你的周界是特定于设备/资产的，并且你把它存储为设备/资产属性，则启用；</li>" +
                "<li>周界密钥名称——存储周界信息的元数据密钥名称;</li>" +
                "<li>对于多边形周长类型: <ul>" +
                "    <li>多边形定义 - 包含以下格式的坐标阵列的字符串： [[lat1, lon1], [lat2, lon2], [lat3, lon3], ... , [latN, lonN]]</li>" +
                "</ul></li>" +
                "<li>对于圆周型:<ul>" +
                "   <li>圆心纬度——圆周长圆心的纬度;</li>" +
                "   <li>圆心经度——圆周长圆心的经度;</li>" +
                "   <li>范围 -圆周长范围值，双精度浮点值;</li>" +
                "   <li>范围单位-米，公里，英尺，英里，海里之一;</li>" +
                "</ul></li></ul></br>" +
                "如果启用了 \"从信息元数据中获取周边信息\"并且没有配置 \"周边密钥名称\"，规则节点将使用默认的元数据键名。" +
                "多边形周长类型的默认元数据键名是 \"周长\"。圆周长的默认元数据键名是： \"centerLatitude\"、\"centerLongitude\"、\"range\"、\"rangeUnit\"。" +
                "</br></br>" +
                "圆周定义的结构(例如存储在服务器端属性中):" +
                "</br></br>" +
                "{\"latitude\":  48.198618758582384, \"longitude\": 24.65322245153503, \"radius\":  100.0, \"radiusUnit\": \"METER\" }" +
                "</br></br>" +
                "可选半径单位:米、公里、英尺、英里、海里;",
        uiResources = {"static/rulenode/rulenode-core-config.js"},
        configDirective = "tbFilterNodeGpsGeofencingConfig")
public class TbGpsGeofencingFilterNode extends AbstractGeofencingNode<TbGpsGeofencingFilterNodeConfiguration> {

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) throws TbNodeException {
        ctx.tellNext(msg, checkMatches(msg) ? "True" : "False");
    }

    @Override
    protected Class<TbGpsGeofencingFilterNodeConfiguration> getConfigClazz() {
        return TbGpsGeofencingFilterNodeConfiguration.class;
    }
}
