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
package org.thingsboard.rule.engine.action;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.rule.engine.api.RuleNode;
import org.thingsboard.rule.engine.api.TbContext;
import org.thingsboard.rule.engine.api.TbNodeConfiguration;
import org.thingsboard.rule.engine.api.TbNodeException;
import org.thingsboard.rule.engine.api.util.TbNodeUtils;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.alarm.Alarm;
import org.thingsboard.server.common.data.alarm.AlarmApiCallResult;
import org.thingsboard.server.common.data.id.AlarmId;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.msg.TbMsg;

@Slf4j
@RuleNode(
        type = ComponentType.ACTION,
        name = "清除告警", relationTypes = {"Cleared", "False"},
        configClazz = TbClearAlarmNodeConfiguration.class,
        nodeDescription = "Clear Alarm",
        nodeDetails =
                "详细信息 - 根据传入消息创建 JSON 对象的 JS 函数。 该对象将被添加到 Alarm.details 字段中。\n" +
                        "节点输出：\n" +
                        "如果未清除警报，则返回原始消息。 否则返回类型为“ALARM”的新消息，“msg”属性和“元数据”中的警报对象将包含“isClearedAlarm”属性。" +
                        "可以通过 <code>msg</code> 属性访问消息负载。 例如 <code>'temperature = ' + msg.temperature ;</code>。" +
                        "可以通过 <code>metadata</code> 属性访问消息元数据。 例如 <code>'name = ' + metadata.customerName;</code>。",
        uiResources = {"static/rulenode/rulenode-core-config.js"},
        configDirective = "tbActionNodeClearAlarmConfig",
        icon = "notifications_off"
)
public class TbClearAlarmNode extends TbAbstractAlarmNode<TbClearAlarmNodeConfiguration> {

    @Override
    protected TbClearAlarmNodeConfiguration loadAlarmNodeConfig(TbNodeConfiguration configuration) throws TbNodeException {
        return TbNodeUtils.convert(configuration, TbClearAlarmNodeConfiguration.class);
    }

    @Override
    protected ListenableFuture<TbAlarmResult> processAlarm(TbContext ctx, TbMsg msg) {
        String alarmType = TbNodeUtils.processPattern(this.config.getAlarmType(), msg);
        Alarm alarm;
        if (msg.getOriginator().getEntityType().equals(EntityType.ALARM)) {
            alarm = ctx.getAlarmService().findAlarmById(ctx.getTenantId(), new AlarmId(msg.getOriginator().getId()));
        } else {
            alarm = ctx.getAlarmService().findLatestActiveByOriginatorAndType(ctx.getTenantId(), msg.getOriginator(), alarmType);
        }
        if (alarm != null && !alarm.getStatus().isCleared()) {
            return clearAlarm(ctx, msg, alarm);
        }
        return Futures.immediateFuture(new TbAlarmResult(false, false, false, null));
    }

    private ListenableFuture<TbAlarmResult> clearAlarm(TbContext ctx, TbMsg msg, Alarm alarm) {
        ctx.logJsEvalRequest();
        ListenableFuture<JsonNode> asyncDetails = buildAlarmDetails(ctx, msg, alarm.getDetails());
        return Futures.transform(asyncDetails, details -> {
            ctx.logJsEvalResponse();
            AlarmApiCallResult result = ctx.getAlarmService().clearAlarm(ctx.getTenantId(), alarm.getId(), System.currentTimeMillis(), details);
            if (result.isSuccessful()) {
                return new TbAlarmResult(false, false, result.isCleared(), result.getAlarm());
            } else {
                return new TbAlarmResult(false, false, false, alarm);
            }
        }, ctx.getDbCallbackExecutor());
    }
}
