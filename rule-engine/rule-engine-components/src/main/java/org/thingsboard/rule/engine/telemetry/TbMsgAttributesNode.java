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
package org.thingsboard.rule.engine.telemetry;

import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.rule.engine.api.RuleNode;
import org.thingsboard.rule.engine.api.TbContext;
import org.thingsboard.rule.engine.api.TbNode;
import org.thingsboard.rule.engine.api.TbNodeConfiguration;
import org.thingsboard.rule.engine.api.TbNodeException;
import org.thingsboard.rule.engine.api.util.TbNodeUtils;
import org.thingsboard.server.common.data.StringUtils;
import org.thingsboard.server.common.data.kv.AttributeKvEntry;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.msg.TbMsg;
import org.thingsboard.server.common.msg.session.SessionMsgType;
import org.thingsboard.server.common.transport.adaptor.JsonConverter;

import java.util.ArrayList;
import java.util.List;

import static org.thingsboard.server.common.data.DataConstants.CLIENT_SCOPE;
import static org.thingsboard.server.common.data.DataConstants.NOTIFY_DEVICE_METADATA_KEY;
import static org.thingsboard.server.common.data.DataConstants.SCOPE;

@Slf4j
@RuleNode(
        type = ComponentType.ACTION,
        name = "保存属性",
        configClazz = TbMsgAttributesNodeConfiguration.class,
        nodeDescription = "保存属性数据",
        nodeDetails = "根据可配置的范围参数保存实体属性。 需要消息类型为“POST_ATTRIBUTES_REQUEST”的消息。" +
                      "如果更新插入（更新/插入）操作成功完成，规则节点将通过<b>Success</b>链发送传入消息，否则，使用<b>Failure</b>链。" +
                      "此外，如果复选框 <b>发送属性更新通知</b> 设置为 true，规则节点将放置“Attributes Updated”" +
                      "<b>SHARED_SCOPE</b> 和 <b>SERVER_SCOPE</b> 属性的事件更新到相应的规则引擎队列。",
        uiResources = {"static/rulenode/rulenode-core-config.js"},
        configDirective = "tbActionNodeAttributesConfig",
        icon = "file_upload"
)
public class TbMsgAttributesNode implements TbNode {

    private TbMsgAttributesNodeConfiguration config;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, TbMsgAttributesNodeConfiguration.class);
        if (config.getNotifyDevice() == null) {
            config.setNotifyDevice(true);
        }
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) {
        if (!msg.getType().equals(SessionMsgType.POST_ATTRIBUTES_REQUEST.name())) {
            ctx.tellFailure(msg, new IllegalArgumentException("Unsupported msg type: " + msg.getType()));
            return;
        }
        String src = msg.getData();
        List<AttributeKvEntry> attributes = new ArrayList<>(JsonConverter.convertToAttributes(JsonParser.parseString(src)));
        if (attributes.isEmpty()) {
            ctx.tellSuccess(msg);
            return;
        }
        String scope = getScope(msg.getMetaData().getValue(SCOPE));
        boolean sendAttributesUpdateNotification = checkSendNotification(scope);
        ctx.getTelemetryService().saveAndNotify(
                ctx.getTenantId(),
                msg.getOriginator(),
                scope,
                attributes,
                checkNotifyDevice(msg.getMetaData().getValue(NOTIFY_DEVICE_METADATA_KEY)),
                sendAttributesUpdateNotification ?
                        new AttributesUpdateNodeCallback(ctx, msg, scope, attributes) :
                        new TelemetryNodeCallback(ctx, msg)
        );
    }

    private boolean checkSendNotification(String scope) {
        return config.isSendAttributesUpdatedNotification() && !CLIENT_SCOPE.equals(scope);
    }

    private boolean checkNotifyDevice(String notifyDeviceMdValue) {
        return config.getNotifyDevice() || StringUtils.isEmpty(notifyDeviceMdValue) || Boolean.parseBoolean(notifyDeviceMdValue);
    }

    private String getScope(String mdScopeValue) {
        if (StringUtils.isNotEmpty(mdScopeValue)) {
            return mdScopeValue;
        }
        return config.getScope();
    }

}
