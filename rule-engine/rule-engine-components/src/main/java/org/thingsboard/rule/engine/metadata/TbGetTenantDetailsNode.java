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
package org.thingsboard.rule.engine.metadata;

import com.google.common.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.rule.engine.api.RuleNode;
import org.thingsboard.rule.engine.api.TbContext;
import org.thingsboard.rule.engine.api.TbNodeConfiguration;
import org.thingsboard.rule.engine.api.TbNodeException;
import org.thingsboard.rule.engine.api.util.TbNodeUtils;
import org.thingsboard.server.common.data.ContactBased;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.msg.TbMsg;

@Slf4j
@RuleNode(type = ComponentType.ENRICHMENT,
        name = "租户详细信息",
        configClazz = TbGetTenantDetailsNodeConfiguration.class,
        nodeDescription = "将租户详细信息中的字段添加到消息正文或元数据",
        nodeDetails = "如果勾选复选框：<b>将所选详细信息添加到消息元数据</b>被选中，现有字段将被添加到消息元数据而不是消息数据。<br><br>" +
                "<b>注意</b>：仅允许设备、资产和实体视图类型。<br><br>" +
                "如果消息的发起者未分配给 Tenant，或者发起者类型不受支持 -消息将被转发到 <b>Failure</b> 链，否则，将使用 <b>Success</b> 链。",
        uiResources = {"static/rulenode/rulenode-core-config.js"},
        configDirective = "tbEnrichmentNodeEntityDetailsConfig")
public class TbGetTenantDetailsNode extends TbAbstractGetEntityDetailsNode<TbGetTenantDetailsNodeConfiguration> {

    private static final String TENANT_PREFIX = "tenant_";

    @Override
    protected TbGetTenantDetailsNodeConfiguration loadGetEntityDetailsNodeConfiguration(TbNodeConfiguration configuration) throws TbNodeException {
        return TbNodeUtils.convert(configuration, TbGetTenantDetailsNodeConfiguration.class);
    }

    @Override
    protected ListenableFuture<TbMsg> getDetails(TbContext ctx, TbMsg msg) {
        return getTbMsgListenableFuture(ctx, msg, getDataAsJson(msg), TENANT_PREFIX);
    }

    @Override
    protected ListenableFuture<? extends ContactBased> getContactBasedListenableFuture(TbContext ctx, TbMsg msg) {
        return ctx.getTenantService().findTenantByIdAsync(ctx.getTenantId(), ctx.getTenantId());
    }
}
