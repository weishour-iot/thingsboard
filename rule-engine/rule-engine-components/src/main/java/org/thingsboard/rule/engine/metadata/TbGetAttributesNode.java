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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.rule.engine.api.RuleNode;
import org.thingsboard.rule.engine.api.TbContext;
import org.thingsboard.rule.engine.api.TbNodeConfiguration;
import org.thingsboard.rule.engine.api.TbNodeException;
import org.thingsboard.rule.engine.api.util.TbNodeUtils;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.msg.TbMsg;

/**
 * Created by ashvayka on 19.01.18.
 */
@Slf4j
@RuleNode(type = ComponentType.ENRICHMENT,
          name = "发起者属性",
          configClazz = TbGetAttributesNodeConfiguration.class,
          nodeDescription = "使用发起者属性或时间序列数据扩充消息正文或元数据",
          nodeDetails = "如果配置了属性扩充，<b>CLIENT/SHARED/SERVER</b> 属性将添加到消息数据/元数据中" +
                "具有特定前缀：<i>cs/shared/ss</i>。 最新的遥测值添加到没有前缀的消息数据/元数据中。" +
                  "要访问其他节点中的这些属性，可以使用此模板" +
                "<code>metadata.cs_temperature</code> or <code>metadata.shared_limit</code> ",
        uiResources = {"static/rulenode/rulenode-core-config.js"},
        configDirective = "tbEnrichmentNodeOriginatorAttributesConfig")
public class TbGetAttributesNode extends TbAbstractGetAttributesNode<TbGetAttributesNodeConfiguration, EntityId> {

    @Override
    protected TbGetAttributesNodeConfiguration loadGetAttributesNodeConfig(TbNodeConfiguration configuration) throws TbNodeException {
        return TbNodeUtils.convert(configuration, TbGetAttributesNodeConfiguration.class);
    }

    @Override
    protected ListenableFuture<EntityId> findEntityIdAsync(TbContext ctx, TbMsg msg) {
        return Futures.immediateFuture(msg.getOriginator());
    }

}
