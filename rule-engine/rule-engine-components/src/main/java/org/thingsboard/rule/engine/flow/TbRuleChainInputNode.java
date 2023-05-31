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
package org.thingsboard.rule.engine.flow;

import lombok.extern.slf4j.Slf4j;
import org.thingsboard.rule.engine.api.RuleNode;
import org.thingsboard.rule.engine.api.TbContext;
import org.thingsboard.rule.engine.api.TbNode;
import org.thingsboard.rule.engine.api.TbNodeConfiguration;
import org.thingsboard.rule.engine.api.TbNodeException;
import org.thingsboard.rule.engine.api.util.TbNodeUtils;
import org.thingsboard.server.common.data.id.RuleChainId;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.msg.TbMsg;

import java.util.UUID;

@Slf4j
@RuleNode(
        type = ComponentType.FLOW,
        name = "规则链",
        configClazz = TbRuleChainInputNodeConfiguration.class,
        nodeDescription = "将消息传输到另一个规则链",
        nodeDetails = "允许嵌套类似于单个规则节点的规则链。" +
                "传入消息被转发到指定目标规则链的输入节点。" +
                "目标规则链可能会产生多个带标签的输出。" +
                "您可以使用输出将处理结果转发给其他规则节点。",
        uiResources = {"static/rulenode/rulenode-core-config.js"},
        configDirective = "tbFlowNodeRuleChainInputConfig",
        relationTypes = {},
        ruleChainNode = true,
        customRelations = true
)
public class TbRuleChainInputNode implements TbNode {

    private TbRuleChainInputNodeConfiguration config;
    private RuleChainId ruleChainId;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, TbRuleChainInputNodeConfiguration.class);
        this.ruleChainId = new RuleChainId(UUID.fromString(config.getRuleChainId()));
        ctx.checkTenantEntity(ruleChainId);
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) {
        ctx.input(msg, ruleChainId);
    }

}
