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
package org.thingsboard.rule.engine.transform;

import com.google.common.util.concurrent.ListenableFuture;
import org.thingsboard.rule.engine.api.RuleNode;
import org.thingsboard.rule.engine.api.ScriptEngine;
import org.thingsboard.rule.engine.api.TbContext;
import org.thingsboard.rule.engine.api.TbNodeConfiguration;
import org.thingsboard.rule.engine.api.TbNodeException;
import org.thingsboard.rule.engine.api.util.TbNodeUtils;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.data.script.ScriptLanguage;
import org.thingsboard.server.common.msg.TbMsg;

import java.util.List;

@RuleNode(
        type = ComponentType.TRANSFORMATION,
        name = "脚本",
        configClazz = TbTransformMsgNodeConfiguration.class,
        nodeDescription = "使用 JavaScript 更改消息负载、元数据或消息类型",
        nodeDetails = "JavaScript 函数接收 3 个输入参数 <br/>" +
                "<code>metadata</code> - 是消息元数据。<br/>" +
                "<code>msg</code> - 是消息负载。<br/>" +
                "<code>msgType</code> - 是消息类型。<br/>" +
                "应返回以下结构：<br/>" +
                "<code>{ msg: <i style=\"color: #666;\">新有效负载</i>,<br/>&nbsp&nbsp&nbsp元数据: <i style=\"color: #666;\">新元数据< /i>,<br/>&nbsp&nbsp&nbsp消息类型: <i style=\"color: #666;\">新消息类型</i> </code><br/>" +
                "结果对象中的所有字段都是可选的，如果未指定，将从原始消息中获取。",
        uiResources = {"static/rulenode/rulenode-core-config.js"},
        configDirective = "tbTransformationNodeScriptConfig"
)
public class TbTransformMsgNode extends TbAbstractTransformNode {

    private TbTransformMsgNodeConfiguration config;
    private ScriptEngine scriptEngine;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, TbTransformMsgNodeConfiguration.class);
        scriptEngine = ctx.createScriptEngine(config.getScriptLang(),
                ScriptLanguage.TBEL.equals(config.getScriptLang()) ? config.getTbelScript() : config.getJsScript());
        setConfig(config);
    }

    @Override
    protected ListenableFuture<List<TbMsg>> transform(TbContext ctx, TbMsg msg) {
        ctx.logJsEvalRequest();
        return scriptEngine.executeUpdateAsync(msg);
    }

    @Override
    protected void transformSuccess(TbContext ctx, TbMsg msg, TbMsg m) {
        ctx.logJsEvalResponse();
        super.transformSuccess(ctx, msg, m);
    }

    @Override
    protected void transformFailure(TbContext ctx, TbMsg msg, Throwable t) {
        ctx.logJsEvalFailure();
        super.transformFailure(ctx, msg, t);
    }

    @Override
    public void destroy() {
        if (scriptEngine != null) {
            scriptEngine.destroy();
        }
    }
}
