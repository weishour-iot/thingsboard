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

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.common.util.JacksonUtil;
import org.thingsboard.rule.engine.api.RuleNode;
import org.thingsboard.rule.engine.api.TbContext;
import org.thingsboard.rule.engine.api.TbNode;
import org.thingsboard.rule.engine.api.TbNodeConfiguration;
import org.thingsboard.rule.engine.api.TbNodeException;
import org.thingsboard.rule.engine.api.util.TbNodeUtils;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.msg.TbMsg;

import java.util.concurrent.ExecutionException;

@Slf4j
@RuleNode(
        type = ComponentType.TRANSFORMATION,
        name = "JsonPath",
        configClazz = TbJsonPathNodeConfiguration.class,
        nodeDescription = "使用 JSONPath 表达式转换传入的消息正文。",
        nodeDetails = "JSONPath 表达式指定 JSON 结构中一个元素或一组元素的路径。 <br/>"
                + "<b>'$'</b> 表示根对象或数组。 <br/>"
                + "如果 JSONPath 表达式评估失败，则传入消息通过 <code>Failure</code> 链路由，"
                + "否则使用 <code>Success</code> 链。",
        uiResources = {"static/rulenode/rulenode-core-config.js"},
        icon = "functions",
        configDirective = "tbTransformationNodeJsonPathConfig"
)
public class TbJsonPathNode implements TbNode {

    private TbJsonPathNodeConfiguration config;
    private Configuration configurationJsonPath;
    private JsonPath jsonPath;
    private String jsonPathValue;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, TbJsonPathNodeConfiguration.class);
        this.jsonPathValue = config.getJsonPath();
        if (!TbJsonPathNodeConfiguration.DEFAULT_JSON_PATH.equals(this.jsonPathValue)) {
            this.configurationJsonPath = Configuration.builder()
                    .jsonProvider(new JacksonJsonNodeJsonProvider())
                    .build();
            this.jsonPath = JsonPath.compile(config.getJsonPath());
        }
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) throws ExecutionException, InterruptedException, TbNodeException {
        if (!TbJsonPathNodeConfiguration.DEFAULT_JSON_PATH.equals(this.jsonPathValue)) {
            try {
                Object jsonPathData = jsonPath.read(msg.getData(), this.configurationJsonPath);
                ctx.tellSuccess(TbMsg.transformMsg(msg, msg.getType(), msg.getOriginator(), msg.getMetaData(), JacksonUtil.toString(jsonPathData)));
            } catch (PathNotFoundException e) {
                ctx.tellFailure(msg, e);
            }
        } else {
            ctx.tellSuccess(msg);
        }
    }
}
