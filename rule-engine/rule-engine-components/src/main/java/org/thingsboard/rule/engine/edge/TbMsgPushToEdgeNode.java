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
package org.thingsboard.rule.engine.edge;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.thingsboard.rule.engine.api.RuleNode;
import org.thingsboard.rule.engine.api.TbContext;
import org.thingsboard.server.common.data.DataConstants;
import org.thingsboard.server.common.data.EdgeUtils;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.edge.EdgeEvent;
import org.thingsboard.server.common.data.edge.EdgeEventActionType;
import org.thingsboard.server.common.data.edge.EdgeEventType;
import org.thingsboard.server.common.data.id.EdgeId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.page.PageData;
import org.thingsboard.server.common.data.page.PageLink;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.data.rule.RuleChainType;
import org.thingsboard.server.common.msg.TbMsg;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Slf4j
@RuleNode(
        type = ComponentType.ACTION,
        name = "推送边缘",
        configClazz = TbMsgPushToEdgeNodeConfiguration.class,
        nodeDescription = "将消息从云端推送到边缘",
        nodeDetails = "将消息从云端推送到边缘。" +
                "消息发起者必须分配给特定的边缘，或者消息发起者是 <b>EDGE</b> 实体本身。" +
                "该节点仅在云实例上使用，用于将消息从云端推送到边缘。" +
                "一旦消息到达该节点，它将被转换为边缘事件并保存到数据库中。" +
                "Node 不会直接将消息推送到边缘，而是将事件存储在边缘队列中。" +
                "<br>支持以下发起人类型：" +
                "<br><code>DEVICE</code>" +
                "<br><code>ASSET</code>" +
                "<br><code>ENTITY_VIEW</code>" +
                "<br><code>DASHBOARD</code>" +
                "<br><code>TENANT</code>" +
                "<br><code>CUSTOMER</code>" +
                "<br><code>EDGE</code><br><br>" +
                "节点也支持以下消息类型：" +
                "<br><code>POST_TELEMETRY_REQUEST</code>" +
                "<br><code>POST_ATTRIBUTES_REQUEST</code>" +
                "<br><code>ATTRIBUTES_UPDATED</code>" +
                "<br><code>ATTRIBUTES_DELETED</code>" +
                "<br><code>ALARM</code><br><br>" +
                "如果节点无法将边缘事件保存到数据库或到达不支持的发起者类型/消息类型，消息将通过<b>Failure</b>路由进行路由。" +
                "如果成功将边缘事件存储到数据库消息，将通过 <b>Success</b> 路由进行路由。",
        uiResources = {"static/rulenode/rulenode-core-config.js"},
        configDirective = "tbActionNodePushToEdgeConfig",
        icon = "cloud_download",
        ruleChainTypes = RuleChainType.CORE
)
public class TbMsgPushToEdgeNode extends AbstractTbMsgPushNode<TbMsgPushToEdgeNodeConfiguration, EdgeEvent, EdgeEventType> {

    static final int DEFAULT_PAGE_SIZE = 100;

    @Override
    EdgeEvent buildEvent(TenantId tenantId, EdgeEventActionType eventAction, UUID entityId,
                         EdgeEventType eventType, JsonNode entityBody) {
        EdgeEvent edgeEvent = new EdgeEvent();
        edgeEvent.setTenantId(tenantId);
        edgeEvent.setAction(eventAction);
        edgeEvent.setEntityId(entityId);
        edgeEvent.setType(eventType);
        edgeEvent.setBody(entityBody);
        return edgeEvent;
    }

    @Override
    EdgeEventType getEventTypeByEntityType(EntityType entityType) {
        return EdgeUtils.getEdgeEventTypeByEntityType(entityType);
    }

    @Override
    EdgeEventType getAlarmEventType() {
        return EdgeEventType.ALARM;
    }

    @Override
    String getIgnoredMessageSource() {
        return DataConstants.EDGE_MSG_SOURCE;
    }

    @Override
    protected Class<TbMsgPushToEdgeNodeConfiguration> getConfigClazz() {
        return TbMsgPushToEdgeNodeConfiguration.class;
    }

    @Override
    protected void processMsg(TbContext ctx, TbMsg msg) {
        try {
            if (EntityType.EDGE.equals(msg.getOriginator().getEntityType())) {
                EdgeEvent edgeEvent = buildEvent(msg, ctx);
                EdgeId edgeId = new EdgeId(msg.getOriginator().getId());
                ListenableFuture<Void> future = notifyEdge(ctx, edgeEvent, edgeId);
                FutureCallback<Void> futureCallback = new FutureCallback<>() {
                    @Override
                    public void onSuccess(@Nullable Void result) {
                        ctx.tellSuccess(msg);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        ctx.tellFailure(msg, t);
                    }
                };
                Futures.addCallback(future, futureCallback, ctx.getDbCallbackExecutor());
            } else {
                PageLink pageLink = new PageLink(DEFAULT_PAGE_SIZE);
                PageData<EdgeId> pageData;
                List<ListenableFuture<Void>> futures = new ArrayList<>();
                do {
                    pageData = ctx.getEdgeService().findRelatedEdgeIdsByEntityId(ctx.getTenantId(), msg.getOriginator(), pageLink);
                    if (pageData != null && pageData.getData() != null && !pageData.getData().isEmpty()) {
                        for (EdgeId edgeId : pageData.getData()) {
                            EdgeEvent edgeEvent = buildEvent(msg, ctx);
                            futures.add(notifyEdge(ctx, edgeEvent, edgeId));
                        }
                        if (pageData.hasNext()) {
                            pageLink = pageLink.nextPageLink();
                        }
                    }
                } while (pageData != null && pageData.hasNext());

                if (futures.isEmpty()) {
                    // ack in case no edges are related to provided entity
                    ctx.ack(msg);
                } else {
                    Futures.addCallback(Futures.allAsList(futures), new FutureCallback<>() {
                        @Override
                        public void onSuccess(@Nullable List<Void> voids) {
                            ctx.tellSuccess(msg);
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            ctx.tellFailure(msg, t);
                        }
                    }, ctx.getDbCallbackExecutor());
                }
            }
        } catch (Exception e) {
            log.error("Failed to build edge event", e);
            ctx.tellFailure(msg, e);
        }
    }

    private ListenableFuture<Void> notifyEdge(TbContext ctx, EdgeEvent edgeEvent, EdgeId edgeId) {
        edgeEvent.setEdgeId(edgeId);
        ListenableFuture<Void> future = ctx.getEdgeEventService().saveAsync(edgeEvent);
        return Futures.transform(future, result -> {
            ctx.onEdgeEventUpdate(ctx.getTenantId(), edgeId);
            return null;
        }, ctx.getDbCallbackExecutor());
    }

}
