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

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.rule.engine.api.RuleNode;
import org.thingsboard.rule.engine.api.TbContext;
import org.thingsboard.rule.engine.api.TbNode;
import org.thingsboard.rule.engine.api.TbNodeConfiguration;
import org.thingsboard.rule.engine.api.TbNodeException;
import org.thingsboard.rule.engine.api.util.TbNodeUtils;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.data.rule.RuleChainType;
import org.thingsboard.server.common.msg.TbMsg;
import org.thingsboard.server.dao.cassandra.CassandraCluster;
import org.thingsboard.server.dao.cassandra.guava.GuavaSession;
import org.thingsboard.server.dao.nosql.CassandraStatementTask;
import org.thingsboard.server.dao.nosql.TbResultSetFuture;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.thingsboard.common.util.DonAsynchron.withCallback;

@Slf4j
@RuleNode(type = ComponentType.ACTION,
        name = "保存到自定义表",
        configClazz = TbSaveToCustomCassandraTableNodeConfiguration.class,
        nodeDescription = "节点将来自传入消息负载的数据存储到 Cassandra 数据库到预定义的自定义表中" +
                "应该有 <b>cs_tb_</b> 前缀，以避免数据插入到常见的 TB 表。<br>" +
                "<b>注意:</b>规则节点只能用于 Cassandra DB。",
        nodeDetails = "管理员应设置不带前缀的自定义表名称：<b>cs_tb_</b>。 <br>" +
                "管理员可以配置消息字段名称和表列名称之间的映射。<br>" +
                "<b>注意:</b>如果映射键是 <b>$entity_id</b>，由消息发起者标识，则将消息发起者 ID 写入适当的列名（映射值）。<br><br>" +
                "如果指定的消息字段不存在或不是 JSON 原语，则出站消息将通过 <b>failure</b> 链路由，" +
                "否则，消息将通过 <b>success</b> 链进行路由。",
        uiResources = {"static/rulenode/rulenode-core-config.js"},
        configDirective = "tbActionNodeCustomTableConfig",
        icon = "file_upload",
        ruleChainTypes = RuleChainType.CORE)
public class TbSaveToCustomCassandraTableNode implements TbNode {

    private static final String TABLE_PREFIX = "cs_tb_";
    private static final JsonParser parser = new JsonParser();
    private static final String ENTITY_ID = "$entityId";

    private TbSaveToCustomCassandraTableNodeConfiguration config;
    private GuavaSession session;
    private CassandraCluster cassandraCluster;
    private ConsistencyLevel defaultWriteLevel;
    private PreparedStatement saveStmt;
    private ExecutorService readResultsProcessingExecutor;
    private Map<String, String> fieldsMap;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        config = TbNodeUtils.convert(configuration, TbSaveToCustomCassandraTableNodeConfiguration.class);
        cassandraCluster = ctx.getCassandraCluster();
        if (cassandraCluster == null) {
            throw new RuntimeException("Unable to connect to Cassandra database");
        } else {
            startExecutor();
            saveStmt = getSaveStmt();
        }
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) {
        withCallback(save(msg, ctx), aVoid -> ctx.tellSuccess(msg), e -> ctx.tellFailure(msg, e), ctx.getDbCallbackExecutor());
    }

    @Override
    public void destroy() {
        stopExecutor();
        saveStmt = null;
    }

    private void startExecutor() {
        readResultsProcessingExecutor = Executors.newCachedThreadPool();
    }

    private void stopExecutor() {
        if (readResultsProcessingExecutor != null) {
            readResultsProcessingExecutor.shutdownNow();
        }
    }

    private PreparedStatement prepare(String query) {
        return getSession().prepare(query);
    }

    private GuavaSession getSession() {
        if (session == null) {
            session = cassandraCluster.getSession();
            defaultWriteLevel = cassandraCluster.getDefaultWriteConsistencyLevel();
        }
        return session;
    }

    private PreparedStatement getSaveStmt() {
        fieldsMap = config.getFieldsMapping();
        if (fieldsMap.isEmpty()) {
            throw new RuntimeException("Fields(key,value) map is empty!");
        } else {
            return prepareStatement(new ArrayList<>(fieldsMap.values()));
        }
    }

    private PreparedStatement prepareStatement(List<String> fieldsList) {
        return prepare(createQuery(fieldsList));
    }

    private String createQuery(List<String> fieldsList) {
        int size = fieldsList.size();
        StringBuilder query = new StringBuilder();
        query.append("INSERT INTO ")
                .append(TABLE_PREFIX)
                .append(config.getTableName())
                .append("(");
        for (String field : fieldsList) {
            query.append(field);
            if (fieldsList.get(size - 1).equals(field)) {
                query.append(")");
            } else {
                query.append(",");
            }
        }
        query.append(" VALUES(");
        for (int i = 0; i < size; i++) {
            if (i == size - 1) {
                query.append("?)");
            } else {
                query.append("?, ");
            }
        }
        return query.toString();
    }

    private ListenableFuture<Void> save(TbMsg msg, TbContext ctx) {
        JsonElement data = parser.parse(msg.getData());
        if (!data.isJsonObject()) {
            throw new IllegalStateException("Invalid message structure, it is not a JSON Object:" + data);
        } else {
            JsonObject dataAsObject = data.getAsJsonObject();
            BoundStatementBuilder stmtBuilder = new BoundStatementBuilder(saveStmt.bind());
            AtomicInteger i = new AtomicInteger(0);
            fieldsMap.forEach((key, value) -> {
                if (key.equals(ENTITY_ID)) {
                    stmtBuilder.setUuid(i.get(), msg.getOriginator().getId());
                } else if (dataAsObject.has(key)) {
                    JsonElement dataKeyElement = dataAsObject.get(key);
                    if (dataKeyElement.isJsonPrimitive()) {
                        JsonPrimitive primitive = dataKeyElement.getAsJsonPrimitive();
                        if (primitive.isNumber()) {
                            if (primitive.getAsString().contains(".")) {
                                stmtBuilder.setDouble(i.get(), primitive.getAsDouble());
                            } else {
                                stmtBuilder.setLong(i.get(), primitive.getAsLong());
                            }
                        } else if (primitive.isBoolean()) {
                            stmtBuilder.setBoolean(i.get(), primitive.getAsBoolean());
                        } else if (primitive.isString()) {
                            stmtBuilder.setString(i.get(), primitive.getAsString());
                        } else {
                            stmtBuilder.setToNull(i.get());
                        }
                    } else if (dataKeyElement.isJsonObject()) {
                        stmtBuilder.setString(i.get(), dataKeyElement.getAsJsonObject().toString());
                    } else {
                        throw new IllegalStateException("Message data key: '" + key + "' with value: '" + value + "' is not a JSON Object or JSON Primitive!");
                    }
                } else {
                    throw new RuntimeException("Message data doesn't contain key: " + "'" + key + "'!");
                }
                i.getAndIncrement();
            });
            return getFuture(executeAsyncWrite(ctx, stmtBuilder.build()), rs -> null);
        }
    }

    private TbResultSetFuture executeAsyncWrite(TbContext ctx, Statement statement) {
        return executeAsync(ctx, statement, defaultWriteLevel);
    }

    private TbResultSetFuture executeAsync(TbContext ctx, Statement statement, ConsistencyLevel level) {
        if (log.isDebugEnabled()) {
            log.debug("Execute cassandra async statement {}", statementToString(statement));
        }
        if (statement.getConsistencyLevel() == null) {
            statement.setConsistencyLevel(level);
        }
        return ctx.submitCassandraWriteTask(new CassandraStatementTask(ctx.getTenantId(), getSession(), statement));
    }

    private static String statementToString(Statement statement) {
        if (statement instanceof BoundStatement) {
            return ((BoundStatement) statement).getPreparedStatement().getQuery();
        } else {
            return statement.toString();
        }
    }

    private <T> ListenableFuture<T> getFuture(TbResultSetFuture future, java.util.function.Function<AsyncResultSet, T> transformer) {
        return Futures.transform(future, new Function<AsyncResultSet, T>() {
            @Nullable
            @Override
            public T apply(@Nullable AsyncResultSet input) {
                return transformer.apply(input);
            }
        }, readResultsProcessingExecutor);
    }

}
