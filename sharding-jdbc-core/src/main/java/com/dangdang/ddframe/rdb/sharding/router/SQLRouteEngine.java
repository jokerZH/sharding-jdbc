/*
 * Copyright 1999-2015 dangdang.com.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */
package com.dangdang.ddframe.rdb.sharding.router;

import com.codahale.metrics.Timer.Context;
import com.dangdang.ddframe.rdb.sharding.api.rule.ShardingRule;
import com.dangdang.ddframe.rdb.sharding.constants.DatabaseType;
import com.dangdang.ddframe.rdb.sharding.exception.SQLParserException;
import com.dangdang.ddframe.rdb.sharding.exception.ShardingJdbcException;
import com.dangdang.ddframe.rdb.sharding.metrics.MetricsContext;
import com.dangdang.ddframe.rdb.sharding.parser.SQLParserFactory;
import com.dangdang.ddframe.rdb.sharding.parser.result.SQLParsedResult;
import com.dangdang.ddframe.rdb.sharding.parser.result.merger.Limit;
import com.dangdang.ddframe.rdb.sharding.parser.result.router.ConditionContext;
import com.dangdang.ddframe.rdb.sharding.parser.result.router.SQLBuilder;
import com.dangdang.ddframe.rdb.sharding.parser.result.router.SQLStatementType;
import com.dangdang.ddframe.rdb.sharding.parser.result.router.Table;
import com.dangdang.ddframe.rdb.sharding.router.binding.BindingTablesRouter;
import com.dangdang.ddframe.rdb.sharding.router.mixed.MixedTablesRouter;
import com.dangdang.ddframe.rdb.sharding.router.single.SingleTableRouter;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Sets;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/* SQL路由引擎 */
@RequiredArgsConstructor
@Slf4j
public final class SQLRouteEngine {
    private final ShardingRule shardingRule;
    private final DatabaseType databaseType;

    /* 预解析SQL路由 */
    public PreparedSQLRouter/*预解析SQL路由器*/ prepareSQL(final String logicSql/*逻辑SQL*/) { return new PreparedSQLRouter(logicSql, this); }

    /* SQL路由 */
    public SQLRouteResult/*路由结果*/ route(final String logicSql/*逻辑SQL*/) throws SQLParserException { return route(logicSql, Collections.emptyList()); }
    public SQLRouteResult/*路由结果*/ route(final String logicSql/*逻辑SQL*/, final List<Object> parameters/*参数列表*/) throws SQLParserException { return routeSQL(parseSQL(logicSql, parameters), parameters); }
    /* 解析sql语句 */
    SQLParsedResult parseSQL(final String logicSql, final List<Object> parameters) {
        Context context = MetricsContext.start("Parse SQL");
        SQLParsedResult result = SQLParserFactory.create(databaseType, logicSql, parameters, shardingRule.getAllShardingColumns()).parse();
        MetricsContext.stop(context);
        return result;
    }

    /* 路由的核心功能 */
    SQLRouteResult routeSQL(final SQLParsedResult parsedResult, final List<Object> parameters) {
        Context context = MetricsContext.start("Route SQL");
        SQLRouteResult result = new SQLRouteResult(parsedResult.getRouteContext().getSqlStatementType(), parsedResult.getMergeContext());
        // 获得执行单元
        for (ConditionContext each : parsedResult.getConditionContexts()) {
            result.getExecutionUnits().addAll(
                    routeSQL(
                            each,
                            Sets.newLinkedHashSet(
                                    Collections2.transform(
                                            parsedResult.getRouteContext().getTables(),
                                            new Function<Table, String>() {
                                                @Override
                                                public String apply(final Table input) {
                                                    return input.getName();
                                                }
                                            }
                                    )
                            ),
                            parsedResult.getRouteContext().getSqlBuilder(),
                            parsedResult.getRouteContext().getSqlStatementType())
            );
        }
        processLimit(result.getExecutionUnits(), parsedResult, parameters);
        MetricsContext.stop(context);
        if (result.getExecutionUnits().isEmpty()) {
            throw new ShardingJdbcException("Sharding-JDBC: cannot route any result, please check your sharding rule.");
        }
        log.debug("final route result:{}", result.getExecutionUnits());
        log.debug("merge context:{}", result.getMergeContext());
        return result;
    }
    
    private Collection<SQLExecutionUnit> routeSQL(
            final ConditionContext conditionContext,    /* 计算表达式 */
            final Set<String> logicTables,              /* 逻辑表名 */
            final SQLBuilder sqlBuilder,                /* sql语句创建器 */
            final SQLStatementType type                 /* sql语句类型 */
    ) {
        RoutingResult result;
        if (1 == logicTables.size()) {
            result = new SingleTableRouter(shardingRule, logicTables.iterator().next(), conditionContext, type).route();
        } else if (shardingRule.isAllBindingTables(logicTables)) {
            /* TODO */
            result = new BindingTablesRouter(shardingRule, logicTables, conditionContext, type).route();
        } else {
            /* TODO */
            // TODO 可配置是否执行笛卡尔积
            result = new MixedTablesRouter(shardingRule, logicTables, conditionContext, type).route();
        }
        return result.getSQLExecutionUnits(sqlBuilder);
    }

    /* 处理limit的情况, 修改limit, 如果是单个sql就不变,如果是多个,则改成 0, offset+limit */
    private void processLimit(final Set<SQLExecutionUnit> sqlExecutionUnits, final SQLParsedResult parsedResult, final List<Object> parameters) {
        if (!parsedResult.getMergeContext().hasLimit()) {
            return;
        }
        int offset;
        int rowCount;
        Limit limit = parsedResult.getMergeContext().getLimit();
        if (sqlExecutionUnits.size() > 1) {
            offset = 0;
            rowCount = limit.getOffset() + limit.getRowCount();
        } else {
            offset = limit.getOffset();
            rowCount = limit.getRowCount();
        }

        // sql中写明的情况
        if (parsedResult.getRouteContext().getSqlBuilder().containsToken(Limit.OFFSET_NAME) || parsedResult.getRouteContext().getSqlBuilder().containsToken(Limit.COUNT_NAME)) {
            for (SQLExecutionUnit each : sqlExecutionUnits) {
                SQLBuilder sqlBuilder = each.getSqlBuilder();
                sqlBuilder.buildSQL(Limit.OFFSET_NAME, String.valueOf(offset));
                sqlBuilder.buildSQL(Limit.COUNT_NAME, String.valueOf(rowCount));
                each.setSql(sqlBuilder.toSQL());
            }
        }

        // ?的情况
        if (limit.getOffsetParameterIndex().isPresent()) {
            parameters.set(limit.getOffsetParameterIndex().get(), offset);
        }
        if (limit.getRowCountParameterIndex().isPresent()) {
            parameters.set(limit.getRowCountParameterIndex().get(), rowCount);
        }
    }
}
