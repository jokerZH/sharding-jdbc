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
package com.dangdang.ddframe.rdb.sharding.merger;

import com.dangdang.ddframe.rdb.sharding.merger.pipeline.coupling.GroupByCouplingResultSet;
import com.dangdang.ddframe.rdb.sharding.merger.pipeline.coupling.LimitCouplingResultSet;
import com.dangdang.ddframe.rdb.sharding.merger.pipeline.coupling.MemoryOrderByCouplingResultSet;
import com.dangdang.ddframe.rdb.sharding.merger.pipeline.reducer.IteratorReducerResultSet;
import com.dangdang.ddframe.rdb.sharding.merger.pipeline.reducer.MemoryOrderByReducerResultSet;
import com.dangdang.ddframe.rdb.sharding.merger.pipeline.reducer.StreamingOrderByReducerResultSet;
import com.dangdang.ddframe.rdb.sharding.parser.result.merger.MergeContext;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/* 分片结果集归并工厂 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public final class ResultSetFactory {
    /* 获取结果集, 考虑聚合运算 */
    public static ResultSet/*结果集包装*/ getResultSet(final List<ResultSet> resultSets/*结果集列表*/, final MergeContext mergeContext/*结果归并上下文*/) throws SQLException {
        ShardingResultSets shardingResultSets = new ShardingResultSets(resultSets);
        log.debug("Sharding-JDBC: Sharding result sets type is '{}'", shardingResultSets.getType().toString());
        switch (shardingResultSets.getType()) {
            case EMPTY:
                return buildEmpty(resultSets);
            case SINGLE:
                return buildSingle(shardingResultSets);
            case MULTIPLE:
                return buildMultiple(shardingResultSets, mergeContext);
            default:
                throw new UnsupportedOperationException(shardingResultSets.getType().toString());
        }
    }
    
    private static ResultSet buildEmpty(final List<ResultSet> resultSets) {
        return resultSets.get(0);
    }
    
    private static ResultSet buildSingle(final ShardingResultSets shardingResultSets) throws SQLException {
        return shardingResultSets.getResultSets().get(0);
    }

    /* 多个resultSet的情况下 */
    private static ResultSet buildMultiple(final ShardingResultSets shardingResultSets, final MergeContext mergeContext) throws SQLException {
        ResultSetMergeContext resultSetMergeContext = new ResultSetMergeContext(shardingResultSets, mergeContext);
        return buildCoupling(buildReducer(resultSetMergeContext), resultSetMergeContext);
    }

    /* 处理多个resultSet的情况,根据orderBy和groupBy返回对应的结果, 返回对应的resultSet对象 */
    private static ResultSet buildReducer(final ResultSetMergeContext resultSetMergeContext) throws SQLException {
        if (resultSetMergeContext.isNeedMemorySortForGroupBy()) {
            resultSetMergeContext.setGroupByKeysToCurrentOrderByKeys();
            return new MemoryOrderByReducerResultSet(resultSetMergeContext);
        }
        if (resultSetMergeContext.getMergeContext().hasGroupBy() || resultSetMergeContext.getMergeContext().hasOrderBy()) {
            return new StreamingOrderByReducerResultSet(resultSetMergeContext);
        }
        return new IteratorReducerResultSet(resultSetMergeContext);
    }

    /* 在处理排序的基础上, 处理聚合函数 */
    private static ResultSet buildCoupling(final ResultSet resultSet, final ResultSetMergeContext resultSetMergeContext) throws SQLException {
        ResultSet result = resultSet;
        if (resultSetMergeContext.getMergeContext().hasGroupByOrAggregation()) {
            result = new GroupByCouplingResultSet(result, resultSetMergeContext);
        }

        if (resultSetMergeContext.isNeedMemorySortForOrderBy()) {
            resultSetMergeContext.setOrderByKeysToCurrentOrderByKeys();
            result = new MemoryOrderByCouplingResultSet(result, resultSetMergeContext);
        }

        if (resultSetMergeContext.getMergeContext().hasLimit()) {
            result = new LimitCouplingResultSet(result, resultSetMergeContext.getMergeContext());
        }

        return result;
    }
}
