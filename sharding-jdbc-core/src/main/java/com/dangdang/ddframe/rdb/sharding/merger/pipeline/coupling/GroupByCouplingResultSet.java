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
package com.dangdang.ddframe.rdb.sharding.merger.pipeline.coupling;

import com.dangdang.ddframe.rdb.sharding.merger.ResultSetMergeContext;
import com.dangdang.ddframe.rdb.sharding.merger.resultset.memory.AbstractMemoryResultSet;
import com.dangdang.ddframe.rdb.sharding.merger.resultset.memory.row.GroupByResultSetRow;
import com.dangdang.ddframe.rdb.sharding.merger.resultset.memory.row.ResultSetRow;
import com.dangdang.ddframe.rdb.sharding.parser.result.merger.AggregationColumn;
import com.dangdang.ddframe.rdb.sharding.parser.result.merger.GroupByColumn;
import com.google.common.base.Optional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

/* 分组的连接结果集 groupBy + 聚合函数 */
public final class GroupByCouplingResultSet extends AbstractMemoryResultSet {
    private final List<GroupByColumn> groupByColumns;
    private final List<AggregationColumn> aggregationColumns;
    
    private ResultSet resultSet;    /* resultSet就一个 */

    private boolean hasNext;
    
    public GroupByCouplingResultSet(final ResultSet resultSet, final ResultSetMergeContext resultSetMergeContext) throws SQLException {
        super(Collections.singletonList(resultSet));
        groupByColumns = resultSetMergeContext.getMergeContext().getGroupByColumns();
        aggregationColumns = resultSetMergeContext.getMergeContext().getAggregationColumns();
    }
    
    @Override
    protected void initRows(final List<ResultSet> resultSets) throws SQLException {
        resultSet = resultSets.get(0);
        hasNext = resultSet.next();
    }

    /* FIXME what's the fuck */
    @Override
    protected Optional<? extends ResultSetRow> nextRow() throws SQLException {
        if (!hasNext) {
            return Optional.absent();
        }

        // 处理一个分组的所有数据对应聚合函数相关的操作 跨过同组的分组字段相同的row FIXME 会有问题吧
        GroupByResultSetRow result = new GroupByResultSetRow(resultSet, groupByColumns, aggregationColumns);
        List<Object> groupByValues = result.getGroupByValues();
        while (hasNext && (groupByColumns.isEmpty() || groupByValues.equals(result.getGroupByValues()))) {
            result.aggregate();
            hasNext = resultSet.next();
        }
        result.generateResult();
        return Optional.of(result);
    }
}
