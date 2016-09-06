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
package com.dangdang.ddframe.rdb.sharding.router.strategy;

import com.dangdang.ddframe.rdb.sharding.api.ShardingValue;
import com.dangdang.ddframe.rdb.sharding.parser.result.router.SQLStatementType;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.Collections;

/* 分片策略 */
@RequiredArgsConstructor
public class ShardingStrategy {
    @Getter
    private final Collection<String> shardingColumns;   /* 分表字段集合 */
    private final ShardingAlgorithm shardingAlgorithm;  /* 算法 */
    
    public ShardingStrategy(final String shardingColumn, final ShardingAlgorithm shardingAlgorithm) { this(Collections.singletonList(shardingColumn), shardingAlgorithm); }
    
    /* 计算静态分片 */
    public Collection<String>/*分库后指向的数据源名称集合*/ doStaticSharding(
            final SQLStatementType sqlStatementType,            // SQL语句的类型
            final Collection<String> availableTargetNames,      // 所有的可用分片资源集合
            final Collection<ShardingValue<?>> shardingValues   // 分片字段值集合
    ) {
        if (shardingValues.isEmpty()) {
            Preconditions.checkState(!isInsertMultiple(sqlStatementType, availableTargetNames), "INSERT statement should contain sharding value.");
            return availableTargetNames;
        }
        return doSharding(shardingValues, availableTargetNames);
    }
    
    /* 计算动态分片 */
    public Collection<String> doDynamicSharding(final Collection<ShardingValue<?>> shardingValues) {
        Preconditions.checkState(!shardingValues.isEmpty(), "Dynamic table should contain sharding value.");
        Collection<String> availableTargetNames = Collections.emptyList();
        return doSharding(shardingValues, availableTargetNames);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private Collection<String> doSharding(final Collection<ShardingValue<?>> shardingValues, final Collection<String> availableTargetNames) {
        if (shardingAlgorithm instanceof SingleKeyShardingAlgorithm) {
            SingleKeyShardingAlgorithm<?> singleKeyShardingAlgorithm = (SingleKeyShardingAlgorithm<?>) shardingAlgorithm;
            ShardingValue shardingValue = shardingValues.iterator().next();
            // FIXME 特么就算第一个么
            switch (shardingValue.getType()) {
                case SINGLE:
                    return Collections.singletonList(singleKeyShardingAlgorithm.doEqualSharding(availableTargetNames, shardingValue));
                case LIST:
                    return singleKeyShardingAlgorithm.doInSharding(availableTargetNames, shardingValue);
                case RANGE:
                    return singleKeyShardingAlgorithm.doBetweenSharding(availableTargetNames, shardingValue);
                default:
                    throw new UnsupportedOperationException(shardingValue.getType().getClass().getName());
            }
        }
        if (shardingAlgorithm instanceof MultipleKeysShardingAlgorithm) {
            return ((MultipleKeysShardingAlgorithm) shardingAlgorithm).doSharding(availableTargetNames, shardingValues);
        }
        throw new UnsupportedOperationException(shardingAlgorithm.getClass().getName());
    }

    /* 是否是多值得insert */
    private boolean isInsertMultiple(final SQLStatementType sqlStatementType, final Collection<String> availableTargetNames) {
        return SQLStatementType.INSERT.equals(sqlStatementType) && availableTargetNames.size() > 1;
    }
}
