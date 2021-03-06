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

package com.dangdang.ddframe.rdb.sharding.api.rule;

import com.dangdang.ddframe.rdb.sharding.api.strategy.database.DatabaseShardingStrategy;
import com.dangdang.ddframe.rdb.sharding.api.strategy.table.TableShardingStrategy;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

/* 表规则配置对象, 对应一个逻辑表 */
@Getter
@ToString
public final class TableRule {
    private final String logicTable;        // 逻辑表名
    private final boolean dynamic;          // 物理db和物理表计算的得到
    private final List<DataNode> actualTables;  // 物理表
    private final DatabaseShardingStrategy databaseShardingStrategy;    // db算法
    private final TableShardingStrategy tableShardingStrategy;          // 物理表算法
    
    /* 全属性构造器 */
    @Deprecated
    public TableRule(
            final String logicTable,            // 逻辑表名
            final boolean dynamic,              // TODO
            final List<String> actualTables,    // 每个db中的物理表名列表,且各个db的物理表名是相同的
            final DataSourceRule dataSourceRule,        // datasource集合
            final Collection<String> dataSourceNames,   // 当前逻辑表用到的datasourceName
            final DatabaseShardingStrategy databaseShardingStrategy,    // 策略
            final TableShardingStrategy tableShardingStrategy           // 策略
    ) {
        Preconditions.checkNotNull(logicTable);
        this.logicTable = logicTable;
        this.dynamic = dynamic;
        this.databaseShardingStrategy = databaseShardingStrategy;
        this.tableShardingStrategy = tableShardingStrategy;
        if (dynamic) {
            Preconditions.checkNotNull(dataSourceRule);
            this.actualTables = generateDataNodes(dataSourceRule);
        } else if (null == actualTables || actualTables.isEmpty()) {
            Preconditions.checkNotNull(dataSourceRule);
            this.actualTables = generateDataNodes(Collections.singletonList(logicTable), dataSourceRule, dataSourceNames);
        } else {
            this.actualTables = generateDataNodes(actualTables, dataSourceRule, dataSourceNames);
        }
    }
    

    /* 将dataSourceRule中的dbName和默认的动态物理表名构建actualTables */
    private List<DataNode> generateDataNodes(final DataSourceRule dataSourceRule) {
        Collection<String> dataSourceNames = dataSourceRule.getDataSourceNames();
        List<DataNode> result = new ArrayList<>(dataSourceNames.size());
        for (String each : dataSourceNames) {
            result.add(new DynamicDataNode(each));
        }
        return result;
    }

    /* 获得actualTables  */
    private List<DataNode> generateDataNodes(final List<String> actualTables, final DataSourceRule dataSourceRule, final Collection<String> actualDataSourceNames) {
        Collection<String> dataSourceNames = getDataSourceNames(dataSourceRule, actualDataSourceNames);
        List<DataNode> result = new ArrayList<>(actualTables.size() * (dataSourceNames.isEmpty() ? 1 : dataSourceNames.size()));
        for (String actualTable : actualTables) {
            if (DataNode.isValidDataNode(actualTable)) {
                result.add(new DataNode(actualTable));
            } else {
                for (String dataSourceName : dataSourceNames) {
                    result.add(new DataNode(dataSourceName, actualTable));
                }
            }
        }
        return result;
    }

    /* 处理参数为null或者空的情况 */
    private Collection<String> getDataSourceNames(final DataSourceRule dataSourceRule, final Collection<String> actualDataSourceNames) {
        if (null == dataSourceRule) {
            return Collections.emptyList();
        }
        if (null == actualDataSourceNames || actualDataSourceNames.isEmpty()) {
            return dataSourceRule.getDataSourceNames();
        }
        return actualDataSourceNames;
    }
    
    /* 根据数据源名称过滤获取真实数据单元 */
    public Collection<DataNode> getActualDataNodes(final Collection<String> targetDataSources, final Collection<String> targetTables) {
        return dynamic ? getDynamicDataNodes(targetDataSources, targetTables) : getStaticDataNodes(targetDataSources, targetTables);
    }

    /* 根据参数返回结果信息 */
    private Collection<DataNode> getDynamicDataNodes(final Collection<String> targetDataSources, final Collection<String> targetTables) {
        Collection<DataNode> result = new LinkedHashSet<>(targetDataSources.size() * targetTables.size());
        for (String targetDataSource : targetDataSources) {
            for (String targetTable : targetTables) {
                result.add(new DataNode(targetDataSource, targetTable));
            }
        }
        return result;
    }

    /* 返回actualTables在参数中存在的数据 */
    private Collection<DataNode> getStaticDataNodes(final Collection<String> targetDataSources, final Collection<String> targetTables) {
        Collection<DataNode> result = new LinkedHashSet<>(actualTables.size());
        for (DataNode each : actualTables) {
            if (targetDataSources.contains(each.getDataSourceName()) && targetTables.contains(each.getTableName())) {
                result.add(each);
            }
        }
        return result;
    }
    
    
    /* 获取真实数据源 */
    public Collection<String> getActualDatasourceNames() {
        Collection<String> result = new LinkedHashSet<>(actualTables.size());
        for (DataNode each : actualTables) {
            result.add(each.getDataSourceName());
        }
        return result;
    }
    
    /* 根据数据源名称过滤获取真实表名称 */
    public Collection<String> getActualTableNames(final Collection<String> targetDataSources) {
        Collection<String> result = new LinkedHashSet<>(actualTables.size());
        for (DataNode each : actualTables) {
            if (targetDataSources.contains(each.getDataSourceName())) {
                result.add(each.getTableName());
            }
        }
        return result;
    }

    /* 返回物理表的下标 */
    int findActualTableIndex(final String dataSourceName, final String actualTableName) {
        int result = 0;
        for (DataNode each : actualTables) {
            if (each.getDataSourceName().equals(dataSourceName) && each.getTableName().equals(actualTableName)) {
                return result;
            }
            result++;
        }
        return -1;
    }


    /**
     * 获取表规则配置对象构建器.
     *
     * @param logicTable 逻辑表名称
     * @return 表规则配置对象构建器
     */
    public static TableRuleBuilder builder(final String logicTable) {
        return new TableRuleBuilder(logicTable);
    }

    /* 表规则配置对象构建器, 对应一个逻辑表 */
    @RequiredArgsConstructor
    public static class TableRuleBuilder {
        
        private final String logicTable;
        
        private boolean dynamic;
        
        private List<String> actualTables;
        
        private DataSourceRule dataSourceRule;
        
        private Collection<String> dataSourceNames;
        
        private DatabaseShardingStrategy databaseShardingStrategy;
        
        private TableShardingStrategy tableShardingStrategy;
        
        /**
         * 构建是否为动态表.
         *
         * @param dynamic 是否为动态表
         * @return 真实表集合
         */
        public TableRuleBuilder dynamic(final boolean dynamic) {
            this.dynamic = dynamic;
            return this;
        }
        
        /**
         * 构建真实表集合.
         *
         * @param actualTables 真实表集合
         * @return 真实表集合
         */
        public TableRuleBuilder actualTables(final List<String> actualTables) {
            this.actualTables = actualTables;
            return this;
        }
        
        /**
         * 构建数据源分片规则.
         *
         * @param dataSourceRule 数据源分片规则
         * @return 规则配置对象构建器
         */
        public TableRuleBuilder dataSourceRule(final DataSourceRule dataSourceRule) {
            this.dataSourceRule = dataSourceRule;
            return this;
        }
        
        /**
         * 构建数据源分片规则.
         *
         * @param dataSourceNames 数据源名称集合
         * @return 规则配置对象构建器
         */
        public TableRuleBuilder dataSourceNames(final Collection<String> dataSourceNames) {
            this.dataSourceNames = dataSourceNames;
            return this;
        }
        
        /**
         * 构建数据库分片策略.
         *
         * @param databaseShardingStrategy 数据库分片策略
         * @return 规则配置对象构建器
         */
        public TableRuleBuilder databaseShardingStrategy(final DatabaseShardingStrategy databaseShardingStrategy) {
            this.databaseShardingStrategy = databaseShardingStrategy;
            return this;
        }
        
        /**
         * 构建表分片策略.
         *
         * @param tableShardingStrategy 表分片策略
         * @return 规则配置对象构建器
         */
        public TableRuleBuilder tableShardingStrategy(final TableShardingStrategy tableShardingStrategy) {
            this.tableShardingStrategy = tableShardingStrategy;
            return this;
        }
        
        /**
         * 构建表规则配置对象.
         *
         * @return 表规则配置对象
         */
        public TableRule build() {
            return new TableRule(logicTable, dynamic, actualTables, dataSourceRule, dataSourceNames, databaseShardingStrategy, tableShardingStrategy);
        }
    }
}
