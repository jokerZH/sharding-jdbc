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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.List;

/* Binding表规则配置对象, 用于管理多个逻辑表,保证数据分布是相同的 */
@RequiredArgsConstructor
@Getter
public final class BindingTableRule {
    private final List<TableRule> tableRules;
    
    /* 判断此绑定表规则是否包含该逻辑表 */
    public boolean hasLogicTable(final String logicTableName) {
        for (TableRule each : tableRules) {
            if (each.getLogicTable().equals(logicTableName)) {
                return true;
            }
        }
        return false;
    }
    
    /* 根绝一个表的物理表名找到另外一个表中和这个物理表名下表一样的表, 在inner join中会用到 */
    public String getBindingActualTable(final String dataSource, final String logicTable, final String otherActualTable) {
        int index = -1;
        for (TableRule each : tableRules) {
            if (each.isDynamic()) {
                throw new UnsupportedOperationException("Dynamic table cannot support Binding table.");
            }
            index = each.findActualTableIndex(dataSource, otherActualTable);
            if (-1 != index) {
                break;
            }
        }
        Preconditions.checkState(-1 != index, String.format("Actual table [%s].[%s] is not in table config", dataSource, otherActualTable));
        for (TableRule each : tableRules) {
            if (each.getLogicTable().equals(logicTable)) {
                return each.getActualTables().get(index).getTableName();
            }
        }
        throw new IllegalStateException(String.format("Cannot find binding actual table, data source: %s, logic table: %s, other actual table: %s", dataSource, logicTable, otherActualTable));
    }
    
    Collection<String> getAllLogicTables() {
        return Lists.transform(tableRules, new Function<TableRule, String>() {
            
            @Override
            public String apply(final TableRule input) {
                return input.getLogicTable();
            }
        });
    }
}
