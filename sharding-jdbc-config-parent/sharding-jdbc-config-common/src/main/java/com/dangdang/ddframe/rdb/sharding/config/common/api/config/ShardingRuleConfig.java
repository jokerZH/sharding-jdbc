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
package com.dangdang.ddframe.rdb.sharding.config.common.api.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

import lombok.Getter;
import lombok.Setter;

/* 分片规则配置, 对一个逻辑DB的配置 */
@Getter
@Setter
public class ShardingRuleConfig {
    /* mysql数据源 */
    private Map<String/*datasourceName*/, DataSource> dataSource = new HashMap<>();

    /* 默认db的名字 */
    private String defaultDataSourceName;

    /* 逻辑表到规则的映射 */
    private Map<String/*logicTableName*/, TableRuleConfig> tables = new HashMap<>();

    /* inner join的时候会用到 */
    private List<BindingTableRuleConfig> bindingTables = new ArrayList<>();

    /* 默认的分库算法 */
    private StrategyConfig defaultDatabaseStrategy;

    /* 默认的分表算法 */
    private StrategyConfig defaultTableStrategy;
}
