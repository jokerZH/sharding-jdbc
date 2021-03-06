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

package com.dangdang.ddframe.rdb.sharding.jdbc;

import com.dangdang.ddframe.rdb.sharding.api.rule.ShardingRule;
import com.dangdang.ddframe.rdb.sharding.executor.ExecutorEngine;
import com.dangdang.ddframe.rdb.sharding.router.SQLRouteEngine;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/* 数据源运行期上下文, 处理一个sql需要用到的所有部件  */
@RequiredArgsConstructor
@Getter
public final class ShardingContext {
    private final ShardingRule shardingRule;        /* 逻辑db上的逻辑表对象 */
    private final SQLRouteEngine sqlRouteEngine;    /* sql路由器 */
    private final ExecutorEngine executorEngine;    /* 执行器 */
}
