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
package com.dangdang.ddframe.rdb.sharding.parser.result.router;

import java.util.Collection;
import java.util.LinkedHashSet;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/* SQL路由上下文, 从sql中解析出逻辑表名和sql中需要替换的地方 */
@Getter
@Setter
@ToString
@RequiredArgsConstructor
public final class RouteContext {
    private final Collection<Table> tables = new LinkedHashSet<>(); /* 逻辑sql中的逻辑表名 */
    private SQLStatementType sqlStatementType;  /* sql类型 */
    private SQLBuilder sqlBuilder;              /* sql模板, 用于产生最终的sql语句 */
}
