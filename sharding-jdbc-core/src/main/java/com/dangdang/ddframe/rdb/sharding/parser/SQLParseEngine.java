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
package com.dangdang.ddframe.rdb.sharding.parser;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.visitor.SQLASTOutputVisitor;
import com.dangdang.ddframe.rdb.sharding.exception.SQLParserException;
import com.dangdang.ddframe.rdb.sharding.parser.result.SQLParsedResult;
import com.dangdang.ddframe.rdb.sharding.parser.result.router.SQLStatementType;
import com.dangdang.ddframe.rdb.sharding.parser.visitor.SQLVisitor;
import com.dangdang.ddframe.rdb.sharding.parser.visitor.or.OrParser;
import com.google.common.base.Preconditions;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.List;

/* 不包含OR语句的SQL构建器解析 */
@RequiredArgsConstructor
@Slf4j
public final class SQLParseEngine {
    private final SQLStatement sqlStatement;    /* 逻辑sql语句 */
    private final List<Object> parameters;      /* 参数 */
    private final SQLASTOutputVisitor visitor;  /* 获得sql信息,修改sql语句 */
    private final Collection<String> shardingColumns;   /* 当前逻辑db的分表字段集合 */
    
    /*  解析SQL */
    public SQLParsedResult parse() {
        Preconditions.checkArgument(visitor instanceof SQLVisitor);

        SQLVisitor sqlVisitor = (SQLVisitor) visitor;
        visitor.setParameters(parameters);
        sqlVisitor.getParseContext().setShardingColumns(shardingColumns);
        sqlStatement.accept(visitor);

        SQLParsedResult result = sqlVisitor.getParseContext().getParsedResult();
        if (sqlVisitor.getParseContext().isHasOrCondition()) {
            // 如果有or操作 特么and呢
            new OrParser(sqlStatement, visitor).fillConditionContext(result);
        } 
        sqlVisitor.getParseContext().mergeCurrentConditionContext();
        log.debug("Parsed SQL result: {}", result);
        log.debug("Parsed SQL: {}", sqlVisitor.getSQLBuilder());
        result.getRouteContext().setSqlBuilder(sqlVisitor.getSQLBuilder());
        result.getRouteContext().setSqlStatementType(getType());
        return result;
    }
    
    private SQLStatementType getType() {
        if (sqlStatement instanceof SQLSelectStatement) {
            return SQLStatementType.SELECT;
        }
        if (sqlStatement instanceof SQLInsertStatement) {
            return SQLStatementType.INSERT;
        }
        if (sqlStatement instanceof SQLUpdateStatement) {
            return SQLStatementType.UPDATE;
        }
        if (sqlStatement instanceof SQLDeleteStatement) {
            return SQLStatementType.DELETE;
        }
        throw new SQLParserException("Unsupported SQL statement: [%s]", sqlStatement);
    }
}
