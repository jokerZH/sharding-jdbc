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

package com.dangdang.ddframe.rdb.sharding.parser.visitor.or;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.visitor.SQLASTOutputVisitor;
import com.dangdang.ddframe.rdb.sharding.parser.result.SQLParsedResult;
import com.dangdang.ddframe.rdb.sharding.parser.visitor.or.node.AbstractOrASTNode;
import com.google.common.base.Optional;

/* OR表达式解析类 */
public final class OrParser {
    
    private final SQLStatement sqlStatement;
    
    private final OrVisitor orVisitor;
    
    public OrParser(final SQLStatement sqlStatement, final SQLASTOutputVisitor dependencyVisitor) {
        this.sqlStatement = sqlStatement;
        orVisitor = new OrVisitor(dependencyVisitor);
    }
    
    /**
     *  填充条件上下文.
     * 
     * @param parsedResult 初步解析结果
     */
    public void fillConditionContext(final SQLParsedResult parsedResult) {
        Optional<AbstractOrASTNode> rootASTNode = orVisitor.visitHandle(sqlStatement);
        if (rootASTNode.isPresent()) {
            parsedResult.getConditionContexts().clear();
            parsedResult.getConditionContexts().addAll(rootASTNode.get().getCondition());
        }
    }
}
