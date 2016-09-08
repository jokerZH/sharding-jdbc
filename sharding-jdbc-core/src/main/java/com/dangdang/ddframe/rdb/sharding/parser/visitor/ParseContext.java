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
package com.dangdang.ddframe.rdb.sharding.parser.visitor;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.visitor.SQLEvalVisitor;
import com.alibaba.druid.sql.visitor.SQLEvalVisitorUtils;
import com.alibaba.druid.util.JdbcUtils;
import com.dangdang.ddframe.rdb.sharding.constants.DatabaseType;
import com.dangdang.ddframe.rdb.sharding.parser.result.SQLParsedResult;
import com.dangdang.ddframe.rdb.sharding.parser.result.merger.AggregationColumn;
import com.dangdang.ddframe.rdb.sharding.parser.result.merger.AggregationColumn.AggregationType;
import com.dangdang.ddframe.rdb.sharding.parser.result.merger.GroupByColumn;
import com.dangdang.ddframe.rdb.sharding.parser.result.merger.OrderByColumn;
import com.dangdang.ddframe.rdb.sharding.parser.result.merger.OrderByColumn.OrderByType;
import com.dangdang.ddframe.rdb.sharding.parser.result.router.Condition;
import com.dangdang.ddframe.rdb.sharding.parser.result.router.Condition.BinaryOperator;
import com.dangdang.ddframe.rdb.sharding.parser.result.router.Condition.Column;
import com.dangdang.ddframe.rdb.sharding.parser.result.router.ConditionContext;
import com.dangdang.ddframe.rdb.sharding.parser.result.router.Table;
import com.dangdang.ddframe.rdb.sharding.parser.visitor.basic.mysql.MySQLEvalVisitor;
import com.dangdang.ddframe.rdb.sharding.util.SQLUtil;
import com.google.common.base.Optional;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

/* 解析过程的上下文对象 */
@Getter
public final class ParseContext {
    private static final String AUTO_GEN_TOKE_KEY_TEMPLATE = "sharding_auto_gen_%d";
    private static final String SHARDING_GEN_ALIAS = "sharding_gen_%s"; /* sharding jdbc增加的函数, 使用的别名 */
    
    private final String autoGenTokenKey;                               /* 用于替换需要修改的sql部分,作为token */
    private final SQLParsedResult parsedResult = new SQLParsedResult(); /* 解析结果 */
    private final int parseContextIndex;                                /* 分配给当前解析流程的id, 有子查询的时候会>0 */

    @Setter
    private Collection<String> shardingColumns; /* 当前逻辑db中用到的分表字段集合 */
    
    @Setter
    private boolean hasOrCondition; /* 有or操作 */
    
    private final ConditionContext currentConditionContext = new ConditionContext();    /* 当前解析的计算表达式对象 */
    
    private Table currentTable;
    
    private int selectItemsCount;   /* sharding jdbc增加的函数的别名中的计数下标,保证别名唯一性 */
    
    private final Collection<String> selectItems = new HashSet<>(); /* select中包含的字段名 */
    
    private boolean hasAllColumn;   /* select中包含所有字段 */
    
    @Setter
    private ParseContext parentParseContext;    /* TODO 子查询 */
    
    private List<ParseContext> subParseContext = new LinkedList<>();
    
    private int itemIndex;  /* select中字段的个数 */
    
    public ParseContext(final int parseContextIndex) {
        this.parseContextIndex = parseContextIndex;
        autoGenTokenKey = String.format(AUTO_GEN_TOKE_KEY_TEMPLATE, parseContextIndex);
    }
    
    /* 增加查询投射项数量 */
    public void increaseItemIndex() { itemIndex++; }

    /* 将表名字符串加入解析上下文 */
    public void setCurrentTable(final String currentTableName/*表名称*/, final Optional<String> currentAlias/*表别名*/) {
        Table table = new Table(
                SQLUtil.getExactlyValue(currentTableName),
                currentAlias.isPresent() ? Optional.of(SQLUtil.getExactlyValue(currentAlias.get())) : currentAlias
        );

        // 加到逻辑表集合中
        parsedResult.getRouteContext().getTables().add(table);
        currentTable = table;
    }

    /* 将表对象加入解析上下文 */
    public Table addTable(final SQLExprTableSource x/*表名表达式, 来源于FROM, INSERT ,UPDATE, DELETE等语句*/) {
        Table result = new Table(SQLUtil.getExactlyValue(x.getExpr().toString()), SQLUtil.getExactlyValue(x.getAlias()));
        parsedResult.getRouteContext().getTables().add(result);
        return result;
    }
    
    /* 向解析上下文中添加计算表达式对象 */
    public void addCondition(
            final SQLExpr expr,                 /*SQL表达式*/
            final BinaryOperator operator,      /*操作符*/
            final List<SQLExpr> valueExprList,  /*值对象表达式集合*/
            final DatabaseType databaseType,    /*数据库类型*/
            final List<Object> parameters       /*通过占位符传进来的参数*/
    ) {
        Optional<Column> column = getColumn(expr);
        if (!column.isPresent() || !shardingColumns.contains(column.get().getColumnName())) {
            // 不属于分表字段
            return;
        }

        List<ValuePair> values = new ArrayList<>(valueExprList.size());
        for (SQLExpr each : valueExprList) {
            ValuePair evalValue = evalExpression(databaseType, each, parameters);
            if (null != evalValue) {
                values.add(evalValue);
            }
        }
        if (values.isEmpty()) {
            return;
        }
        addCondition(column.get(), operator, values);
    }
    
    /* 将计算表达式对象加入解析上下文, 已经获得字段名和表名了, 且值是单个 */
    public void addCondition(
            final String columnName,
            final String tableName,
            final BinaryOperator operator,
            final SQLExpr valueExpr,
            final DatabaseType databaseType,
            final List<Object> parameters
    ) {
        Column column = createColumn(columnName, tableName);
        if (!shardingColumns.contains(column.getColumnName())) {
            return;
        }
        ValuePair value = evalExpression(databaseType, valueExpr, parameters);
        if (null != value) {
            addCondition(column, operator, Collections.singletonList(value));
        }
    }

    /* 讲一个计算表达式加入 */
    private void addCondition(final Column column, final BinaryOperator operator, final List<ValuePair> valuePairs) {
        Optional<Condition> optionalCondition = currentConditionContext.find(column.getTableName(), column.getColumnName(), operator);
        Condition condition;
        // 相同操作, 字段,表相同,则值就放一起了  我呵呵
        // TODO 待讨论
        if (optionalCondition.isPresent()) {
            condition = optionalCondition.get();
        } else {
            condition = new Condition(column, operator);
            currentConditionContext.add(condition);
        }
        for (ValuePair each : valuePairs) {
            condition.getValues().add(each.value);
            if (each.paramIndex > -1) {
                condition.getValueIndices().add(each.paramIndex);
            }
        }
    }

    /* 讲durid中得到的语法对象转成自己的值对象 */
    private ValuePair evalExpression(final DatabaseType databaseType, final SQLObject sqlObject, final List<Object> parameters) {
        if (sqlObject instanceof SQLMethodInvokeExpr) {
            // TODO 解析函数中的sharingValue不支持
            return null;
        }

        /* 通过visitor获得值 */
        SQLEvalVisitor visitor;
        switch (databaseType.name().toLowerCase()) {
            case JdbcUtils.MYSQL:
            case JdbcUtils.H2: 
                visitor = new MySQLEvalVisitor();
                break;
            default: 
                visitor = SQLEvalVisitorUtils.createEvalVisitor(databaseType.name());    
        }
        visitor.setParameters(parameters);
        sqlObject.accept(visitor);
        
        Object value = SQLEvalVisitorUtils.getValue(sqlObject);
        if (null == value) {
            // TODO 对于NULL目前解析为空字符串,此处待考虑解决方法
            return null;
        }

        Comparable<?> finalValue;
        if (value instanceof Comparable<?>) {
            finalValue = (Comparable<?>) value;
        } else {
            finalValue = "";
        }
        Integer index = (Integer) sqlObject.getAttribute(MySQLEvalVisitor.EVAL_VAR_INDEX);
        if (null == index) {
            index = -1;
        }
        return new ValuePair(finalValue, index);
    }

    /* 从sql对象中找到涉及的字段 */
    private Optional<Column> getColumn(final SQLExpr expr) {
        if (expr instanceof SQLPropertyExpr) {
            return Optional.fromNullable(getColumnWithQualifiedName((SQLPropertyExpr) expr));
        }
        if (expr instanceof SQLIdentifierExpr) {
            return Optional.fromNullable(getColumnWithoutAlias((SQLIdentifierExpr) expr));
        }
        return Optional.absent();
    }
    
    private Column getColumnWithQualifiedName(final SQLPropertyExpr expr) {
        Optional<Table> table = findTable(((SQLIdentifierExpr) expr.getOwner()).getName());
        return expr.getOwner() instanceof SQLIdentifierExpr && table.isPresent() ? createColumn(expr.getName(), table.get().getName()) : null;
    }
    
    private Column getColumnWithoutAlias(final SQLIdentifierExpr expr) {
        return null != currentTable ? createColumn(expr.getName(), currentTable.getName()) : null;
    }
    
    private Column createColumn(final String columnName, final String tableName) {
        return new Column(SQLUtil.getExactlyValue(columnName), SQLUtil.getExactlyValue(tableName));
    }
    
    private Optional<Table> findTable(final String tableNameOrAlias) {
        Optional<Table> tableFromName = findTableFromName(tableNameOrAlias);
        return tableFromName.isPresent() ? tableFromName : findTableFromAlias(tableNameOrAlias);
    }
    
    /* 判断SQL表达式是否为二元操作且带有别名 */
    public boolean/*是否为二元操作且带有别名*/ isBinaryOperateWithAlias(final SQLPropertyExpr x/*待判断的SQL表达式*/, final String tableOrAliasName/*表名称或别名*/) {
        return x.getParent() instanceof SQLBinaryOpExpr && findTableFromAlias(SQLUtil.getExactlyValue(tableOrAliasName)).isPresent();
    }
    
    private Optional<Table> findTableFromName(final String name) {
        for (Table each : parsedResult.getRouteContext().getTables()) {
            if (each.getName().equalsIgnoreCase(SQLUtil.getExactlyValue(name))) {
                return Optional.of(each);
            }
        }
        return Optional.absent();
    }
    
    private Optional<Table> findTableFromAlias(final String alias) {
        for (Table each : parsedResult.getRouteContext().getTables()) {
            if (each.getAlias().isPresent() && each.getAlias().get().equalsIgnoreCase(SQLUtil.getExactlyValue(alias))) {
                return Optional.of(each);
            }
        }
        return Optional.absent();
    }
    
    /* 将求平均值函数的补列加入解析上下文, 讲求平均的函数转化成求sum和count的函数 */
    public void addDerivedColumnsForAvgColumn(final AggregationColumn avgColumn) {
        addDerivedColumnForAvgColumn(avgColumn, getDerivedCountColumn(avgColumn));
        addDerivedColumnForAvgColumn(avgColumn, getDerivedSumColumn(avgColumn));
    }

    /* 增加一个函数对象 */
    private void addDerivedColumnForAvgColumn(final AggregationColumn avgColumn, final AggregationColumn derivedColumn) {
        avgColumn.getDerivedColumns().add(derivedColumn);
        parsedResult.getMergeContext().getAggregationColumns().add(derivedColumn);
    }

    /* 获得一个count对象 */
    private AggregationColumn getDerivedCountColumn(final AggregationColumn avgColumn) {
        String expression = avgColumn.getExpression().replaceFirst(AggregationType.AVG.toString(), AggregationType.COUNT.toString());
        return new AggregationColumn(expression, AggregationType.COUNT, Optional.of(generateDerivedColumnAlias()), avgColumn.getOption());
    }

    /*　对于加入的函数,使用别名　*/
    private String generateDerivedColumnAlias() {
        return String.format(SHARDING_GEN_ALIAS, ++selectItemsCount);
    }

    /* 获得一个sum*/
    private AggregationColumn getDerivedSumColumn(final AggregationColumn avgColumn) {
        String expression = avgColumn.getExpression().replaceFirst(AggregationType.AVG.toString(), AggregationType.SUM.toString());
        if (avgColumn.getOption().isPresent()) {
            // 消除option　FIXME
            expression = expression.replaceFirst(avgColumn.getOption().get() + " ", "");
        }
        return new AggregationColumn(expression, AggregationType.SUM, Optional.of(generateDerivedColumnAlias()), Optional.<String>absent());
    }
    
    /* 将排序列加入解析上下文 */
    public void addOrderByColumn(final int index/* 字段下标 */, final OrderByType orderByType) {
        parsedResult.getMergeContext().getOrderByColumns().add(new OrderByColumn(index, orderByType));
    }
    
    /* 将排序列加入解析上下文 */
    public void addOrderByColumn(final Optional<String> owner/*列拥有者*/, final String name/*列名称*/, final OrderByType orderByType/*排序类型*/) {
        String rawName = SQLUtil.getExactlyValue(name);
        parsedResult.getMergeContext().getOrderByColumns().add(new OrderByColumn(owner, rawName, getAlias(rawName), orderByType));
    }
    
    private Optional<String> getAlias(final String name) {
        if (containsSelectItem(name)) {
            return Optional.absent();
        }
        return Optional.of(generateDerivedColumnAlias());
    }

    /* 判断sleectItem是否已经在selec中存在 */
    private boolean containsSelectItem(final String selectItem) {
        return hasAllColumn || selectItems.contains(selectItem);
    }
    
    /* 将分组列加入解析上下文 */
    public void addGroupByColumns(final Optional<String> owner/*列拥有者*/, final String name/*列名称*/, final OrderByType orderByType/*排序类型*/) {
        String rawName = SQLUtil.getExactlyValue(name);
        parsedResult.getMergeContext().getGroupByColumns().add(new GroupByColumn(owner, rawName, getAlias(rawName), orderByType));
    }
    
    
    /* 将当前解析的条件对象归并入解析结果 */
    public void mergeCurrentConditionContext() {
        if (!parsedResult.getRouteContext().getTables().isEmpty()) {
            // 如果逻辑表有的,则直接放入
            if (parsedResult.getConditionContexts().isEmpty()) {
                parsedResult.getConditionContexts().add(currentConditionContext);
            }
            return;
        }
        // 否则 先找到routeContext的表
        Optional<SQLParsedResult> target = findValidParseResult();
        if (!target.isPresent()) {
            if (parsedResult.getConditionContexts().isEmpty()) {
                parsedResult.getConditionContexts().add(currentConditionContext);
            }
            return;
        }
        parsedResult.getRouteContext().getTables().addAll(target.get().getRouteContext().getTables());
        parsedResult.getConditionContexts().addAll(target.get().getConditionContexts());
    }

    /* TODO */
    private Optional<SQLParsedResult> findValidParseResult() {
        for (ParseContext each : subParseContext) {
            each.mergeCurrentConditionContext();
            if (each.getParsedResult().getRouteContext().getTables().isEmpty()) {
                continue;
            }
            return Optional.of(each.getParsedResult()); 
        }
        return Optional.absent();
    }
    
    /* 注册SELECT语句中声明的列名称或别名 */
    public void registerSelectItem(final String selectItem/*SELECT语句中声明的列名称或别名*/) {
        String rawItemExpr = SQLUtil.getExactlyValue(selectItem);
        if ("*".equals(rawItemExpr)) {
            hasAllColumn = true;
            return;
        }
        selectItems.add(rawItemExpr);
    }

    /* sql语句中一个值 */
    @RequiredArgsConstructor
    private static class ValuePair {
        private final Comparable<?> value;  /* 值 */
        private final Integer paramIndex;   /* ? */
    }
}
