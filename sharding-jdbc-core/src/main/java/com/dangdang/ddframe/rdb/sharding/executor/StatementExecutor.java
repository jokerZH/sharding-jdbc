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
package com.dangdang.ddframe.rdb.sharding.executor;

import com.codahale.metrics.Timer.Context;
import com.dangdang.ddframe.rdb.sharding.executor.event.DMLExecutionEvent;
import com.dangdang.ddframe.rdb.sharding.executor.event.DMLExecutionEventBus;
import com.dangdang.ddframe.rdb.sharding.executor.event.DQLExecutionEvent;
import com.dangdang.ddframe.rdb.sharding.executor.event.DQLExecutionEventBus;
import com.dangdang.ddframe.rdb.sharding.executor.event.EventExecutionType;
import com.dangdang.ddframe.rdb.sharding.executor.wrapper.StatementExecutorWrapper;
import com.dangdang.ddframe.rdb.sharding.metrics.MetricsContext;
import com.google.common.base.Optional;
import lombok.RequiredArgsConstructor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/* 多线程执行静态语句对象请求的执行器 */
@RequiredArgsConstructor
public final class StatementExecutor {
    private final ExecutorEngine executorEngine;    /* 多线程处理器 */
    private final Collection<StatementExecutorWrapper> statementExecutorWrappers = new ArrayList<>();   /* 对应mysql物理后端执行单元 */
    
    /* 添加静态语句对象至执行上下文 */
    public void addStatement(final StatementExecutorWrapper statementExecutorWrapper) { statementExecutorWrappers.add(statementExecutorWrapper); }
    
    /* 执行SQL查询 */
    public List<ResultSet> executeQuery() {
        Context context = MetricsContext.start("ShardingStatement-executeQuery");

        postExecutionEvents();
        final boolean isExceptionThrown = ExecutorExceptionHandler.isExceptionThrown();
        final Map<String, Object> dataMap = ExecutorDataMap.getDataMap();
        List<ResultSet> result;
        try {
            if (1 == statementExecutorWrappers.size()) {
                // 单个物理sql，直接执行
                return Collections.singletonList(executeQueryInternal(statementExecutorWrappers.iterator().next(), isExceptionThrown, dataMap));
            }

            // 多个sql语句 异步执行
            result = executorEngine.execute(statementExecutorWrappers, new ExecuteUnit<StatementExecutorWrapper, ResultSet>() {
        
                @Override
                public ResultSet execute(final StatementExecutorWrapper input) throws Exception {
                    return executeQueryInternal(input, isExceptionThrown, dataMap);
                }
            });
        } finally {
            MetricsContext.stop(context);
        }
        return result;
    }

    /* 执行单个sql */
    private ResultSet executeQueryInternal(final StatementExecutorWrapper statementExecutorWrapper, final boolean isExceptionThrown, final Map<String, Object> dataMap) {
        ResultSet result;
        ExecutorExceptionHandler.setExceptionThrown(isExceptionThrown);
        ExecutorDataMap.setDataMap(dataMap);
        try {
            result = statementExecutorWrapper.getStatement().executeQuery(statementExecutorWrapper.getSqlExecutionUnit().getSql());
        } catch (final SQLException ex) {
            postExecutionEventsAfterExecution(statementExecutorWrapper, EventExecutionType.EXECUTE_FAILURE, Optional.of(ex));
            ExecutorExceptionHandler.handleException(ex);
            return null;
        }
        postExecutionEventsAfterExecution(statementExecutorWrapper);
        return result;
    }
    
    /* 执行SQL更新 */
    public int executeUpdate() {
        return executeUpdate(new Updater() {
            
            @Override
            public int executeUpdate(final Statement statement, final String sql) throws SQLException {
                return statement.executeUpdate(sql);
            }
        });
    }
    
    public int executeUpdate(final int autoGeneratedKeys) {
        return executeUpdate(new Updater() {
            
            @Override
            public int executeUpdate(final Statement statement, final String sql) throws SQLException {
                return statement.executeUpdate(sql, autoGeneratedKeys);
            }
        });
    }
    
    public int executeUpdate(final int[] columnIndexes) {
        return executeUpdate(new Updater() {
            
            @Override
            public int executeUpdate(final Statement statement, final String sql) throws SQLException {
                return statement.executeUpdate(sql, columnIndexes);
            }
        });
    }
    
    public int executeUpdate(final String[] columnNames) {
        return executeUpdate(new Updater() {
            
            @Override
            public int executeUpdate(final Statement statement, final String sql) throws SQLException {
                return statement.executeUpdate(sql, columnNames);
            }
        });
    }
    
    private int executeUpdate(final Updater updater) {
        Context context = MetricsContext.start("ShardingStatement-executeUpdate");
        postExecutionEvents();
        final boolean isExceptionThrown = ExecutorExceptionHandler.isExceptionThrown();
        final Map<String, Object> dataMap = ExecutorDataMap.getDataMap();
        try {
            if (1 == statementExecutorWrappers.size()) {
                return executeUpdateInternal(updater, statementExecutorWrappers.iterator().next(), isExceptionThrown, dataMap);
            }
            return executorEngine.execute(statementExecutorWrappers, new ExecuteUnit<StatementExecutorWrapper, Integer>() {
        
                @Override
                public Integer execute(final StatementExecutorWrapper input) throws Exception {
                    return executeUpdateInternal(updater, input, isExceptionThrown, dataMap);
                }
            }, new MergeUnit<Integer, Integer>() {
        
                @Override
                public Integer merge(final List<Integer> results) {
                    if (null == results) {
                        return 0;
                    }
                    int result = 0;
                    for (int each : results) {
                        result += each;
                    }
                    return result;
                }
            });
        } finally {
            MetricsContext.stop(context);
        }
    }
    
    private int executeUpdateInternal(final Updater updater, final StatementExecutorWrapper statementExecutorWrapper,
                                      final boolean isExceptionThrown, final Map<String, Object> dataMap) {
        int result;
        ExecutorExceptionHandler.setExceptionThrown(isExceptionThrown);
        ExecutorDataMap.setDataMap(dataMap);
        try {
            result = updater.executeUpdate(statementExecutorWrapper.getStatement(), statementExecutorWrapper.getSqlExecutionUnit().getSql());
        } catch (final SQLException ex) {
            postExecutionEventsAfterExecution(statementExecutorWrapper, EventExecutionType.EXECUTE_FAILURE, Optional.of(ex));
            ExecutorExceptionHandler.handleException(ex);
            return 0;
        }
        postExecutionEventsAfterExecution(statementExecutorWrapper);
        return result;
    }
    
    public boolean execute() {
        return execute(new Executor() {

            @Override
            public boolean execute(final Statement statement, final String sql) throws SQLException {
                return statement.execute(sql);
            }
        });
    }
    
    public boolean execute(final int autoGeneratedKeys) {
        return execute(new Executor() {
            
            @Override
            public boolean execute(final Statement statement, final String sql) throws SQLException {
                return statement.execute(sql, autoGeneratedKeys);
            }
        });
    }
    
    public boolean execute(final int[] columnIndexes) {
        return execute(new Executor() {
            
            @Override
            public boolean execute(final Statement statement, final String sql) throws SQLException {
                return statement.execute(sql, columnIndexes);
            }
        });
    }
    
    public boolean execute(final String[] columnNames) {
        return execute(new Executor() {
            
            @Override
            public boolean execute(final Statement statement, final String sql) throws SQLException {
                return statement.execute(sql, columnNames);
            }
        });
    }
    
    private boolean execute(final Executor executor) {
        Context context = MetricsContext.start("ShardingStatement-execute");
        postExecutionEvents();
        final boolean isExceptionThrown = ExecutorExceptionHandler.isExceptionThrown();
        final Map<String, Object> dataMap = ExecutorDataMap.getDataMap();
        try {
            if (1 == statementExecutorWrappers.size()) {
                return executeInternal(executor, statementExecutorWrappers.iterator().next(), isExceptionThrown, dataMap);
            }
            List<Boolean> result = executorEngine.execute(statementExecutorWrappers, new ExecuteUnit<StatementExecutorWrapper, Boolean>() {
        
                @Override
                public Boolean execute(final StatementExecutorWrapper input) throws Exception {
                    return executeInternal(executor, input, isExceptionThrown, dataMap);
                }
            });
            return (null == result || result.isEmpty()) ? false : result.get(0);
        } finally {
            MetricsContext.stop(context);
        }
    }
    
    private boolean executeInternal(final Executor executor, final StatementExecutorWrapper statementExecutorWrapper,
                                    final boolean isExceptionThrown, final Map<String, Object> dataMap) {
        boolean result;
        ExecutorExceptionHandler.setExceptionThrown(isExceptionThrown);
        ExecutorDataMap.setDataMap(dataMap);
        try {
            result = executor.execute(statementExecutorWrapper.getStatement(), statementExecutorWrapper.getSqlExecutionUnit().getSql());
        } catch (final SQLException ex) {
            postExecutionEventsAfterExecution(statementExecutorWrapper, EventExecutionType.EXECUTE_FAILURE, Optional.of(ex));
            ExecutorExceptionHandler.handleException(ex);
            return false;
        }
        postExecutionEventsAfterExecution(statementExecutorWrapper);
        return result;
    }

    /* 发送执行对event, bus是在执行物理sql对有所操作 */
    private void postExecutionEvents() {
        for (StatementExecutorWrapper each : statementExecutorWrappers) {
            if (each.getDMLExecutionEvent().isPresent()) {
                DMLExecutionEventBus.post(each.getDMLExecutionEvent().get());
            } else if (each.getDQLExecutionEvent().isPresent()) {
                DQLExecutionEventBus.post(each.getDQLExecutionEvent().get());
            }
        }
    }

    /* 正常执行的情况 */
    private void postExecutionEventsAfterExecution(final StatementExecutorWrapper statementExecutorWrapper) {
        postExecutionEventsAfterExecution(statementExecutorWrapper, EventExecutionType.EXECUTE_SUCCESS, Optional.<SQLException>absent());
    }

    /* 异常情况 */
    private void postExecutionEventsAfterExecution(final StatementExecutorWrapper statementExecutorWrapper, final EventExecutionType eventExecutionType, final Optional<SQLException> exp) {
        if (statementExecutorWrapper.getDMLExecutionEvent().isPresent()) {
            DMLExecutionEvent event = statementExecutorWrapper.getDMLExecutionEvent().get();
            event.setEventExecutionType(eventExecutionType);
            event.setExp(exp);
            DMLExecutionEventBus.post(event);
        } else if (statementExecutorWrapper.getDQLExecutionEvent().isPresent()) {
            DQLExecutionEvent event = statementExecutorWrapper.getDQLExecutionEvent().get();
            event.setEventExecutionType(eventExecutionType);
            event.setExp(exp);
            DQLExecutionEventBus.post(event);
        }
    }
    
    private interface Updater {
        int executeUpdate(Statement statement, String sql) throws SQLException;
    }
    
    private interface Executor {
        boolean execute(Statement statement, String sql) throws SQLException;
    }
}
