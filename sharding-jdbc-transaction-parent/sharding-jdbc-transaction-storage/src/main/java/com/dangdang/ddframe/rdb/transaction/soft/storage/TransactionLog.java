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

package com.dangdang.ddframe.rdb.transaction.soft.storage;

import com.dangdang.ddframe.rdb.transaction.soft.constants.SoftTransactionType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

/* 事务日志记录 */
@AllArgsConstructor
@Getter
public final class TransactionLog {
    private final String id;                            /* 业务主键 */
    private final String transactionId;                 /* 事物id */
    private final SoftTransactionType transactionType;  /* 事物类型 */
    private final String dataSource;                    /* 目标分片 */
    private final String sql;                           /* 执行的sql语句 */
    private final List<Object> parameters;              /* sql的参数 */
    private final long creationTime;                    /* 创建时间 */
    
    @Setter
    private int asyncDeliveryTryTimes;  /* 异步执行的次数 */
}
