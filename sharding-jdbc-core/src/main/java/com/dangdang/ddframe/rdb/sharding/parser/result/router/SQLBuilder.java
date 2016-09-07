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

import com.google.common.base.Joiner;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/* SQL构建器, 构建sql用的,特别中sql中有一些${xxx}这样的替换的地方的, append一般的,然后嗲用appendToken加入token,然后只要设置token的值就可以得到不同的sql语句  */
public class SQLBuilder implements Appendable {
    private final Collection<Object> segments = new LinkedList<>();
    private final Map<String/*token*/, StringToken> tokenMap = new HashMap<>();  /* TODO */
    private StringBuilder currentSegment;   /* 存放结果sql语句 */
    
    public SQLBuilder() {
        currentSegment = new StringBuilder();
        segments.add(currentSegment);
    }
    
    /* 增加占位符 */
    public SQLBuilder appendToken(final String token/*占位符*/) { return appendToken(token, true); }
    
    /* 增加占位符 */
    public SQLBuilder appendToken(final String token/*占位符*/, final boolean isSetValue/*是否设置占位值*/) {
        StringToken stringToken;
        if (tokenMap.containsKey(token)) {
            stringToken = tokenMap.get(token);
        } else {
            stringToken = new StringToken();
            if (isSetValue) {
                stringToken.value = token;
            }
            tokenMap.put(token, stringToken);
        }
        segments.add(stringToken);
        currentSegment = new StringBuilder();
        segments.add(currentSegment);
        return this;
    }
    
    /* 用实际的值替代占位符 */
    public SQLBuilder buildSQL(final String originToken/*占位符*/, final String newToken/*实际的值*/) {
        if (tokenMap.containsKey(originToken)) {
            tokenMap.get(originToken).value = newToken;
        }
        return this;
    }
    
    /* 是否包含占位符 */
    public boolean containsToken(final String token/*占位符*/) { return tokenMap.containsKey(token); }
    
    /* 生成SQL语句, 将各个token和string输出 */
    public String/*SQL语句*/ toSQL() {
        StringBuilder result = new StringBuilder();
        for (Object each : segments) {
            result.append(each.toString());
        }
        return result.toString();
    }
    
    @Override
    public Appendable append(final CharSequence sql) throws IOException {
        currentSegment.append(sql);
        return this;
    }
    
    @Override
    public Appendable append(final CharSequence sql, final int start, final int end) throws IOException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public Appendable append(final char c) throws IOException {
        currentSegment.append(c);
        return this;
    }

    /* 显示调试信息 */
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        for (Object each : segments) {
            if (each instanceof StringToken) {
                result.append(((StringToken) each).toToken());
            } else {
                result.append(each.toString());
            }
        }
        return result.toString();
    }
    
    /* 复制构建器 */
    public SQLBuilder cloneBuilder() {
        SQLBuilder result = new SQLBuilder();
        for (Object each : segments) {
            if (each instanceof StringToken) {
                StringToken token = (StringToken) each;
                StringToken newToken = new StringToken();
                newToken.value = token.value;
                result.segments.add(newToken);
                result.tokenMap.put(newToken.value, newToken);
            } else {
                result.segments.add(each);
            }
        }
        return result;
    }
    
    private class StringToken {
        private String value;
        
        public String toToken() {
            return null == value ? "" : Joiner.on("").join("[Token(", value, ")]");
        }
        
        @Override
        public String toString() {
            return null == value ? "" : value;
        }
    }
}
