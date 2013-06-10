/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db;

import static com.google.common.collect.Iterables.transform;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import com.datastax.driver.core.ResultSet;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.yahoo.ycsb.ByteIterator;

/**
 * Cassandra CQL3 Binary Protocol client for YCSB framework with simple statements
 */
public class CassandraClient12Q extends AbstractCassandraClient12 {

	@Override
	protected ResultSet read(String table, String key, Set<String> fields) {
			StringBuilder query = new StringBuilder("SELECT ");
			if (fields != null) {
				query.append(Joiner.on(',').join(fields) + " ");
			} else {
			    query.append(" * ");
			}
			query.append("FROM " + table );
			query.append(" WHERE " + _keyColumnName + " = '" + key+"'");

			return session.execute(query.toString());
	}

	@Override
	protected ResultSet scan(String table, String startkey, int recordcount, Set<String> fields) {
            StringBuilder query = new StringBuilder("SELECT ");
			if (fields != null) {
				query.append(Joiner.on(',').join(fields) + " ");
			} else {
			    query.append(" * ");
			}
			query.append("FROM " + table);
			query.append(" WHERE " + _keyColumnName + " > '" + startkey+"'");
			query.append(" limit " + recordcount);

			return session.execute(query.toString());
	}

	@Override
	protected void innerUpdate(String table, String key, HashMap<String, ByteIterator> values) {
			StringBuilder updateQuery = new StringBuilder("UPDATE " + table + " SET ");
			updateQuery.append(Joiner.on(',').join(Iterables.transform(values.entrySet(), fieldAssignmentFunction)));
			updateQuery.append(" WHERE " + _keyColumnName + "= '"+key+"'");

			session.execute(updateQuery.toString());
	}

	private Function<Map.Entry<String, ByteIterator>, String> fieldAssignmentFunction = new Function<Map.Entry<String, ByteIterator>, String>() {
		@Override
		@Nullable
		public String apply(@Nullable Map.Entry<String, ByteIterator> item) {
			return " " + item.getKey() + " = '"+item.getValue()+"' ";
		}
	};

	@Override
	protected void innerInsert(String table, String key, HashMap<String, ByteIterator> values) {
			StringBuilder insertQuery = new StringBuilder("INSERT INTO " + table + "(" + _keyColumnName + ",");
			insertQuery.append(Joiner.on(',').join(values.keySet()));
			insertQuery.append(") VALUES ('"+key+"',");
			insertQuery.append(Joiner.on(',').join(transform(values.values(), valueInsertFunction)));
			insertQuery.append(")");
			session.execute(insertQuery.toString());
	}
	
	private Function<ByteIterator, String> valueInsertFunction = new Function<ByteIterator, String>() {
		@Override
		@Nullable
		public String apply(@Nullable ByteIterator item) {
			return "'"+item.toString()+"'";
		}
	};

	@Override
	protected void innerDelete(String table, String key) {
			StringBuilder query = new StringBuilder("DELETE FROM " + table + " WHERE " + _keyColumnName + "= '"+key+"'");
			session.execute(query.toString());
	}

}
