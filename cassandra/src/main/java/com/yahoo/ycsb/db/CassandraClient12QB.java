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

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nullable;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.Assignment;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.yahoo.ycsb.ByteIterator;

/**
 * Cassandra CQL3 Binary Protocol client for YCSB framework with QueryBuilder
 */
public class CassandraClient12QB extends AbstractCassandraClient12 {

	@Override
	protected ResultSet read(String table, String key, Set<String> fields) {
		com.datastax.driver.core.querybuilder.Select.Builder selection = null;
		if(fields != null && !fields.isEmpty()){
			selection = QueryBuilder.select(fields.toArray(new String[0]));
		}else{
			selection = QueryBuilder.select();
		}
		Select select = selection.from(_keyspace, _columnFamily);
		select.where(QueryBuilder.eq(_keyColumnName, key));
		select.setConsistencyLevel(readConsistencyLevel);

		return session.execute(select);
	}

	@Override
	protected ResultSet scan(String table, String startkey, int recordcount, Set<String> fields) {
		com.datastax.driver.core.querybuilder.Select.Builder selection = null;
		if(fields != null && !fields.isEmpty()){
			selection = QueryBuilder.select(fields.toArray(new String[0]));
		}else{
			selection = QueryBuilder.select();
		}
		Select select = selection.from(_keyspace,_columnFamily);
		select.where(QueryBuilder.gte(_keyColumnName, startkey));
		select.limit(recordcount);
		select.setConsistencyLevel(readConsistencyLevel);

		return session.execute(select);
	}

	@Override
	protected void innerUpdate(String table, String key, HashMap<String, ByteIterator> values) {
		Update update = QueryBuilder.update(_keyspace, _columnFamily);

		Iterable<Assignment> assignments = Iterables.transform(values.entrySet(), assignmentFunction);
		for (Assignment assignment : assignments) {
			update.with(assignment);
		}

		update.where(eq(_keyColumnName, key));
		update.setConsistencyLevel(writeConsistencyLevel);
		
		session.execute(update);
	}

	Function<Map.Entry<String, ByteIterator>, Assignment> assignmentFunction = new Function<Map.Entry<String, ByteIterator>, Assignment>() {
		@Override
		@Nullable
		public Assignment apply(@Nullable Entry<String, ByteIterator> arg0) {
			return QueryBuilder.set(arg0.getKey(), arg0.getValue().toString());
		}
	};

	@Override
	protected void innerInsert(String table, String key, HashMap<String, ByteIterator> values) {
		Insert insert = QueryBuilder.insertInto(_keyspace, _columnFamily);
		insert.value(_keyColumnName, key);
		for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
			insert.value(entry.getKey(), entry.getValue().toString());
		}
		insert.setConsistencyLevel(writeConsistencyLevel);
		session.execute(insert);
	}

	@Override
	protected void innerDelete(String table, String key) {
		Delete delete = QueryBuilder.delete().from(_keyspace, _columnFamily);
		delete.where(eq(_keyColumnName, key));
		delete.setConsistencyLevel(deleteConsistencyLevel);
		
		session.execute(delete);		
	}

}
