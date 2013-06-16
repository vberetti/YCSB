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

import java.util.HashMap;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;

import com.datastax.driver.core.ResultSet;
import com.yahoo.ycsb.ByteIterator;

/**
 * Cassandra CQL3 Binary Protocol client for YCSB framework with simple statements
 */
public class CassandraJavaDriverClientQ extends AbstractCassandraJavaDriverClient {

	@Override
	protected ResultSet read(String table, String key, Set<String> fields) {
			return session.execute(QueryHelper.readQuery(table, _keyColumnName, key, fields, readConsistencyLevel));
	}

	@Override
	protected ResultSet scan(String table, String startkey, int recordcount, Set<String> fields) {
	    throw new NotImplementedException();
	}

	@Override
	protected void innerUpdate(String table, String key, HashMap<String, ByteIterator> values) {
			session.execute(QueryHelper.updateQuery(table, _keyColumnName, key, values, writeConsistencyLevel));
	}

	@Override
	protected void innerInsert(String table, String key, HashMap<String, ByteIterator> values) {
			session.execute(QueryHelper.insertQuery(table, _keyColumnName, key, values, writeConsistencyLevel));
	}
	
	@Override
	protected void innerDelete(String table, String key) {
			session.execute(QueryHelper.deleteQuery(table, _keyColumnName, key, deleteConsistencyLevel));
	}

}
