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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.annotation.Nullable;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.Assignment;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.yahoo.ycsb.ByteIterator;

/**
 * Cassandra CQL3 Binary Protocol client for YCSB framework with PreparedStatements
 */
public class CassandraJavaDriverClientPS extends AbstractCassandraJavaDriverClient {

	Map<String, PreparedStatement> preparedStatements = new HashMap<String, PreparedStatement>();

	@Override
	protected ResultSet read(String table, String key, Set<String> fields) {
		String cacheKey = getReadCacheKey(table, fields);
		PreparedStatement pStmt = preparedStatements.get(cacheKey);

		if (pStmt == null) {
			if (_debug) {
				System.out.println("Creating statement: " + cacheKey);
			}
			
			String query = "SELECT ";
			if (fields != null) {
				query += Joiner.on(',').join(fields) + " ";
			} else {
				query += " * ";
			}
			query += "FROM " + table;
			query += " WHERE " + _keyColumnName + " = ?";

			pStmt = session.prepare(query);
			preparedStatements.put(cacheKey, pStmt);
		} else if (_debug) {
			System.out.println("Reusing statement: " + cacheKey);
		}
		BoundStatement bndStmt = pStmt.bind(key);
		bndStmt.setConsistencyLevel(toDriverConsistencyLevel(readConsistencyLevel));

		return session.execute(bndStmt);
	}

	private String getReadCacheKey(String table, Set<String> fields) {
		return "READ" + table + (fields != null ? Joiner.on(',').join(fields) : "NULL");
	}

	@Override
	protected ResultSet scan(String table, String startkey, int recordcount, Set<String> fields) {
		String cacheKey = getScanCacheKey(table, fields, recordcount);
		PreparedStatement pStmt = preparedStatements.get(cacheKey);

		if (pStmt == null) {
			if (_debug) {
				System.out.println("Creating statement: " + cacheKey);
			}
			
			String query = "SELECT ";
			if (fields != null) {
				String fieldsQuery = Joiner.on(',').join(fields);
				query += fieldsQuery + " ";
			} else {
				query += " * ";
			}
			query += "FROM " + table;
			query += " WHERE " + _keyColumnName + " > ? ";
			query += " limit " + recordcount;

			pStmt = session.prepare(query);
			preparedStatements.put(cacheKey, pStmt);
		} else if (_debug) {
			System.out.println("Reusing statement: " + cacheKey);
		}
		BoundStatement bndStmt = pStmt.bind(startkey);
		bndStmt.setConsistencyLevel(toDriverConsistencyLevel(scanConsistencyLevel));

		return session.execute(bndStmt);
	}

	private String getScanCacheKey(String table, Set<String> fields, int recordCount) {
		return "SCAN" + table + (fields != null ? Joiner.on(',').join(fields) : "NULL") + recordCount;
	}

	@Override
	protected void innerUpdate(String table, String key, HashMap<String, ByteIterator> values) {
		SortedSet<String> fields = new TreeSet<String>(values.keySet());
		String cacheKey = getUpdateCacheKey(table, fields);
		PreparedStatement pStmt = preparedStatements.get(cacheKey);
		if (pStmt == null) {
			StringBuilder updateQuery = new StringBuilder("UPDATE " + table + " SET ");
			updateQuery.append(Joiner.on(',').join(Iterables.transform(fields, fieldAssignmentFunction)));
			updateQuery.append(" WHERE " + _keyColumnName + "= ?");

			pStmt = session.prepare(updateQuery.toString());
			preparedStatements.put(cacheKey, pStmt);
		}
		BoundStatement bndStmt = pStmt.bind();
		bndStmt.setConsistencyLevel(toDriverConsistencyLevel(writeConsistencyLevel));

		int bndIndex = 0;
		for (String field : fields) {
			bndStmt.setString(bndIndex, values.get(field).toString());
			bndIndex++;
		}
		bndStmt.setString(bndIndex, key);

		session.execute(bndStmt);
	}

	private Function<String, String> fieldAssignmentFunction = new Function<String, String>() {
		@Override
		@Nullable
		public String apply(@Nullable String item) {
			return " " + item + "= ? ";
		}
	};

	private String getUpdateCacheKey(String table, Set<String> fields) {
		return "UPDATE" + table + (fields != null ? Joiner.on(',').join(fields) : "NULL");
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
		SortedSet<String> fields = new TreeSet<String>(values.keySet());
		String cacheKey = getInsertCacheKey(table, fields);
		PreparedStatement pStmt = preparedStatements.get(cacheKey);
		if (pStmt == null) {
			if (_debug) {
				System.out.println("Creating statement: " + cacheKey);
			}
			StringBuilder insertQuery = new StringBuilder("INSERT INTO " + table + "(" + _keyColumnName + ",");
			insertQuery.append(Joiner.on(',').join(fields));
			insertQuery.append(") VALUES (?,");
			insertQuery.append(Joiner.on(',').join(Collections.nCopies(fields.size(), "?")));
			insertQuery.append(")");
			pStmt = session.prepare(insertQuery.toString());
			preparedStatements.put(cacheKey, pStmt);
		} else if (_debug) {
			System.out.println("Reusing statement: " + cacheKey);
		}
		BoundStatement bndStmt = pStmt.bind();
		bndStmt.setConsistencyLevel(toDriverConsistencyLevel(writeConsistencyLevel));

		int bndIndex = 0;
		bndStmt.setString(bndIndex, key);
		bndIndex++;
		for (String field : fields) {
			bndStmt.setString(bndIndex, values.get(field).toString());
			bndIndex++;
		}

		session.execute(bndStmt);
	}

	private String getInsertCacheKey(String table, Set<String> fields) {
		return "INSERT" + table + (fields != null ? Joiner.on(',').join(fields) : "NULL");
	}

	@Override
	protected void innerDelete(String table, String key) {
		String cacheKey = getDeleteCacheKey(table);
		PreparedStatement pStmt = preparedStatements.get(cacheKey);

		if (pStmt == null) {
			String query = "DELETE FROM " + table + " WHERE " + _keyColumnName + "= ?";
			pStmt = session.prepare(query);
			preparedStatements.put(cacheKey, pStmt);
		}
		BoundStatement bndStmt = pStmt.bind(key);
		bndStmt.setConsistencyLevel(toDriverConsistencyLevel(deleteConsistencyLevel));

		session.execute(bndStmt);
	}

	private String getDeleteCacheKey(String table) {
		return "DELETE" + table;
	}

}
