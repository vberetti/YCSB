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

import static com.google.common.collect.Maps.newHashMap;

import java.util.HashMap;
import java.util.Random;
import java.util.Set;
import java.util.Vector;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

/**
 * Cassandra CQL3 Binary Protocol client for YCSB framework
 */
public abstract class AbstractCassandraClient12 extends DB {
	static Random random = new Random();
	public static final int Ok = 0;
	public static final int Error = -1;

	public int ConnectionRetries;
	public int OperationRetries;
	public String _keyspace;
	public String _columnFamily;
	public String _keyColumnName;

	public static final String CONNECTION_RETRY_PROPERTY = "cassandra.connectionretries";
	public static final String CONNECTION_RETRY_PROPERTY_DEFAULT = "300";

	public static final String OPERATION_RETRY_PROPERTY = "cassandra.operationretries";
	public static final String OPERATION_RETRY_PROPERTY_DEFAULT = "300";

	public static final String USERNAME_PROPERTY = "cassandra.username";
	public static final String PASSWORD_PROPERTY = "cassandra.password";

	public static final String KEYSPACE_PROPERTY = "cassandra.keyspace";
	public static final String KEYSPACE_PROPERTY_DEFAULT = "usertable";

	public static final String COLUMN_FAMILY_PROPERTY = "cassandra.columnfamily";
	public static final String COLUMN_FAMILY_PROPERTY_DEFAULT = "data";

	public static final String KEY_COLUMN_NAME_PROPERTY = "cassandra.key.column";
	public static final String KEY_COLUMN_NAME_PROPERTY_DEFAULT = "key";

	public static final String READ_CONSISTENCY_LEVEL_PROPERTY = "cassandra.readconsistencylevel";
	public static final String READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";

	public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY = "cassandra.writeconsistencylevel";
	public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";

	public static final String SCAN_CONSISTENCY_LEVEL_PROPERTY = "cassandra.scanconsistencylevel";
	public static final String SCAN_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";

	public static final String DELETE_CONSISTENCY_LEVEL_PROPERTY = "cassandra.deleteconsistencylevel";
	public static final String DELETE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";

	Session session;

	boolean _debug = false;

	Exception errorexception = null;

	ConsistencyLevel readConsistencyLevel = ConsistencyLevel.ONE;
	ConsistencyLevel writeConsistencyLevel = ConsistencyLevel.ONE;
	ConsistencyLevel scanConsistencyLevel = ConsistencyLevel.ONE;
	ConsistencyLevel deleteConsistencyLevel = ConsistencyLevel.ONE;

	/**
	 * Initialize any state for this DB. Called once per DB instance; there is one DB instance per client thread.
	 */
	@Override
	public void init() throws DBException {
		String hosts = getProperties().getProperty("hosts");
		if (hosts == null) {
			throw new DBException("Required property \"hosts\" missing for CassandraClient");
		}

		_columnFamily = getProperties().getProperty(COLUMN_FAMILY_PROPERTY, COLUMN_FAMILY_PROPERTY_DEFAULT);
		_keyspace = getProperties().getProperty(KEYSPACE_PROPERTY, KEYSPACE_PROPERTY_DEFAULT);
		_keyColumnName = getProperties().getProperty(KEY_COLUMN_NAME_PROPERTY, KEY_COLUMN_NAME_PROPERTY_DEFAULT);

		ConnectionRetries = Integer.parseInt(getProperties().getProperty(CONNECTION_RETRY_PROPERTY, CONNECTION_RETRY_PROPERTY_DEFAULT));
		OperationRetries = Integer.parseInt(getProperties().getProperty(OPERATION_RETRY_PROPERTY, OPERATION_RETRY_PROPERTY_DEFAULT));

		String username = getProperties().getProperty(USERNAME_PROPERTY);
		String password = getProperties().getProperty(PASSWORD_PROPERTY);

		readConsistencyLevel = ConsistencyLevel.valueOf(getProperties().getProperty(READ_CONSISTENCY_LEVEL_PROPERTY,
				READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
		writeConsistencyLevel = ConsistencyLevel.valueOf(getProperties().getProperty(WRITE_CONSISTENCY_LEVEL_PROPERTY,
				WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
		scanConsistencyLevel = ConsistencyLevel.valueOf(getProperties().getProperty(SCAN_CONSISTENCY_LEVEL_PROPERTY,
				SCAN_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
		deleteConsistencyLevel = ConsistencyLevel.valueOf(getProperties().getProperty(DELETE_CONSISTENCY_LEVEL_PROPERTY,
				DELETE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));

		_debug = Boolean.parseBoolean(getProperties().getProperty("debug", "false"));

		String[] allhosts = hosts.split(",");

		for (int retry = 0; retry < ConnectionRetries; retry++) {

			try {
				Builder clusterBuilder = new Cluster.Builder().addContactPoints(allhosts);
				if (username != null && password != null) {
					clusterBuilder.withCredentials(username, password);
				}
				clusterBuilder.withCompression(Compression.SNAPPY);
				clusterBuilder.withoutMetrics();
				clusterBuilder.withoutJMXReporting();
				Cluster cluster = clusterBuilder.build();
				session = cluster.connect();
				session.execute("USE " + _keyspace);
				break;
			} catch (Exception e) {
				e.printStackTrace();
				errorexception = e;
			}
		}

	}

	/**
	 * Cleanup any state for this DB. Called once per DB instance; there is one DB instance per client thread.
	 */
	@Override
	public void cleanup() throws DBException {
		session.shutdown();
	}

	/**
	 * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
	 * 
	 * @param table
	 *            The name of the table
	 * @param key
	 *            The record key of the record to read.
	 * @param fields
	 *            The list of fields to read, or null for all of them
	 * @param result
	 *            A HashMap of field/value pairs for the result
	 * @return Zero on success, a non-zero error code on error
	 */
	@Override
	public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
		if (!_columnFamily.equals(table)) {
			_columnFamily = table;
		}

		for (int i = 0; i < OperationRetries; i++) {

			try {
				if (_debug) {
					System.out.print("Reading key: " + key);
				}

				ResultSet resultSet = read(table, key, fields);
				for (Row row : resultSet.all()) {
					for (Definition def : row.getColumnDefinitions().asList()) {
						String name = def.getName();
						ByteIterator value = new StringByteIterator(row.getString(name));
						result.put(name, value);
					}
				}

				if (_debug) {
					System.out.println();
					System.out.println("ConsistencyLevel=" + readConsistencyLevel.toString());
				}

				return Ok;
			} catch (Exception e) {
				errorexception = e;
			}

			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
			}
		}
		errorexception.printStackTrace();
		errorexception.printStackTrace(System.out);
		return Error;

	}

	protected abstract ResultSet read(String table, String key, Set<String> fields);

	/**
	 * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
	 * 
	 * @param table
	 *            The name of the table
	 * @param startkey
	 *            The record key of the first record to read.
	 * @param recordcount
	 *            The number of records to read
	 * @param fields
	 *            The list of fields to read, or null for all of them
	 * @param result
	 *            A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
	 * @return Zero on success, a non-zero error code on error
	 */
	@Override
	public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		if (!_columnFamily.equals(table)) {
			_columnFamily = table;
		}

		for (int i = 0; i < OperationRetries; i++) {

			try {

				if (_debug) {
					System.out.println("Scanning startkey: " + startkey);
				}

				ResultSet resultSet = scan(table, startkey, recordcount, fields);
				for (Row row : resultSet.all()) {
					HashMap<String, ByteIterator> tuple = newHashMap();
					for (Definition def : row.getColumnDefinitions().asList()) {
						String name = def.getName();
						ByteIterator value = new StringByteIterator(row.getString(name));
						tuple.put(name, value);
					}
					result.add(tuple);
				}

				if (_debug) {
					System.out.println();
					System.out.println("ConsistencyLevel=" + readConsistencyLevel.toString());
				}

				return Ok;
			} catch (Exception e) {
				errorexception = e;
			}

			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
			}
		}

		errorexception.printStackTrace();
		errorexception.printStackTrace(System.out);
		return Error;
	}

	protected abstract ResultSet scan(String table, String startkey, int recordcount, Set<String> fields);

	/**
	 * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the
	 * specified record key, overwriting any existing values with the same field name.
	 * 
	 * @param table
	 *            The name of the table
	 * @param key
	 *            The record key of the record to write.
	 * @param values
	 *            A HashMap of field/value pairs to update in the record
	 * @return Zero on success, a non-zero error code on error
	 */
	@Override
	public int update(String table, String key, HashMap<String, ByteIterator> values) {
		if (!_columnFamily.equals(table)) {
			_columnFamily = table;
		}

		for (int i = 0; i < OperationRetries; i++) {
			if (_debug) {
				System.out.println("Inserting key: " + key);
			}
			try {
				innerUpdate(table, key, values);
				return Ok;
			} catch (Exception e) {
				errorexception = e;
			}
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
			}

		}
		errorexception.printStackTrace();
		errorexception.printStackTrace(System.out);
		return Error;
	}

	protected abstract void innerUpdate(String table, String key, HashMap<String, ByteIterator> values);

	/**
	 * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the
	 * specified record key.
	 * 
	 * @param table
	 *            The name of the table
	 * @param key
	 *            The record key of the record to insert.
	 * @param values
	 *            A HashMap of field/value pairs to insert in the record
	 * @return Zero on success, a non-zero error code on error
	 */
	@Override
	public int insert(String table, String key, HashMap<String, ByteIterator> values) {
		if (!_columnFamily.equals(table)) {
			_columnFamily = table;
		}

		for (int i = 0; i < OperationRetries; i++) {
			if (_debug) {
				System.out.println("Inserting key: " + key);
			}

			try {
				innerInsert(table, key, values);
				if (_debug) {
					System.out.println("ConsistencyLevel=" + writeConsistencyLevel.toString());
				}

				return Ok;
			} catch (Exception e) {
				errorexception = e;
			}
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
			}
		}

		errorexception.printStackTrace();
		errorexception.printStackTrace(System.out);
		return Error;
	}

	protected abstract void innerInsert(String table, String key, HashMap<String, ByteIterator> values);

	/**
	 * Delete a record from the database.
	 * 
	 * @param table
	 *            The name of the table
	 * @param key
	 *            The record key of the record to delete.
	 * @return Zero on success, a non-zero error code on error
	 */
	@Override
	public int delete(String table, String key) {
		if (!_columnFamily.equals(table)) {
			_columnFamily = table;
		}

		for (int i = 0; i < OperationRetries; i++) {
			try {

				innerDelete(table, key);
				if (_debug) {
					System.out.println("Delete key: " + key);
					System.out.println("ConsistencyLevel=" + deleteConsistencyLevel.toString());
				}

				return Ok;
			} catch (Exception e) {
				errorexception = e;
			}
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
			}
		}
		errorexception.printStackTrace();
		errorexception.printStackTrace(System.out);
		return Error;
	}

	protected abstract void innerDelete(String table, String key);

}
