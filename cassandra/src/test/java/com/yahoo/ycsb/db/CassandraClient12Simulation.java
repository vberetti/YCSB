package com.yahoo.ycsb.db;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static com.yahoo.ycsb.db.AbstractCassandraClient12.Ok;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Vector;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

public class CassandraClient12Simulation {

	private static AbstractCassandraClient12 cli = new CassandraClient12PS();
	
	public static void main(String[] args) {

		Properties props = new Properties();

		props.setProperty("hosts", "127.0.0.1");
		props.setProperty("cassandra.operationretries", "2");
		cli.setProperties(props);

		try {
			cli.init();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}

		String table = "usertest";
		String ageColumn = "age";
		String middlenameColumn = "middlename";
		String favoriteColorColumn = "favoritecolor";

		HashMap<String, ByteIterator> vals = new HashMap<String, ByteIterator>();
		vals.put(ageColumn, new StringByteIterator("57"));
		vals.put(middlenameColumn, new StringByteIterator("bradley"));
		vals.put(favoriteColorColumn, new StringByteIterator("blue"));
		int res = cli.insert(table, "BrianFrankCooper", vals);
		System.out.println("Result of insert: " + res);

		HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
		HashSet<String> fields = newHashSet();
		fields.add(middlenameColumn);
		fields.add(ageColumn);
		fields.add(favoriteColorColumn);
		res = cli.read(table, "BrianFrankCooper", null, result);
		System.out.println("Result of read: " + res);
		for (String s : result.keySet()) {
			System.out.println("[" + s + "]=[" + result.get(s) + "]");
		}

		res = cli.delete(table, "BrianFrankCooper");
		System.out.println("Result of delete: " + res);
		
		result.clear();
		res = cli.read(table, "BrianFrankCooper", null, result);
		System.out.println("Result of read: " + res);
		for (String s : result.keySet()) {
			System.out.println("[" + s + "]=[" + result.get(s) + "]");
		}

		res = Ok;
		vals = new HashMap<String, ByteIterator>();
		vals.put(ageColumn, new StringByteIterator("30"));
		vals.put(middlenameColumn, new StringByteIterator("John"));
		vals.put(favoriteColorColumn, new StringByteIterator("yellow"));
		res += cli.insert(table, "John", vals);
		vals = new HashMap<String, ByteIterator>();
		vals.put(ageColumn, new StringByteIterator("31"));
		vals.put(middlenameColumn, new StringByteIterator("Paul"));
		vals.put(favoriteColorColumn, new StringByteIterator("yellow"));
		res += cli.insert(table, "Paul", vals);
		vals = new HashMap<String, ByteIterator>();
		vals.put(ageColumn, new StringByteIterator("32"));
		vals.put(middlenameColumn, new StringByteIterator("Ringo"));
		vals.put(favoriteColorColumn, new StringByteIterator("yellow"));
		res += cli.insert(table, "Ringo", vals);
		vals = new HashMap<String, ByteIterator>();
		vals.put(ageColumn, new StringByteIterator("33"));
		vals.put(middlenameColumn, new StringByteIterator("George"));
		vals.put(favoriteColorColumn, new StringByteIterator("yellow"));
		res += cli.insert(table, "George", vals);
		System.out.println("Result of inserts: " + res);

		Vector<HashMap<String, ByteIterator>> results = new Vector<HashMap<String, ByteIterator>>();
		res = cli.scan(table, "John", 2, null, results);
		System.out.println("Result of scan: " + res);

		HashMap<String, ByteIterator> updates = newHashMap();
		updates.put(ageColumn, new StringByteIterator("70"));
		res = cli.update(table, "Paul", updates);
		System.out.println("Result of update: " + res);

		result.clear();
		res = cli.read(table, "Paul", null, result);
		System.out.println("Result of read: " + res);
		for (String s : result.keySet()) {
			System.out.println("[" + s + "]=[" + result.get(s) + "]");
		}

		res = Ok;
		res += cli.delete(table, "John");
		res += cli.delete(table, "Paul");
		res += cli.delete(table, "Ringo");
		res += cli.delete(table, "Georges");
		System.out.println("Result of deletes: " + res);
	}
}
