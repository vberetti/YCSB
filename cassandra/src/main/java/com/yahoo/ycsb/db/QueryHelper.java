package com.yahoo.ycsb.db;

import static com.google.common.collect.Iterables.transform;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.cassandra.db.ConsistencyLevel;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.yahoo.ycsb.ByteIterator;

public class QueryHelper {

    public static String readQuery(String table, String keyColumnName, String key, Set<String> fields, ConsistencyLevel consistencyLevel) {
        StringBuilder query = new StringBuilder("SELECT ");
        if (fields != null) {
            query.append(Joiner.on(',').join(fields) + " ");
        } else {
            query.append(" * ");
        }
        query.append("FROM " + table);
        query.append(" WHERE " + keyColumnName + " = '" + key + "'");
       // query.append(" WITH CONSISTENCY " + consistencyLevel);
        return query.toString();
    }

    public static String updateQuery(String table, String keyColumnName, String key, HashMap<String, ByteIterator> values, ConsistencyLevel consistencyLevel) {
        StringBuilder updateQuery = new StringBuilder("UPDATE " + table + " SET ");
        updateQuery.append(Joiner.on(',').join(transform(values.entrySet(), fieldAssignmentFunction)));
        updateQuery.append(" WHERE " + keyColumnName + "= '" + key + "'");

        return updateQuery.toString();
    }

    public static Function<Map.Entry<String, ByteIterator>, String> fieldAssignmentFunction = new Function<Map.Entry<String, ByteIterator>, String>() {
        @Override
        @Nullable
        public String apply(@Nullable Map.Entry<String, ByteIterator> item) {
            return " " + item.getKey() + " = '" + item.getValue().toString().replaceAll("\'", "\'\'") + "' ";
        }
    };

    public static String insertQuery(String table, String keyColumnName, String key, HashMap<String, ByteIterator> values, ConsistencyLevel consistencyLevel) {
        StringBuilder insertQuery = new StringBuilder("INSERT INTO " + table + "(" + keyColumnName + ",");
        insertQuery.append(Joiner.on(',').join(values.keySet()));
        insertQuery.append(") VALUES ('" + key + "',");
        insertQuery.append(Joiner.on(',').join(transform(values.values(), valueInsertFunction)));
        insertQuery.append(")");
       // insertQuery.append(" WITH CONSISTENCY " + consistencyLevel);
        return insertQuery.toString();
    }

    private static Function<ByteIterator, String> valueInsertFunction = new Function<ByteIterator, String>() {
        @Override
        @Nullable
        public String apply(@Nullable ByteIterator item) {
            return "'" + item.toString().replaceAll("\'", "\'\'") + "'";
        }
    };

    public static String deleteQuery(String table, String keyColumnName, String key, ConsistencyLevel consistencyLevel) {
        StringBuilder query = new StringBuilder("DELETE FROM " + table + " WHERE " + keyColumnName + "= '" + key + "'");
        //query.append(" WITH CONSISTENCY " + consistencyLevel);
        return query.toString();
    }
}
