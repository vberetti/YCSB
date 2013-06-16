package com.yahoo.ycsb.db;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Vector;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.commons.lang.NotImplementedException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import com.google.common.collect.Iterables;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

public class CassandraClientThriftCQL3 extends DB{
    
    static Random random = new Random();
    public static final int Ok = 0;
    public static final int Error = -1;
    public static final ByteBuffer emptyByteBuffer = ByteBuffer.wrap(new byte[0]);

    public int ConnectionRetries;
    public int OperationRetries;
    public String column_family;

    public static final String CONNECTION_RETRY_PROPERTY = "cassandra.connectionretries";
    public static final String CONNECTION_RETRY_PROPERTY_DEFAULT = "300";

    public static final String OPERATION_RETRY_PROPERTY = "cassandra.operationretries";
    public static final String OPERATION_RETRY_PROPERTY_DEFAULT = "300";

    public static final String USERNAME_PROPERTY = "cassandra.username";
    public static final String PASSWORD_PROPERTY = "cassandra.password";

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
    
    public static final String COMPRESSION_PROPERTY = "cassandra.compression";
    public static final String COMPRESSION_PROPERTY_DEFAULT = "false";

    TTransport tr;
    Cassandra.Client client;

    boolean _debug = false;

    String _table = "";
    String _keyColumnName;
    Exception errorexception = null;

    ColumnParent parent;
   
    ConsistencyLevel readConsistencyLevel = ConsistencyLevel.ONE;
    ConsistencyLevel writeConsistencyLevel = ConsistencyLevel.ONE;
    ConsistencyLevel scanConsistencyLevel = ConsistencyLevel.ONE;
    ConsistencyLevel deleteConsistencyLevel = ConsistencyLevel.ONE;
    
    Compression compression = Compression.NONE;


    /**
     * Initialize any state for this DB. Called once per DB instance; there is one
     * DB instance per client thread.
     */
    public void init() throws DBException
    {
      String hosts = getProperties().getProperty("hosts");
      if (hosts == null)
      {
        throw new DBException("Required property \"hosts\" missing for CassandraClient");
      }

      column_family = getProperties().getProperty(COLUMN_FAMILY_PROPERTY, COLUMN_FAMILY_PROPERTY_DEFAULT);
      _keyColumnName = getProperties().getProperty(KEY_COLUMN_NAME_PROPERTY, KEY_COLUMN_NAME_PROPERTY_DEFAULT);
      parent = new ColumnParent(column_family);

      ConnectionRetries = Integer.parseInt(getProperties().getProperty(CONNECTION_RETRY_PROPERTY,
          CONNECTION_RETRY_PROPERTY_DEFAULT));
      OperationRetries = Integer.parseInt(getProperties().getProperty(OPERATION_RETRY_PROPERTY,
          OPERATION_RETRY_PROPERTY_DEFAULT));
      
      compression = Boolean.parseBoolean(getProperties().getProperty(COMPRESSION_PROPERTY, COMPRESSION_PROPERTY_DEFAULT)) ? Compression.GZIP : Compression.NONE;

      String username = getProperties().getProperty(USERNAME_PROPERTY);
      String password = getProperties().getProperty(PASSWORD_PROPERTY);
      
      readConsistencyLevel = ConsistencyLevel.valueOf(getProperties().getProperty(READ_CONSISTENCY_LEVEL_PROPERTY, READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
      writeConsistencyLevel = ConsistencyLevel.valueOf(getProperties().getProperty(WRITE_CONSISTENCY_LEVEL_PROPERTY, WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
      scanConsistencyLevel = ConsistencyLevel.valueOf(getProperties().getProperty(SCAN_CONSISTENCY_LEVEL_PROPERTY, SCAN_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
      deleteConsistencyLevel = ConsistencyLevel.valueOf(getProperties().getProperty(DELETE_CONSISTENCY_LEVEL_PROPERTY, DELETE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));


      _debug = Boolean.parseBoolean(getProperties().getProperty("debug", "false"));

      String[] allhosts = hosts.split(",");
      String myhost = allhosts[random.nextInt(allhosts.length)];

      Exception connectexception = null;

      for (int retry = 0; retry < ConnectionRetries; retry++)
      {
        tr = new TFramedTransport(new TSocket(myhost, 9160));
        TProtocol proto = new TBinaryProtocol(tr);
        client = new Cassandra.Client(proto);
        try
        {
          tr.open();
          connectexception = null;
          break;
        } catch (Exception e)
        {
          connectexception = e;
        }
        try
        {
          Thread.sleep(1000);
        } catch (InterruptedException e)
        {
        }
      }
      if (connectexception != null)
      {
        System.err.println("Unable to connect to " + myhost + " after " + ConnectionRetries
            + " tries");
        throw new DBException(connectexception);
      }

      if (username != null && password != null)
      {
          Map<String,String> cred = new HashMap<String,String>();
          cred.put("username", username);
          cred.put("password", password);
          AuthenticationRequest req = new AuthenticationRequest(cred);
          try
          {
              client.login(req);
          }
          catch (Exception e)
          {
              throw new DBException(e);
          }
      }
    }

    /**
     * Cleanup any state for this DB. Called once per DB instance; there is one DB
     * instance per client thread.
     */
    public void cleanup() throws DBException
    {
      tr.close();
    }

    /**
     * Read a record from the database. Each field/value pair from the result will
     * be stored in a HashMap.
     *
     * @param table
     *          The name of the table
     * @param key
     *          The record key of the record to read.
     * @param fields
     *          The list of fields to read, or null for all of them
     * @param result
     *          A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error
     */
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result)
    {
      if (!_table.equals(table)) {
        try
        {
          client.set_keyspace(table);
          _table = table;
        }
        catch (Exception e)
        {
          e.printStackTrace();
          e.printStackTrace(System.out);
          return Error;
        }
      }

      for (int i = 0; i < OperationRetries; i++)
      {

        try
        {
            String query = QueryHelper.readQuery(table, _keyColumnName, key, fields, readConsistencyLevel);
            CqlResult rows = client.execute_cql3_query(ByteBuffer.wrap(query.getBytes("UTF-8")), compression, toThriftConsistencyLevel(readConsistencyLevel));

          if (_debug)
          {
            System.out.print("Reading key: " + key);
          }

          String name;
          ByteIterator value;
          CqlRow row = Iterables.getFirst(rows.getRows(), null);
          if(row != null)
          {
        	  
              for(Column column : row.getColumns()){
                  
                  name = new String(column.name.array(), column.name.position()+column.name.arrayOffset(), column.name.remaining());
                  
                  if(column.value != null){
                	  value = new ByteArrayByteIterator(column.value.array(), column.value.position()+column.value.arrayOffset(), column.value.remaining());
                  }else{
                	  value=new ByteArrayByteIterator(new byte[0]);
                  }

                  result.put(name,value);

                  if (_debug)
                  {
                    System.out.print("(" + name + "=" + value + ")");
                  }
              }

          }

          if (_debug)
          {
            System.out.println();
            System.out.println("ConsistencyLevel=" + readConsistencyLevel.toString());
          }

          return Ok;
        } catch (Exception e)
        {
          errorexception = e;
        }

        try
        {
          Thread.sleep(500);
        } catch (InterruptedException e)
        {
        }
      }
      errorexception.printStackTrace();
      errorexception.printStackTrace(System.out);
      return Error;

    }

    /**
     * Perform a range scan for a set of records in the database. Each field/value
     * pair from the result will be stored in a HashMap.
     *
     * @param table
     *          The name of the table
     * @param startkey
     *          The record key of the first record to read.
     * @param recordcount
     *          The number of records to read
     * @param fields
     *          The list of fields to read, or null for all of them
     * @param result
     *          A Vector of HashMaps, where each HashMap is a set field/value
     *          pairs for one record
     * @return Zero on success, a non-zero error code on error
     */
    public int scan(String table, String startkey, int recordcount, Set<String> fields,
        Vector<HashMap<String, ByteIterator>> result)
    {
      throw new NotImplementedException("scan not yet implemented");
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified record
     * key, overwriting any existing values with the same field name.
     *
     * @param table
     *          The name of the table
     * @param key
     *          The record key of the record to write.
     * @param values
     *          A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error
     */
    public int update(String table, String key, HashMap<String, ByteIterator> values)
    {
        if (!_table.equals(table)) {
            try
            {
              client.set_keyspace(table);
              _table = table;
            }
            catch (Exception e)
            {
              e.printStackTrace();
              e.printStackTrace(System.out);
              return Error;
            }
          }

          for (int i = 0; i < OperationRetries; i++)
          {
            if (_debug)
            {
              System.out.println("Updating key: " + key);
            }

            try
            {
              String query = QueryHelper.updateQuery(table, _keyColumnName, key, values, writeConsistencyLevel);
              client.execute_cql3_query(ByteBuffer.wrap(query.getBytes("UTF-8")), compression, toThriftConsistencyLevel(writeConsistencyLevel));

              if (_debug)
              {
                 System.out.println("ConsistencyLevel=" + writeConsistencyLevel.toString());
              }

              return Ok;
            } catch (Exception e)
            {
              errorexception = e;
            }
            try
            {
              Thread.sleep(500);
            } catch (InterruptedException e)
            {
            }
          }

          errorexception.printStackTrace();
          errorexception.printStackTrace(System.out);
          return Error;
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified record
     * key.
     *
     * @param table
     *          The name of the table
     * @param key
     *          The record key of the record to insert.
     * @param values
     *          A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error
     */
    public int insert(String table, String key, HashMap<String, ByteIterator> values)
    {
      if (!_table.equals(table)) {
        try
        {
          client.set_keyspace(table);
          _table = table;
        }
        catch (Exception e)
        {
          e.printStackTrace();
          e.printStackTrace(System.out);
          return Error;
        }
      }

      for (int i = 0; i < OperationRetries; i++)
      {
        if (_debug)
        {
          System.out.println("Inserting key: " + key);
        }

        try
        {
          String query = QueryHelper.insertQuery(table, _keyColumnName, key, values, writeConsistencyLevel);
          client.execute_cql3_query(ByteBuffer.wrap(query.getBytes("UTF-8")), compression, toThriftConsistencyLevel(writeConsistencyLevel));

          if (_debug)
          {
             System.out.println("ConsistencyLevel=" + writeConsistencyLevel.toString());
          }

          return Ok;
        } catch (Exception e)
        {
          errorexception = e;
        }
        try
        {
          Thread.sleep(500);
        } catch (InterruptedException e)
        {
        }
      }

      errorexception.printStackTrace();
      errorexception.printStackTrace(System.out);
      return Error;
    }

    /**
     * Delete a record from the database.
     *
     * @param table
     *          The name of the table
     * @param key
     *          The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error
     */
    public int delete(String table, String key)
    {
      if (!_table.equals(table)) {
        try
        {
          client.set_keyspace(table);
          _table = table;
        }
        catch (Exception e)
        {
          e.printStackTrace();
          e.printStackTrace(System.out);
          return Error;
        }
      }

      for (int i = 0; i < OperationRetries; i++)
      {
        try
        {
            
            String query = QueryHelper.deleteQuery(table, _keyColumnName, key, deleteConsistencyLevel);
            client.execute_cql3_query(ByteBuffer.wrap(query.getBytes("UTF-8")), compression, toThriftConsistencyLevel(deleteConsistencyLevel));

          if (_debug)
          {
            System.out.println("Delete key: " + key);
            System.out.println("ConsistencyLevel=" + deleteConsistencyLevel.toString());
          }

          return Ok;
        } catch (Exception e)
        {
          errorexception = e;
        }
        try
        {
          Thread.sleep(500);
        } catch (InterruptedException e)
        {
        }
      }
      errorexception.printStackTrace();
      errorexception.printStackTrace(System.out);
      return Error;
    }
    
    
    public static org.apache.cassandra.thrift.ConsistencyLevel toThriftConsistencyLevel(ConsistencyLevel consistencyLevel){
        return org.apache.cassandra.thrift.ConsistencyLevel.valueOf(consistencyLevel.name());
    }

  }