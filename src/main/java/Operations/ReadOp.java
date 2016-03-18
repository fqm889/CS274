package Operations;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import java.sql.Timestamp;
import java.util.Date;

/**
 * Created by sicongfeng on 16/2/19.
 * modified by Xin on 16/2/24
 */

public class ReadOp extends Operation {
    public Set<String> fields; 
    public HashMap<String,ByteIterator> result;
    //for Helios to check overwritten
    public long ts;  // use current time as ts
    public String columnFamily = "";  

    public ReadOp (String columnFamily, String table, String key, 
		Set<String> fields, HashMap<String,ByteIterator> result) {
	this.columnFamily = columnFamily;
	this.table = table;
	this.key = key;
	this.fields = fields;
	this.result = result;
    }

    @Override
    public Status doOp(Connection connection) {
      TableName tName = TableName.valueOf(table);
      byte[] columnFamilyBytes = Bytes.toBytes(columnFamily);
      Table currentTable;
      try {
        currentTable = connection.getTable(tName);
      } catch (IOException e) {
        System.err.println("Error accessing HBase table: " + e);
        return Status.ERROR;
      }

    Result r = null;
    try {
      Get g = new Get(Bytes.toBytes(key));
      if (fields == null) {
        g.addFamily(columnFamilyBytes);
      } else {
        for (String field : fields) {
          g.addColumn(columnFamilyBytes, Bytes.toBytes(field));
        }
      }
      r = currentTable.get(g);
    } catch (Exception e) {
      return Status.ERROR;
    } 

    if (r.isEmpty()) {
      return Status.NOT_FOUND;
    }

    while (r.advance()) {
      final Cell c = r.current();
      result.put(Bytes.toString(CellUtil.cloneQualifier(c)),
          new ByteArrayByteIterator(CellUtil.cloneValue(c)));
    }
    ts = ( new Date() ).getTime();
    return Status.OK;
    }


    //Assume undoOp() for readOp is always successful
    //don't need to keep the previous value of result
    @Override
    public Status undoOp(Connection connection) {
        return Status.OK;
    }


    //for Helios, to get readSet
    public String getTableKey() {
	return table + '_' + key;
    }

    public boolean isOverwritten(Connection connection) {
      TableName tName = TableName.valueOf(WriteOp.TS_TABLE);
      Table currentTable;
      try {
        currentTable = connection.getTable(tName);
      } catch (IOException e) {
        return false;
      }

    Result r = null;
    try {
      Get g = new Get(Bytes.toBytes(table));
      g.addFamily(Bytes.toBytes(key));
      r = currentTable.get(g);
    } catch (Exception e) {
      return false;
    } 

    if (r.isEmpty()) {
      return false;	
    }
      long currentTs = Bytes.toLong( r.value() );

    if(ts <= currentTs) return false;
    else return true;
}
