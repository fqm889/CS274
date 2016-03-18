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

import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.DB;

/**
 * Created by sicongfeng on 16/2/19.
 * modified by Xin on 16/2/24
 */

public class DeleteOp extends WriteOp {
    public Durability durability = Durability.USE_DEFAULT;

 //   private HashMap<String,ByteIterator> preValues;

    public DeleteOp (String table, String key) {
	this.table = table;
	this.key = key;
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

    Delete d = new Delete(Bytes.toBytes(key));
    d.setDurability(durability);
    try {
        currentTable.delete(d);
    } catch (IOException e) {
      return Status.ERROR;
    }

    // update the timestamp for table:key in the table "TimeStampTable"
    updateTimetamp(table, key, connection);
    return Status.OK;	
  }

    @Override
    public Status undoOp(Connection connection) {
	return Status.OK;
    }
}
