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

public abstract class WriteOp extends Operation {
  public static String TS_TABLE = "TimeStampTable";

  public boolean updateTimetamp(String table, String key, Connection connection) {
      TableName tName = TableName.valueOf(TS_TABLE);
      Table currentTable;
      try {
        currentTable = connection.getTable(tName);
      } catch (IOException e) {
        return false;
      }

      Put p = new Put(Bytes.toBytes(table));
      long ts = ( new Date() ).getTime();
      p.addColumn(Bytes.toBytes(key), Bytes.toBytes(ts));

    try {
        currentTable.put(p);
    } catch (xception e) {
      return false;
    }
    return true;
  }
      
  }
}