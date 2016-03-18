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


/**
 * Created by Xin on 16/3/16
 */

public class ScanOp extends Operation {

    //don't put scan set into the readSet to check conflict
    //no timestamp for scan
    public int recordcount;
    public Set<String> fields; 
    public Vector<HashMap<String,ByteIterator>> result;

    public ScanOp (String columnFamily, String table, String key, int recordcount, 
			Set<String> fields, Vector<HashMap<String,ByteIterator>> result) {
	this.columnFamily = columnFamily;
	this.table = table;
	this.key = key; //the startkey to scan
	this.recordcount = recordcount;
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

    Scan s = new Scan(Bytes.toBytes(key));
    // HBase has no record limit. Here, assume recordcount is small enough to
    // bring back in one call.
    // We get back recordcount records
    s.setCaching(recordcount);

    // add specified fields or else all fields
    if (fields == null) {
      s.addFamily(columnFamilyBytes);
    } else {
      for (String field : fields) {
        s.addColumn(columnFamilyBytes, Bytes.toBytes(field));
      }
    }

    // get results
    ResultScanner scanner = null;
    try {
      scanner = currentTable.getScanner(s);
      int numResults = 0;
      for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
        // get row key
        String key = Bytes.toString(rr.getRow());

        HashMap<String, ByteIterator> rowResult =
            new HashMap<String, ByteIterator>();

        while (rr.advance()) {
          final Cell cell = rr.current();
          rowResult.put(Bytes.toString(CellUtil.cloneQualifier(cell)),
              new ByteArrayByteIterator(CellUtil.cloneValue(cell)));
        }

        // add rowResult to result vector
        result.add(rowResult);
        numResults++;

        // PageFilter does not guarantee that the number of results is <=
        // pageSize, so this
        // break is required.
        if (numResults >= recordcount) {// if hit recordcount, bail out
          break;
        }
      } // done with row
    } catch (IOException e) {
      return Status.ERROR;
    } finally {
      if (scanner != null) {
        scanner.close();
      }
    }

    return Status.OK;
  }

    //Assume undoOp() for ScanOp is always successful
    //don't need to keep the previous value of result
    @Override
    public Status undoOp(Connection connection) {
        return Status.OK;
    }
}
