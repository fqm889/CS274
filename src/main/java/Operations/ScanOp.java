package Operations;

<<<<<<< HEAD
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
 * Created by Xin on 16/3/16
 */

public class ScanOp extends Operation {
    private int recordcount;
    private Set<String> fields; 
    private Vector<HashMap<String,ByteIterator>> result;

    public ScanOp (Table table, String key, int recordcount, 
			Set<String> fields, Vector<HashMap<String,ByteIterator>> result) {
	this.table = table;
	this.key = key; //the startkey to scan
	this.recordcount = recordcount;
	this.fields = fields;
	this.result = result;
    }

    @Override
    public Status doOp(DB db) {
	return db.scan(table, key, recordcount, fields, result);
    }

    //Assume undoOp() for ScanOp is always successful
    //don't need to keep the previous value of result
    @Override
    public Status undoOp(DB db) {
        return Status.OK;
    }
}
