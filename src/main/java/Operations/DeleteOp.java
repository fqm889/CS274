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

public class DeleteOp extends Operation {

    private HashMap<String,ByteIterator> preValues;

    public DeleteOp (String table, String key, Set<String> fields) {
	this.table = table;
	this.key = key;
	preValues = new HashMap<String,ByteIterator>();
    }

    @Override
    public Status doOp(DB db) {
	//get previous values for the row of (table: key)
	Status readPre = db.read(table, key, null, preValues);
	//error when read previous values
	if( readPre.equals(Status.ERROR) ) return Status.ERROR;
	//row not found
	else if( readPre.equals(Status.NOT_FOUND) ) {
		preValues = null;
		return Status.OK;
	}
	return db.delete(table, key);
    }

    @Override
    public Status undoOp(DB db) {
	if(preValues == null) {
        	return Status.OK;
	}
	return db.insert(table, key, preValues);
    }
}
