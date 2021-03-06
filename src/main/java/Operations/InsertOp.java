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
 * modified by Xin on 16/3/16
 */

public class InsertOp extends WriteOp {
    public HashMap<String,ByteIterator> values;
    public String columnFamily = "";  
    public Durability durability = Durability.USE_DEFAULT;
    UpdateOp updateOp;

    public InsertOp (String columnFamily, String table, String key, HashMap<String,ByteIterator> values) {
	this.columnFamily = columnFamily;
	this.table = table;
	this.key = key;
	this.values = values;
	preValues = new HashMap<String,ByteIterator>();	
	this.updateOp = new UpdateOp(columnFamily, table, key, 
			values);
    }

    @Override
    public Status doOp(Connection connection) {
	updateOp.doOp(connection);
    }

    @Override
    public Status undoOp(Connection connection) {
	if(preValues == null) {
        	return db.delete(table, key);
	}
	Status deleteStatus = db.delete(table, key)
	if( !deleteStatus.equlas(Status.OK) ) return deleteStatus;
	return db.insert(table, key, preValues);
    }
}
