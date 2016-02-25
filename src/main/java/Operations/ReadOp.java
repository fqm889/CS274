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
 * Created by sicongfeng on 16/2/19.
 * modified by Xin on 16/2/24
 * simpply read from table the value of key in HBase
 * table should be provided by scheduler
 */

public class ReadOp extends Operation {
    private Table table;
    private String key;
    private String val;

    public ReadOp (Table table, String key) {
	this.table = table;
	this.key = key;
    }

    @Override
    public boolean doOp() {
	val = null;
	try {
		Get g = new Get(Bytes.toBytes(key));
		Result r = table.get(get);
		if ( r.isEmpty() ) {
			//key is not found
			return false; 
		} else {
			val = new String( r.value() );
			return true;
		}
	} finally {
        	return false;
	}
    }

    @Override
    public boolean undoOp() {
        return false;
    }

    public void setKey(String key) {
	this.key = key;
    }

    public String getKey() {
	return key;
    }

    public String getVal() {
	return val;
    }
}
