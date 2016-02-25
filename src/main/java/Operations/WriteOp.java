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
 * simpply change the the value of key in table from beforeVal to afterVal
 * table should be provided by scheduler
 */

public class WriteOp extends Operation {
    private Table table;
    private String key;
    private boolean existBefore; // the row of key whether exsits before this write op
    private String beforeVal; 
    private String afterVal;

    public static String FAMILY = "value";
    public static String FAMILY_BYTES = Bytes.toBytes(FAMILY);
    public static String QUALIFIER = "value";
    public static String QUALIFIER_BYTES = Bytes.toBytes(QUALIFIER);

    public WriteOp (Table table, String key, String afterVal) {
	this.table = table;
	this.key = key;
	this.afterVal = afterVal;
    }

    @Override
    public boolean doOp() {
	getBeforeVal();
	Put p = new Put(Bytes.toBytes(key));
	p.addColumn(FAMILY_BYTES, QUALIFIER_BYTES, Bytes.toBytes(afterValue) );
	try {
		table.put(p);
		return true;
	} catch(Exception e) {
		return false;
	}
    }

    @Override
    public boolean undoOp() {
	if(existBefore) {
		Put p = new Put(Bytes.toBytes(key));
		p.addColumn(FAMILY_BYTES, QUALIFIER_BYTES, Bytes.toBytes(beforeValue) );
		try {
			table.put(p);
			return true;
		} catch(Exception e) {
			return false;
		}
	} else {
		Delete d = new Delete(Bytes.toBytes(key));
		table.delete(d);
	}

        return false;
    }

    private boolean getBeforeVal() {
	beforeVal = null;
	try {
		Get g = new Get(Bytes.toBytes(key));
		Result r = table.get(get);
		if ( r.isEmpty() ) {
			//key is not found
			existBefore = false;
			return true;
		} else {
			existBefore = true;
			beforeVal = new String( r.value() );
			return true;
		}
	} finally {
        	return false;
	}
    }
}
