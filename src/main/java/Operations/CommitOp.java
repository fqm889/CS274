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
 */
public class CommitOp extends Operation {

    List<WriteOp> toWriteList;
    Stack<WriteOp> writeDoneStack;


    public CommitOp(List<WriteOp> toWriteList) {
	this.toWriteList = toWriteList;
	writeDoneStack = new Stack<WriteOp>();
    }

    @Override
    public boolean doOp() {
	boolean toAbort = false;
	for(WriteOp w : toWriteList) {
		if( w.doOp() ) {
			writeDoneStack.push(w);
		} else {
			toAbort = true;
		}
	}

	if( toAbort ) {
		while( !writeDoneStack.isEmpty() ) {
			WriteOp w = writeDoneStack.pop();
			w.undoOp();
		}
	}

        return !toAbort;
    }

    @Override
    public boolean undoOp() {
        return false;
    }
}
