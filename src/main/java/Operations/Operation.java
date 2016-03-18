package Operations;

import org.apache.hadoop.hbase.client.Connection;

/**
 * Created by sicongfeng on 16/2/19.
 * Modified by Xin Liu on 16/3/16.
 */


public abstract class Operation {
    //the name of the table
    public String table;
    //The record key of the record to oprate
    public String key;
    public abstract Status doOp(Connection connection);
    public abstract Status undoOp(Connection connection);

    public enum Status {
	OK, ERROR, NOT_FOUND
    }
}
