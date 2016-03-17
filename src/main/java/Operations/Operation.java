package Operations;

import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.DB;


/**
 * Created by sicongfeng on 16/2/19.
 * Modified by Xin Liu on 16/3/16.
 */


public abstract class Operation {
    //the name of the table
    private String table;
    //The record key of the record to oprate
    private String key;
    public abstract Status doOp(DB db);
    public abstract Status undoOp(DB db);

}
