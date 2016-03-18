import Operations.Operation;

import java.sql.Timestamp;
import java.util.ArrayList;

/**
 * Created by sicongfeng on 16/2/19.
 */
public class Txns {
    public int hostDCNum;
    public ArrayList<Operation> ops;
    public Timestamp time;
    public Timestamp tLPT;
    public txnsState state;
    public boolean processed;
    public boolean pending;
    public boolean local;

    public void setHostDCNum(int num) {
        hostDCNum = num;
    }

    public int getHostDCNum() {
        return hostDCNum;
    }

    public void AddOp(Operation op) {
        ops.add(op);
    }

    public boolean isProcessed() {
        return processed;
    }

    public void setProcessed(boolean processed) {
        this.processed = processed;
    }

    public boolean isPending() {
        return pending;
    }

    public void setPending(boolean pending) {
        this.pending = pending;
    }

    public boolean isLocal(Scheduler s) {
        return s.DCNum == this.hostDCNum;
    }

    public boolean isLocal() {
        return local;
    }

    public void setLocal(boolean local) {
        this.local = local;
    }

    public txnsState getState() {
        return state;
    }

    public void setState(txnsState state) {
        this.state = state;
    }

    public void commit() {
        state = txnsState.COMMIT;
    }

    public void abort() {
        state = txnsState.ABORT;
    }

    public void write() {
	for(Operation op : ops) {
		if(op instance WriteOp) op.doOp();
	}
    }

    //check this read set and write set not intersect t's write set
    public boolean checkForConflict(Txns t) {
        return false;
    }

}

enum txnsState {
    ACTIVE, COMMIT, ABORT
}
