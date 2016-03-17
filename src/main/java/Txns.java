import Operations.Operation;

import java.sql.Timestamp;
import java.util.ArrayList;

/**
 * Created by sicongfeng on 16/2/19.
 */
public class Txns {
    public ArrayList<Operation> ops;
    public Timestamp time;
    public Timestamp tLPT;
    public txnsState state;
    public boolean processed;
    public boolean pending;
    public boolean local;

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

    public void abort() {}

    public void write() {}

    public boolean checkForConflict(Txns t) {
        return false;
    }

}

enum txnsState {
    ACTIVE, COMMIT, ABORT
}
