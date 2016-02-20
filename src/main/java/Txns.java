import Operations.Operation;

import java.sql.Timestamp;
import java.util.ArrayList;

/**
 * Created by sicongfeng on 16/2/19.
 */
public class Txns {
    public ArrayList<Operation> ops;
    public Timestamp time;
    public txnsState state;

    public void commit() {}

    public void abort() {}

    public boolean checkForConflict(Txns t) {
        return false;
    }

}

enum txnsState {
    ACTIVE, COMMIT, ABORT
}
