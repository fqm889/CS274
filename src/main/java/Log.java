import java.util.ArrayList;

/**
 * Created by sicongfeng on 16/2/19.
 */
public class Log {
    ArrayList<Txns> log;

    public Log() {
	log = new ArrayList<Txns>();
    }

    public ArrayList<Txns> getLog() {
        return log;
    }

    public Log addTxns(Txns t) {
	log.add(t);
        return this;
    }

    public Log delTxns(Txns t) {
	log.remove(t);
        return this;
    }

    public Log GC() {
        return this;
    }

    public Log alterTxns(Txns t) {
        return this;
    }

    public Log execTxns(Txns t) {
        return this;
    }
}
