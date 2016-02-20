import java.util.ArrayList;

/**
 * Created by sicongfeng on 16/2/19.
 */
public class PTProcessor {
    // similar to log processor
    // might need different kinds of PT and PTProcessors

    public Log log;
    public PT pt;
    ArrayList<Txns> waitinglist;

    public PTProcessor(Log log, PT pt) {}

    public void checkPT() {}

    public void processPT() {}
}
