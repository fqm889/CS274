
import java.util.ArrayList;

/**
 * Created by Xin Liu on 16/3/15.
 */
public class HeliosLogProcessor extends LogProcessor {
    // pass in the Log of Scheduler, process log every time the Scheduler calls it
    // executes on its own list, scheduler may access its list and decide which one to send
    // Helios Alg 2, the processed log record need to be removed from log???

    public HeliosScheduler scheduler;

    @Override
    public void processLog() {
        ArrayList<Txns> ls = log.getLog();
	int nextIndex = 0;
        for (Txns t : ls) {
            if ( t.isLocal() ) { continue; }
	    updateLocalPt (t, scheduler);            
	    if (t.getState()==txnsState.ACTIVE) {
		scheduler.addEPT(t);
	    }
            if (t.getState()==txnsState.COMMIT) {
                t.write();
		scheduler.removeEPT(t);
            }
	    //update T according to t.time and host DC
	    scheduler.updateT( t );
	    nextIndex++; //cannot derectly delete t, use nextIndex to point to the next Txns to process
        }
	// all the records in log has been processed
	if( nextIndex == ls.size() ) log.setLog( new ArrayList<Txns>() );
	else log.setLog( subList(nextIndex, ls.size() ) );
    }

    //check externel txn conflict with local preparing Txns
    public void updateLocalPt (Txns txn, HeliosScheduler hScheduler) {
	for(Operation op : txn.ops) {
	    String opKey = op.table + '_' + op.key;
	    if( localWriteSet.containsKey(opKey) ) {
		hScheduler.abortTxnInPt( localWriteSet.get(opKey) );
	    }
	    if( localReadSet.containsKey(opKey) ) {
		hScheduler.abortTxnInPt( localReadSet.get(opKey) );
	    }
	}
    }
}