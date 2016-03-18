import Operations.*;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.client.Connection;

import java.sql.Timestamp;
import java.util.ArrayList;

/**
 * Created by Xin Liu on 16/3/16.
 */

public class HeliosScheduler extends Scheduler {
    // read & write set of PTPool
    public ConcurrentHashMap<String, Txns> localReadSet;
    public ConcurrentHashMap<String, Txns> localWriteSet;
    // write set of EPTPool
    public ConcurrentHashMap<String, List<Txns>> externalWriteSet;
    // assume all coTime is 0
    // public ArrayList<ArrayList<Timestamp>> coTime;

    public HeliosScheduler() {
	// super();
	localReadSet = new HashMap<String, Txns>();
	localWriteSet = new HashMap<String, Txns>();
	externalWriteSet = new HashMap<String, List<Txns>>();
    }

    @Override
    public void algorithm() {
        DBS db = new DBS("127.0.0.1", 9999);
        try {
            db.start();
        } catch (DBException e) {
            e.printStackTrace();
        }
        int count = 0;

        while (true) {
            // check receiver for new request from other DC
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
		//add the received request (txns) to log, how to????
                e.printStackTrace();
            }
            if (count++<10)
                db.read("a", "a", new HashSet<String>(), new HashMap<String, ByteIterator>());

            // check client for new request from client



            // logProcessor

            // ptProcessor

            // send request to other DC
        }
    }

    /**
     * Helios Alg 1
     * process commit request sent by client
     * false means has conflict, abort txn_id 
     * true means no conflict, put txn into pt, put its read set and write set into localSets 
     * for read-only txn, true means direcly commit, no need to preparing and waiting 
     */
    public boolean processCR(String txn_id) {
	Txns txn = currentTxns.get(txn_id);
	if(txn == null) return false; //not a current txn

	//read-only txn don't need to check conflist with PT or EPT
	if( txn.isReadOnly() ) {
		if( isOverwritten(txn) ) {
			txn.abort();			
			currentTxns.remove(txn_id);
			return false; //need to send to client txn has abort
		} else {
			txn.commit();			
			currentTxns.remove(txn_id);
			return true; //need to send to client txn has commit
			//not same as other preparing txn ????
		}
	}

	if( isConflict(txn) ) {
		currentTxns.remove(txn_id); //abort
		return false;
	}

	t.time = new Timestamp( (new Date()).getTime );

	//following method should be atomic
	pt.addTxns(txn);
	updateLocalSets(txn);
	//update T according to t.time
	updateT(txn);
	log.add(txn);
	//assume currentTxns only contains the txns not send commit request yet, 
	//need to remove txn from currentTxns
	currentTxns.remove(txn_id);
    }

    public boolean isConflict (Txns txn) {
	for(Operation op : txn.ops) {
	    String opKey = op.table + '_' + op.key;

	    //read-set or write-set intersect localWriteSet or externalWriteSet
	    if( localWriteSet.containsKey(opKey) || externalWriteSet.containsKey(opKey) ) return true;

	    //read object has been overwritten 
	    if( op instanceof ReadOp && op.isOverwritten(connection) ) return true; 
	}
	return false;
    }

    public boolean isOverwritten (Txns txn) {
	for(Operation op : txn.ops) {
	    //read object has been overwritten 
	    if( op instanceof ReadOp && op.isOverwritten(connection) ) return true; 
	}
	return false;
    }

    //put the read-set of txn into localReadSet
    //put the write-set of txn into localWriteSet
    public void updateLocalSets(Txns txn) {
	for(Operation op : txn.ops) {
	    if( op instanceof ReadOp ) {
		localReadSet.put(op.table + '_' + op.key, txn);
	    } else if ( op instanceof WriteOp) {
		localWriteSet.put(op.table + '_' + op.key, txn);
		containsWriteOp = true;
	    }
	}
    }

    //update local read and write sets, remove txn from pt
    public void commitTxnInPt(Txns txn) {
	for(Operation op : txn.ops) {
	    if( op instanceof ReadOp ) {
		localReadSet.remove(op.table + '_' + op.key);
	    } else if ( op instanceof WriteOp) {
		localWriteSet.remove(op.table + '_' + op.key);
	    }
	}
	txn.time = new Timestamp( (new Date()).getTime );
	txn.setState(txnsState.COMMIT);
	pt.delTxns(txn);
	log.addTxns(txn);
	//after addTxns(), should propagate this txn, is the propagation included in addTxns()???
    }

    public void abortTxnInPt(Txns txn) {
	for(Operation op : txn.ops) {
	    if( op instanceof ReadOp ) {
		localReadSet.remove(op.table + '_' + op.key);
	    } else if ( op instanceof WriteOp) {
		localWriteSet.remove(op.table + '_' + op.key);
	    }
	}
	txn.time = new Timestamp( (new Date()).getTime );
	txn.setState(txnsState.ABORT);
	pt.delTxns(txn);
	log.addTxns(txn);
	//after addTxns(), should propagate this txn, is the propagation included in addTxns()???
    }

    public void addEPT(Txns txn) {
	for(Operation op : txn.ops) {
	    if ( op instanceof WriteOp) {
		String opKey = op.table + '_' + op.key;
		List<Txns> tl = externalWriteSet.get(opKey);
		if (tl != null) tl.add(txn);
		else {
			tl = new LinkedList<Txns>();
			tl.add(txn);
			externalWriteSet.put(opKey, tl);
		}
	    }
	}
    }

    //it's possible that more than one external PT need to write on the same object,
    // local DC will not decide whether these PT commit or abort
    // just bookkeeping all the not finish external PT's write-set
    public void removeEPT(Txns txn) {
	for(Operation op : txn.ops) {
	    if ( op instanceof WriteOp) {
		String opKey = op.table + '_' + op.key;
		List<Txns> tl = externalWriteSet.get(opKey);
		tl.remove(txn);
		if( tl.isEmpty() ) externalWriteSet.remove(opKey);
	    }
	}
    }

    //update T[DCNum, t.hostDCNum] = t.time, true
    //if t.time is not after T[DCNum, t.hostDCNum], no update, false, should not happen
    // t can either be a local txn (become preparing or finish) or an external txn (P or F)
    public boolean updateT(Txns t) {
	Timestamp txnTS = t.time;
	int txnHost = t.hostDCNum;
	ArrayList<Timestamp> thisKnowAll = T.get(DCNum);
	if( txnTS.after ( thisKnowAll.get(txnHost) ) ) {
		thisKnowAll.set(txnHost, txnTS);
		return true;
	} else {
		return false;
	}
    }
}