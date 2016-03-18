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

    public HeliosScheduler() {
	super();
	localWriteSet = new HashMap<String, Txns>();
	externalWriteSet = new HashMap<String, Txns>();
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
     * true means no conflict, put txn into pt 
     */
    public boolean processCR(String txn_id) {
	Txns txn = currentTxns.get(txn_id);
	if(txn == null) return false; //not a current txn

	if( isConflict(txn) ) {
		currentTxns.remove(txn_id); //abort
		return false;
	}

	t.time = new Timestamp( (new Date()).getTime );

	//following method should be atomic, don't know how to do it???
	pt.addTxns(txn);
	updateLocalSets(txn);
	//update T, don't know which represent local DC???
	log.add(txn);
	//assume currentTxns only contains the txns not send commit request yet, 
	//need to remove txn from currentTxns
	currentTxns.remove(txn_id);
    }

    //if not conflict, only leave the write operations in txn
    public boolean isConflict (Txns txn) {
	for(Operation op : txn.ops) {
	    String opKey = op.table + '_' + op.key;
	    if( localWriteSet.containsKey(opKey) || externalWriteSet.containsKey(opKey) ) return true;
	    if( op instanceof ReadOp && op.isOverwritten(connection) ) return true;
	    } 
	}
	return false;
    }

    public void updateLocalSets(Txns txn) {
	for(Operation op : txn.ops) {
	    if( op instanceof ReadOp ) {
		localReadSet.put(op.table + '_' + op.key, txn);
	    } else if ( op instanceof WriteOp) {
		localWriteSet.put(op.table + '_' + op.key, txn);
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
    //either t is local, or t.time is not after T[DCNum, t.hostDCNum], no update, false
    public boolean updateT(Txns t) {
	if( t.isLocal(this) ) return false;
	Timestamp txnTS = t.time;
	int remoteDC = t.hostDCNum;
	ArrayList<Timestamp> thisKnowAll = T.get(DCNum);
	if( txnTS.after ( thisKnowAll.get(remoteDC) ) ) {
		thisKnowAll.set(remoteDC, txnTS);
		return true;
	} else {
		return false;
	}
    }
}