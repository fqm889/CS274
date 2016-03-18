
import java.util.ArrayList;
import java.sql.Timestamp;


/**
 * Created by Xin Liu on 16/3/15.
 * Helios Alg 3
 */
public class HeliosPTProcessor extends PTProcessor {
    public HeliosScheduler hs;

    @override
    public void processPT() {
	ArrayList<Txns> tlist = pt.pt;
	for (Iterator<Txns> it = tlist.iterator(); it.hasNext();) {
		Txns t = it.next;
		if( !timeToCommit(t.time, hs.T, hs.DCNum) ) break;
		else {
			// apply t.write-set to local DC
			t.write();
			//update local
			hs.commitTxnInPt(t); // t is removed in this method
			//update T[local, local], propagate t??? 
			updateT(txn);
			log.add(txn);
		}    		
	}
    }

    //Alg 3, check whether T[hostDC, X] >= t.kts(X) for all DC X
    public boolean timeToCommit(Timestemp txnTS, 
		ArrayList<ArrayList<Timestamp>> T, int hostDC) {
	ArrayList<Timestamp> hostKnowAll = T.get(hostDC);
	for(int i = 0; i < hostKnowAll.size(); i++) {
		if(i == hostDC) continue;
		if(txnTS.after( hostKnowAll.get(i) ) return false;
	}
	return true;
    }
}