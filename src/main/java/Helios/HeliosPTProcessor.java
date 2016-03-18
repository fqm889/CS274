
import java.util.ArrayList;
import java.sql.Timestamp;
import java.util.ArrayList;

/**
 * Created by Xin Liu on 16/3/16.
 * Helios Alg 3
 */
public class HeliosPTProcessor extends PTProcessor {
    public HeliosScheduler hs;

    @override
    public void processPT() {
	ArrayList<Txns> tlist = pt.pt;
	boolean toCommit = true;
	for(Txns t : tlist) {
		//for each DC X, if T[local, X] < t.ktsX, toCommit = false;
		if( !toCommit) break;
		else {
			t.write();
			t.commit();
			t.time = new Timestamp( (new Date()).getTime );
			log.addTxns(t); 
			//update T[local, local], propagate t??? 
		} 
	}
    }
}