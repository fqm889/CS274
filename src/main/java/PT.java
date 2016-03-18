import java.util.ArrayList;

/**
 * Created by sicongfeng on 16/2/19.
 */
public class PT {
    ArrayList<Txns> pt;

    public ArrayList<Txns> getPt() {
        return pt;
    }



    public PT() {
	pt = new ArrayList<Txns>();
    }

    public PT addTxns(Txns t) {
	pt.add(t);
        return this;
    }

    public PT delTxns(Txns t) {
	pt.remove(t);
        return this;
    }

    public PT GC() {
        return this;
    }

    public PT alterTxns(Txns t) {
        return this;
    }

    public PT execTxns(Txns t) {
        return this;
    }


}
