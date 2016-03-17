import java.util.concurrent.BlockingQueue;

/**
 * Created by sicongfeng on 16/2/19.
 */
public class LatencySim {
    public int latency;

    public void delay(ClientReq req) {
        try {
            Thread.sleep(latency);
        } catch (InterruptedException e) {
        } finally {

        }
    }
}
