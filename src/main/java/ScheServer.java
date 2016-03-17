import java.io.IOException;

/**
 * Created by sicongfeng on 16/3/15.
 */
public class ScheServer extends Server {
    public ScheServer(String ip, int port, Scheduler scheduler) throws IOException {
        super(ip, port, scheduler);
    }
}
