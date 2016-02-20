import Operations.Operation;

import java.sql.Timestamp;
import java.util.ArrayList;

/**
 * Created by sicongfeng on 16/2/19.
 */
public abstract class Request {
    // abstract class for all kinds of requests

    Txns req; // request consists of operations and other metadata
    String from;
    String to;

    public Request(String s) {
        decode(s);
    }

    public abstract String encode(); // encode request into a string

    public abstract void decode(String s); // decode a string to request
}
