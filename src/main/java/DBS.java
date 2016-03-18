/**
 * Created by sicongfeng on 16/3/13.
 */
import java.io.IOException;
import java.net.Socket;
import java.util.*;


public class DBS {
    /**
     * Initialize any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    String txn_id;
    String IP;
    int port;
    Socket txn_connection;

    public DBS(String IP, int port) {
        this.IP = IP;
        this.port = port;
        try {
            init();
        } catch (DBException e) {
            e.printStackTrace();
        }
    }

    public void init() throws DBException {
        txn_id = UUID.randomUUID().toString();
        try {
            txn_connection = new Socket(IP, port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Cleanup any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    public void cleanup() throws DBException
    {
    }

    /**
     * Start a database transaction.
     */
    public void start() throws DBException {
        ClientRequestOuterClass.ClientRequest.Builder reqBuilder = ClientRequestOuterClass.ClientRequest.newBuilder();
        reqBuilder.setId(txn_id);
        reqBuilder.setType("START");
        ClientRequestOuterClass.ClientRequest req = reqBuilder.build();

        try {
            req.writeTo(txn_connection.getOutputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Commit the current database transaction.
     */
    public void commit() throws DBException {

    }

    /**
     * Abort the current database transaction.
     */
    public void abort() throws DBException
    {
    }

    /**
     * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return The result of the operation.
     */
    public Status read(String table, String key, Set<String> fields, HashMap<String,ByteIterator> result) {
        ClientRequestOuterClass.ClientRequest.Builder reqBuilder = ClientRequestOuterClass.ClientRequest.newBuilder();
        reqBuilder.setId(txn_id).setType("READ").setTable(table).setKey(key);
        int count = 0;
        for (String s : fields) {
            reqBuilder.setFields(count++, s);
        }
        ClientRequestOuterClass.ClientRequest req = reqBuilder.build();

        try {
            req.writeTo(txn_connection.getOutputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
        // wait for response



        return null;
    }

    /**
     * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param startkey The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param fields The list of fields to read, or null for all of them
     * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
     * @return The result of the operation.
     */
    public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String,ByteIterator>> result) {
        return null;
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key, overwriting any existing values with the same field name.
     *
     * @param table The name of the table
     * @param key The record key of the record to write.
     * @param values A HashMap of field/value pairs to update in the record
     * @return The result of the operation.
     */
    public Status update(String table, String key, HashMap<String,ByteIterator> values) {
        return null;
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key.
     *
     * @param table The name of the table
     * @param key The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return The result of the operation.
     */
    public Status insert(String table, String key, HashMap<String,ByteIterator> values) {
        return null;
    }

    /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key The record key of the record to delete.
     * @return The result of the operation.
     */
    public Status delete(String table, String key) {
        return null;
    }

}

class Status {}
class DBException extends Exception {}
abstract class ByteIterator implements Iterator<Byte> {}