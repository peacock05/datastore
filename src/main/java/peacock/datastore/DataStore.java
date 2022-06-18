package peacock.datastore;

import java.nio.ByteBuffer;

/**
 * DataStore is an interface to read, write and delete the data
 * from a persistent storage.
 *
 * The order of data storage is left to the implementation.
 */
public interface DataStore extends AutoCloseable{

    static int NO_ERROR = 0;
    static  int IO_ERROR = 1;
    /**
     * Write data into the persistent storage and does not delete the data.
     * @param data Data to write into the persistent storage.
     * @return
     */
    boolean write(ByteBuffer data);
    boolean read(ByteBuffer data);
    int readLength();
    void remove();
    boolean sync();
    boolean isEmpty();
    long capacity();
    long size();
    long usage();
    long free();
    int getErrorCode();
    Exception getException();
}
