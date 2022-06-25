package peacock.datastore;



/**
 * FileDataStoreStack implements a persistent queue that allows data to be read, write and remove in LIFO order
 *
 * Persistent storage format
 * ----------------------------------------------------------------------------
 * | Magic number
 * | Head Index
 * | Count
 * | Hash
 * |
 * | Magic number
 * | Head Index
 * | Count
 * | Hash
 * |
 * | Data 0, Data 1, ..... Data N, 0x7E, Data length, ~Data length, Data Hash
 *
 */
public class FileDataStoreStack implements DataStore {


    @Override
    public boolean write(byte[] b, int off, int len) {
        return false;
    }

    @Override
    public boolean write(byte[] b) {
        return false;
    }

    @Override
    public int read(byte[] b, int off, int len) {
        return 0;
    }

    @Override
    public int read(byte[] b) {
        return 0;
    }

    @Override
    public int readLength() {
        return 0;
    }

    @Override
    public boolean sync() {
        return false;
    }

    @Override
    public void remove() {

    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public long count() {
        return 0;
    }

    @Override
    public long capacity() {
        return 0;
    }

    @Override
    public long usage() {
        return 0;
    }

    @Override
    public long free() {
        return 0;
    }

    @Override
    public int getErrorCode() {
        return 0;
    }

    @Override
    public Exception getException() {
        return null;
    }

    @Override
    public void close() throws Exception {

    }
}
