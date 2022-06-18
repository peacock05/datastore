package peacock.datastore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

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
public class FileDataStoreStack implements DataStore{

    private final FileChannel fileChannel;
    private final long capacity;
    private long headIndex = 0;
    private long count;

    public FileDataStoreStack(String queueName, Path directory, long limit) throws IOException {
        capacity = limit;
        fileChannel = FileChannel.open(directory.resolve((queueName+".lifo")), StandardOpenOption.READ,
                    StandardOpenOption.WRITE,StandardOpenOption.CREATE);
    }

    @Override
    public boolean write(ByteBuffer data) {
        return false;
    }

    @Override
    public boolean read(ByteBuffer data) {
        return false;
    }

    @Override
    public int readLength() {
        return 0;
    }

    @Override
    public void remove() {

    }

    @Override
    public boolean sync() {
        return false;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public long capacity() {
        return capacity;
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public long usage() {
        long usage = 0;
        try {
            usage =  fileChannel.position();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return usage;
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
        fileChannel.close();
    }
}
