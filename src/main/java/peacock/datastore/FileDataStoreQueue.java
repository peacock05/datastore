package peacock.datastore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class FileDataStoreQueue implements DataStore{

    private final FileChannel fileChannel;
    private final long capacity;

    public FileDataStoreQueue(String queueName, Path directory, long limit) throws IOException {
        capacity = limit;
        fileChannel = FileChannel.open(directory.resolve(queueName+".fifo"), StandardOpenOption.READ,
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
    public void remove() {

    }

    @Override
    public void sync() {

    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public long capacity() {
        return 0;
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public long usage() {
        return 0;
    }

    @Override
    public void close() throws Exception {
        fileChannel.close();
    }
}
