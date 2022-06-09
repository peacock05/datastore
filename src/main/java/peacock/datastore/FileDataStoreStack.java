package peacock.datastore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class FileDataStoreStack implements DataStore{

    private final FileChannel fileChannel;
    private final long capacity;

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
    public void remove() {

    }

    @Override
    public void sync() {
        try {
            fileChannel.force(false);
        } catch (IOException ignored) {
        }
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
    public void close() throws Exception {
        fileChannel.close();
    }
}
