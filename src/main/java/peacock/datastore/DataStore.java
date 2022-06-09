package peacock.datastore;

import java.nio.ByteBuffer;

public interface DataStore extends AutoCloseable{

    boolean write(ByteBuffer data);
    boolean read(ByteBuffer data);
    void remove();
    void sync();
    boolean isEmpty();
    long capacity();
    long size();
    long usage();
}
