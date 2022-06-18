package peacock.datastore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * FileDataStoreQueue implements a persistent queue that allows data to be read, write and remove in FIFO order
 * <p>
 * Persistent storage format
 * ----------------------------------------------------------------------------
 * | Magic number
 * | Hash
 * | Read Index
 * | Write Index
 * | Count
 * |
 * <p>
 * | Magic number
 * | Hash
 * | ReadIndex
 * | Write Index
 * | Count
 * |
 * | 0x5b77f49e, Data length, ~Data length, Data Hash, Data 0, Data 1, ..... Data N
 */
public class FileDataStoreQueue implements DataStore {

    private final static int MAGIC_NUMBER = 0x34719e13;
    private final static int FRAME_IDENTIFIER = 0x5b77f49e;

    private final FileChannel fileChannel;
    private final ByteBuffer metaData;
    private final ByteBuffer writeHeader, readHeader;
    private final ByteBuffer[] writeBuffer;
    private final long capacity;
    private final int offset;
    private long readIndex;
    private long writeIndex;
    private long count;
    private boolean isMetaDataUpdated;
    private int errorCode = NO_ERROR;
    private Exception exception;

    public FileDataStoreQueue(String queueName, Path directory, long limit) throws IOException {
        String fileName = queueName + ".fifo";
        metaData = ByteBuffer.allocate(32);
        writeHeader = ByteBuffer.allocate(16);
        readHeader = ByteBuffer.allocate(16);
        writeBuffer = new ByteBuffer[2];
        offset = metaData.capacity();
        capacity = limit;
        fileChannel = FileChannel.open(directory.resolve(fileName), StandardOpenOption.READ,
                StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        if (!readMetaData()) {
            writeMetaData();
        }
    }

    private boolean readMetaData() {

        boolean status = false;
        try {
            for (int i = 0; i < 2; i++) {

                fileChannel.position(i * metaData.capacity());
                metaData.clear();
                if (fileChannel.read(metaData) == metaData.capacity()) {
                    metaData.flip();
                    int magic = metaData.getInt();
                    int hash = metaData.getInt();
                    if (magic == MAGIC_NUMBER && hash == metaData.hashCode()) {
                        readIndex = metaData.getLong();
                        writeIndex = metaData.getLong();
                        count = metaData.getLong();
                        status = true;
                        break;
                    }
                }
            }
        } catch (IOException ignore) {

        }

        return status;
    }

    private boolean writeMetaData() {

        boolean status = false;
        metaData.clear();
        metaData.putInt(MAGIC_NUMBER);
        metaData.putInt(0);
        metaData.putLong(readIndex);
        metaData.putLong(writeIndex);
        metaData.putLong(count);
        metaData.flip();
        metaData.position(8);
        int hash = metaData.hashCode();
        metaData.putInt(4, hash);

        try {
            for (int i = 0; i < 2; i++) {

                fileChannel.position(i * metaData.capacity());
                metaData.clear();
                if (fileChannel.write(metaData) == metaData.capacity()) {
                    fileChannel.force(false);
                    status = true;
                }

            }
        } catch (IOException ignore) {

        }

        return status;
    }

    private long getUsedSpace(){
        long used;
        if (writeIndex >= readIndex) {
            // |---offset---readIndex----writeIndex----limit-|
            used = writeIndex - readIndex;

        } else {
            // |---offset---writeIndex----readIndex----limit-|
            used = (capacity - readIndex) + (writeIndex - offset);
        }
        return used;
    }


    private long getFreeSpace(){
        long free;

        if (writeIndex >= readIndex) {
            // |---offset---readIndex----writeIndex----limit-|
            free = (capacity - writeIndex) + (readIndex - offset);
        } else {
            // |---offset---writeIndex----readIndex----limit-|
            free = readIndex - writeIndex;
        }

        return free;
    }


    @Override
    public synchronized boolean write(ByteBuffer data) {

        boolean status = false;
        int dlc = data.remaining();
        int flc = dlc + writeHeader.capacity();
        errorCode = NO_ERROR;
        exception = null;
        if (getFreeSpace() >= flc) {
            writeHeader.clear();
            writeHeader.putInt(FRAME_IDENTIFIER);
            writeHeader.putInt(dlc);
            writeHeader.putInt(~dlc);
            writeHeader.putInt(data.hashCode());
            writeHeader.flip();
            writeBuffer[0] = writeHeader;
            writeBuffer[1] = data;
            try {
                fileChannel.position(writeIndex);
                if (flc == fileChannel.write(writeBuffer)) {
                    status = true;
                    isMetaDataUpdated = true;
                    count++;
                    writeIndex += flc;
                    if (writeIndex >= capacity)
                        writeIndex = offset;
                }
            } catch (IOException e) {
                errorCode = IO_ERROR;
                exception = e;
            }
        }

        return status;
    }

    @Override
    public synchronized boolean read(ByteBuffer data) {

        boolean status = false;
        errorCode = NO_ERROR;
        exception = null;
        if (getUsedSpace() >= readHeader.capacity()) {
            int size = 0;
            try {
                readHeader.clear();
                fileChannel.position(readIndex);
                size = fileChannel.read(readHeader);
            } catch (IOException e) {
                errorCode = IO_ERROR;
                exception = e;
            }
            if (size == readHeader.capacity()) {
                readHeader.flip();
                int fid = readHeader.getInt();
                int dlc = readHeader.getInt();
                int negated = readHeader.getInt();
                int hash = readHeader.getInt();
                if (fid == FRAME_IDENTIFIER && (dlc == (~negated))) {
                    if (data.remaining() >= dlc) {
                        int dataPos = data.position();
                        int dataLimit = data.limit();
                        data.limit(dataPos + dlc);
                        try {
                            size = fileChannel.read(data);
                            if (size == dlc) {
                                data.position(dataPos);
                                if (data.hashCode() == hash) {
                                    data.position(dataPos + dlc);
                                    status = true;
                                }
                            }
                        } catch (IOException e) {
                            errorCode = IO_ERROR;
                            exception = e;
                        } finally {
                            if (!status)
                                data.position(dataPos);
                        }
                        data.limit(dataLimit);
                    }
                }
            }
        }

        return status;
    }


    @Override
    public synchronized int readLength() {

        int length = -1;
        errorCode = NO_ERROR;
        exception = null;
        if (getUsedSpace() >= readHeader.capacity()) {
            try {
                readHeader.clear();
                fileChannel.position(readIndex);
                int size = fileChannel.read(readHeader);
                if (size == readHeader.capacity()) {
                    readHeader.flip();
                    int fid = readHeader.getInt();
                    int dlc = readHeader.getInt();
                    int negated = readHeader.getInt();

                    if (fid == FRAME_IDENTIFIER && (dlc == (~negated))) {
                        length = dlc;
                    }
                }

            } catch (IOException e) {
                errorCode = IO_ERROR;
                exception = e;
            }
        }

        return length;
    }

    @Override
    public synchronized void remove() {
        errorCode = NO_ERROR;
        exception = null;
        if (getUsedSpace() >= readHeader.capacity()) {
            try {
                readHeader.clear();
                fileChannel.position(readIndex);
                int size = fileChannel.read(readHeader);
                isMetaDataUpdated = true;
                if (size == -1) {
                    // Pointer reached end-of-file
                    // Reset the index
                    readIndex = offset;
                } else if (size == readHeader.capacity()) {
                    readHeader.flip();
                    int fid = readHeader.getInt();
                    int dlc = readHeader.getInt();
                    int negated = readHeader.getInt();
                    if (fid == FRAME_IDENTIFIER && (dlc == (~negated))) {
                        count = count > 0 ? count - 1 : 0;
                        readIndex += (dlc + readHeader.capacity());
                        if (readIndex >= capacity)
                            readIndex = offset;
                    } else {
                        // There is corruption in the file pointers
                        // Let's drop data until write index
                        count = 0;
                        readIndex = writeIndex;
                    }
                }

            } catch (IOException e) {
                errorCode = IO_ERROR;
                exception = e;
            }
        }
    }

    @Override
    public synchronized boolean sync() {
        boolean status = true;
        errorCode = NO_ERROR;
        exception = null;
        if (isMetaDataUpdated) {
            isMetaDataUpdated = false;
            status = writeMetaData();
        }
        return status;
    }

    @Override
    public synchronized boolean isEmpty() {
        errorCode = NO_ERROR;
        exception = null;
        return getUsedSpace() >= readHeader.capacity();
    }

    @Override
    public long capacity() {
        errorCode = NO_ERROR;
        exception = null;
        return capacity;
    }

    @Override
    public long size() {
        errorCode = NO_ERROR;
        exception = null;
        return count;
    }


    @Override
    public synchronized long usage() {
        errorCode = NO_ERROR;
        exception = null;
        return getUsedSpace();
    }

    @Override
    public synchronized long free() {
        errorCode = NO_ERROR;
        exception = null;
        return getFreeSpace();
    }

    @Override
    public int getErrorCode() {
        return errorCode;
    }

    @Override
    public Exception getException() {
        return exception;
    }

    @Override
    public void close() throws Exception {
        fileChannel.close();
    }
}
