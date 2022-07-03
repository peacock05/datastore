package io.github.peacock05.datastore;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * FileDataStoreQueue implements a persistent queue that allows data to be read, write and remove in FIFO order
 * <p>
 * Persistent storage format
 * ----------------------------------------------------------------------------
 * | Magic number
 * | Hash
 * | Front Index
 * | Rear Index
 * | Count
 * |
 * <p>
 * | Magic number
 * | Hash
 * | Front Index
 * | Rear Index
 * | Count
 * |
 * | 0x5b77f49e, Data length, ~Data length, Data Hash, Data 0, Data 1, ..... Data N
 */
public class FileDataStoreQueue implements DataStore {

    private final static int ERROR_CODE_OK = 0;
    private final static int ERROR_CODE_IO_ERROR = 1;
    private final static int MAGIC_NUMBER = 0x34719e13;
    private final static int FRAME_IDENTIFIER = 0x5b77f49e;
    private final RandomAccessFile file;
    private final byte[] metaBlock, dataBlockHeader;
    private final long capacity;
    private final int offset;
    private long frontIndex;
    private long rearIndex;
    private long count;
    private boolean isMetaBlockUpdated;
    private int errorCode;
    private Exception exception;


    /**
     * Create the file based persistent data store to read, write and delete the data in FIFO order.
     * @param queueName Name of the queue
     * @param directory Directory to store the file
     * @param limit Maximum amount of space.
     * @throws IOException Upon error in creating, reading or writing to the file.
     */
    public FileDataStoreQueue(String queueName, String directory, long limit) throws IOException {
        metaBlock = new byte[32];
        dataBlockHeader = new byte[16];
        capacity = limit;
        offset = metaBlock.length*2;
        file = new RandomAccessFile(new File(directory, queueName + ".fifo"), "rw");
        if (!readMetaData()) {
            frontIndex = -1;
            rearIndex = -1;
            writeMetaData();
        }
    }

    private long getUsedSpace0() {
        long used;

        if(frontIndex == -1){
            // Queue is not used yet.
            used = 0;
        }else if (rearIndex > frontIndex) {
            // |---offset---frontIndex----rearIndex----limit-|
            used = rearIndex - frontIndex;

        } else {
            // |---offset---rearIndex----frontIndex----limit-|
            used = (capacity - frontIndex) + (rearIndex - offset);
        }
        return used;
    }


    private long getFreeSpace0() {
        long free;

        if(frontIndex == -1){
            free = capacity;
        }else if (rearIndex > frontIndex) {
            // |---offset---frontIndex----rearIndex----limit-|
            free = (capacity - rearIndex) + (frontIndex - offset);
        } else if (rearIndex < frontIndex){
            // |---offset---rearIndex----frontIndex----limit-|
            free = frontIndex - rearIndex;
        } else{
            // Both rearIndex and FrontIndex is equal
            free = capacity;
        }

        return free;
    }

    private boolean readMetaData() {

        boolean status = false;
        errorCode = ERROR_CODE_OK;
        exception = null;
        for (int i = 0; i < 2; i++) {
            int pos = i * metaBlock.length;
            try {
                file.seek(pos);
                file.readFully(metaBlock);
                int magic = DataStoreUtil.getInt(metaBlock, 0);
                int hash = DataStoreUtil.getInt(metaBlock, 4);
                if (magic == MAGIC_NUMBER && hash == DataStoreUtil.getHashCode(metaBlock, 8, metaBlock.length - 8)) {
                    frontIndex = DataStoreUtil.getLong(metaBlock, 8);
                    rearIndex = DataStoreUtil.getLong(metaBlock, 16);
                    count = DataStoreUtil.getLong(metaBlock, 24);
                    status = true;
                    break;
                }
            } catch (IOException e) {
                errorCode = ERROR_CODE_IO_ERROR;
                exception = e;
            }
        }

        return status;
    }


    private boolean writeMetaData() {

        boolean status = false;
        errorCode = ERROR_CODE_OK;
        exception = null;
        DataStoreUtil.putInt(MAGIC_NUMBER, metaBlock, 0);
        DataStoreUtil.putLong(frontIndex, metaBlock, 8);
        DataStoreUtil.putLong(rearIndex, metaBlock, 16);
        DataStoreUtil.putLong(count, metaBlock, 24);
        int hash = DataStoreUtil.getHashCode(metaBlock, 8, metaBlock.length - 8);
        DataStoreUtil.putInt(hash, metaBlock, 4);

        for (int i = 0; i < 2; i++) {
            int pos = i * metaBlock.length;
            try {
                file.seek(pos);
                file.write(metaBlock);
                file.getFD().sync();
                status = true;
            } catch (IOException e) {
                errorCode = ERROR_CODE_IO_ERROR;
                exception = e;
            }
        }

        return status;
    }


    @Override
    public synchronized boolean write(byte[] b, int off, int len) {
        boolean status = false;
        int flc = len + dataBlockHeader.length;
        errorCode = ERROR_CODE_OK;
        exception = null;

        if (flc > 0) { // TODO
            DataStoreUtil.putInt(FRAME_IDENTIFIER, dataBlockHeader, 0);
            DataStoreUtil.putInt(len, dataBlockHeader, 4);
            DataStoreUtil.putInt(~len, dataBlockHeader, 8);
            int hash = DataStoreUtil.getHashCode(b, off, len);
            DataStoreUtil.putInt(hash, dataBlockHeader, 12);

            try {
                long nextRearIndex = (rearIndex == -1)? offset:(rearIndex+flc);

                file.seek(nextRearIndex);
                file.write(dataBlockHeader);
                file.write(b, off, len);
                count++;
                if (frontIndex == -1)
                    frontIndex = offset;
                rearIndex = nextRearIndex;
                isMetaBlockUpdated = true;
                status = true;

            } catch (IOException e) {
                errorCode = ERROR_CODE_IO_ERROR;
                exception = e;
            }
        }

        return status;
    }

    @Override
    public boolean write(byte[] b) {
        return write(b, 0, b.length);
    }

    @Override
    public synchronized int read(byte[] b, int off, int len) {
        int size = -1;
        errorCode = ERROR_CODE_OK;
        exception = null;
        if (off > 0) { //TODO
            size = 0;
            try {
                file.seek(frontIndex);
                file.readFully(dataBlockHeader);
                int fid = DataStoreUtil.getInt(dataBlockHeader, 0);
                int dlc = DataStoreUtil.getInt(dataBlockHeader, 4);
                int negated = DataStoreUtil.getInt(dataBlockHeader, 8);
                int hash = DataStoreUtil.getInt(dataBlockHeader, 12);
                if (fid == FRAME_IDENTIFIER && (dlc == (~negated))) {
                    if (len >= dlc) {
                        file.readFully(b, off, dlc);
                        if (DataStoreUtil.getHashCode(b, off, dlc) == hash) {
                            size = dlc;
                        }
                    }
                }
            } catch (IOException e) {
                errorCode = ERROR_CODE_IO_ERROR;
                exception = e;
            }
        }

        return size;
    }

    @Override
    public int read(byte[] b) {
        return read(b, 0, b.length);
    }

    @Override
    public synchronized int readLength() {
        int size = -1;
        errorCode = ERROR_CODE_OK;
        exception = null;
        if (dataBlockHeader.length == 0) { // TODO
            try {
                file.seek(frontIndex);
                file.readFully(dataBlockHeader);
                int fid = DataStoreUtil.getInt(dataBlockHeader, 0);
                int dlc = DataStoreUtil.getInt(dataBlockHeader, 4);
                int negated = DataStoreUtil.getInt(dataBlockHeader, 8);
                if (fid == FRAME_IDENTIFIER && (dlc == (~negated))) {
                    size = dlc;
                }
            } catch (IOException e) {
                size = 0;
                errorCode = ERROR_CODE_IO_ERROR;
                exception = e;
            }
        }
        return size;
    }

    @Override
    public synchronized boolean sync() {
        boolean status = true;
        errorCode = ERROR_CODE_OK;
        exception = null;
        if (isMetaBlockUpdated) {
            isMetaBlockUpdated = false;
            status = writeMetaData();
        }
        return status;
    }

    @Override
    public synchronized void remove() {
        errorCode = ERROR_CODE_OK;
        exception = null;
        if (false) { //TODO

            try {
                file.seek(frontIndex);
                file.readFully(dataBlockHeader);
                int fid = DataStoreUtil.getInt(dataBlockHeader, 0);
                int dlc = DataStoreUtil.getInt(dataBlockHeader, 4);
                int negated = DataStoreUtil.getInt(dataBlockHeader, 8);
                isMetaBlockUpdated = true;
                if (fid == FRAME_IDENTIFIER && (dlc == (~negated))) {
                    count = count > 0 ? count - 1 : 0;
                    frontIndex += (dlc + dataBlockHeader.length);
                    if (frontIndex >= capacity)
                        frontIndex = offset;
                } else {
                    // There is corruption in the file pointers
                    // Let's drop data until write index
                    count = 0;
                    frontIndex = rearIndex;
                }
            } catch (EOFException e) {
                // Pointer reached end-of-file
                // Reset the index
                isMetaBlockUpdated = true;
                frontIndex = offset;
            } catch (IOException e) {
                errorCode = ERROR_CODE_IO_ERROR;
                exception = e;
            }
        }
    }

    @Override
    public synchronized boolean isEmpty() {
        errorCode = ERROR_CODE_OK;
        exception = null;
        return false; //TODO
    }

    @Override
    public synchronized long count() {
        errorCode = ERROR_CODE_OK;
        exception = null;
        return count;
    }

    @Override
    public synchronized long capacity() {
        errorCode = ERROR_CODE_OK;
        exception = null;
        return capacity;
    }

    @Override
    public synchronized long usage() {
        errorCode = ERROR_CODE_OK;
        exception = null;
        return 0; //TODO
    }

    @Override
    public synchronized long free() {
        errorCode = ERROR_CODE_OK;
        exception = null;
        return 0; // TODO

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
    public synchronized void close() throws Exception {
        file.getFD().sync();
        file.close();
    }
}
