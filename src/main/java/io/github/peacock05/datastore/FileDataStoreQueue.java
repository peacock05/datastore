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
     *
     * @param queueName Name of the queue
     * @param directory Directory to store the file
     * @param limit     Maximum amount of space.
     * @throws IOException Upon error in creating, reading or writing to the file.
     */
    public FileDataStoreQueue(String queueName, String directory, long limit) throws IOException {
        metaBlock = new byte[32];
        dataBlockHeader = new byte[16];
        capacity = limit;
        offset = metaBlock.length * 2;
        file = new RandomAccessFile(new File(directory, queueName + ".fifo"), "rw");
        if (!readMetaData()) {
            frontIndex = offset;
            rearIndex = offset;
            count = 0;
            writeMetaData();
        }
    }

    private static void writeData(RandomAccessFile file, long index, byte[] header, byte[] b, int off, int len)
            throws IOException {

        DataStoreUtil.putInt(FRAME_IDENTIFIER, header, 0);
        DataStoreUtil.putInt(len, header, 4);
        DataStoreUtil.putInt(~len, header, 8);
        int hash = DataStoreUtil.getHashCode(b, off, len);
        DataStoreUtil.putInt(hash, header, 12);
        file.seek(index);
        file.write(header);
        file.write(b, off, len);

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
        try {
            if (rearIndex < frontIndex) {
                // 0---offset---rearIndex----frontIndex----limit
                if ((rearIndex + flc) < frontIndex) {
                    writeData(file, rearIndex, dataBlockHeader, b, off, len);
                    count++;
                    rearIndex = rearIndex + flc;
                    isMetaBlockUpdated = true;
                    status = true;
                }

            } else {
                // 0---offset---frontIndex----rearIndex----limit
                // 0 --offset-- frontIndex and rearIndex --- limit
                if (rearIndex + flc < capacity) {
                    writeData(file, rearIndex, dataBlockHeader, b, off, len);
                    count++;
                    rearIndex = rearIndex + flc;
                    isMetaBlockUpdated = true;
                    status = true;
                } else {
                    if (frontIndex != offset) {
                        writeData(file, rearIndex, dataBlockHeader, b, off, len);
                        count++;
                        rearIndex = offset;
                        isMetaBlockUpdated = true;
                        status = true;
                    }
                }
            }
        } catch (IOException e) {
            errorCode = ERROR_CODE_IO_ERROR;
            exception = e;
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
        if (rearIndex != frontIndex) {
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
        if (rearIndex != frontIndex) {
            size = 0;
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
        if (rearIndex != frontIndex) {
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
        return rearIndex == frontIndex;
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
        long used;
        errorCode = ERROR_CODE_OK;
        exception = null;
        if (rearIndex < frontIndex) {
            // 0---offset---rearIndex----frontIndex----limit
            used = (rearIndex + offset) + (capacity - frontIndex);

        } else {
            // 0---offset---frontIndex----rearIndex----limit
            // 0 --offset-- frontIndex and rearIndex --- limit
            used = rearIndex - frontIndex;
        }
        return used;
    }

    @Override
    public synchronized long free() {
        long free;
        errorCode = ERROR_CODE_OK;
        exception = null;
        if (rearIndex < frontIndex) {
            // 0---offset---rearIndex----frontIndex----limit
            free = frontIndex - rearIndex;

        } else {
            // 0---offset---frontIndex----rearIndex----limit
            // 0 --offset-- frontIndex and rearIndex --- limit
            free = (capacity - rearIndex) + (frontIndex - offset);
        }
        return free;

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
    public synchronized void close() throws IOException {
        file.close();
    }
}
