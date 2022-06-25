package peacock.datastore;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import static peacock.datastore.DataStoreUtil.*;

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

    private final static int ERROR_CODE_OK = 0;
    private final static int ERROR_CODE_IO_ERROR = 1;
    private final static int MAGIC_NUMBER = 0x34719e13;
    private final static int FRAME_IDENTIFIER = 0x5b77f49e;
    private final RandomAccessFile file;
    private final byte[] metaData, header;
    private final long capacity;
    private final int offset;
    private long readIndex;
    private long writeIndex;
    private long count;
    private boolean isMetaDataUpdated;
    private int errorCode;
    private Exception exception;


    public FileDataStoreQueue(String queueName, String directory, long limit) throws IOException {
        metaData = new byte[32];
        header = new byte[16];
        capacity = limit;
        offset = metaData.length*2;
        file = new RandomAccessFile(new File(directory, queueName + ".fifo"), "rw");
        if (!readMetaData()) {
            readIndex = offset;
            writeIndex = offset;
            writeMetaData();
        }
    }

    private long getUsedSpace() {
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


    private long getFreeSpace() {
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

    private boolean readMetaData() {

        boolean status = false;
        errorCode = ERROR_CODE_OK;
        exception = null;
        for (int i = 0; i < 2; i++) {
            int pos = i * metaData.length;
            try {
                file.seek(pos);
                file.readFully(metaData);
                int magic = getInt(metaData, 0);
                int hash = getInt(metaData, 4);
                if (magic == MAGIC_NUMBER && hash == getHashCode(metaData, 8, metaData.length - 8)) {
                    readIndex = getLong(metaData, 8);
                    writeIndex = getLong(metaData, 16);
                    count = getLong(metaData, 24);
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
        putInt(MAGIC_NUMBER, metaData, 0);
        putLong(readIndex, metaData, 8);
        putLong(writeIndex, metaData, 16);
        putLong(count, metaData, 24);
        int hash = getHashCode(metaData, 8, metaData.length - 8);
        putInt(hash, metaData, 4);

        for (int i = 0; i < 2; i++) {
            int pos = i * metaData.length;
            try {
                file.seek(pos);
                file.write(metaData);
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
    public boolean write(byte[] b, int off, int len) {
        boolean status = false;
        int flc = len + header.length;
        errorCode = ERROR_CODE_OK;
        exception = null;
        if (getFreeSpace() >= flc) {
            putInt(FRAME_IDENTIFIER, header, 0);
            putInt(len, header, 4);
            putInt(~len, header, 8);
            int hash = getHashCode(b, off, len);
            putInt(hash, header, 12);

            try {
                file.seek(writeIndex);
                file.write(header);
                file.write(b, off, len);
                status = true;
                isMetaDataUpdated = true;
                count++;
                writeIndex += flc;
                if (writeIndex >= capacity)
                    writeIndex = offset;

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
    public int read(byte[] b, int off, int len) {
        int size = -1;
        errorCode = ERROR_CODE_OK;
        exception = null;
        if (getUsedSpace() >= header.length) {
            try {
                file.seek(readIndex);
                file.readFully(header);
                int fid = getInt(header, 0);
                int dlc = getInt(header, 4);
                int negated = getInt(header, 8);
                int hash = getInt(header, 12);
                if (fid == FRAME_IDENTIFIER && (dlc == (~negated))) {
                    if (len >= dlc) {
                        file.readFully(b, off, dlc);
                        if (getHashCode(b, off, dlc) == hash) {
                            size = dlc;
                        }
                    }
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
    public int read(byte[] b) {
        return read(b, 0, b.length);
    }

    @Override
    public int readLength() {
        int size = -1;
        errorCode = ERROR_CODE_OK;
        exception = null;
        if (getUsedSpace() >= header.length) {
            try {
                file.seek(readIndex);
                file.readFully(header);
                int fid = getInt(header, 0);
                int dlc = getInt(header, 4);
                int negated = getInt(header, 8);
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
    public boolean sync() {
        boolean status = true;
        errorCode = ERROR_CODE_OK;
        exception = null;
        if (isMetaDataUpdated) {
            isMetaDataUpdated = false;
            status = writeMetaData();
        }
        return status;
    }

    @Override
    public void remove() {
        errorCode = ERROR_CODE_OK;
        exception = null;
        if (getUsedSpace() >= header.length) {

            try {
                file.seek(readIndex);
                file.readFully(header);
                int fid = getInt(header, 0);
                int dlc = getInt(header, 4);
                int negated = getInt(header, 8);
                isMetaDataUpdated = true;
                if (fid == FRAME_IDENTIFIER && (dlc == (~negated))) {
                    count = count > 0 ? count - 1 : 0;
                    readIndex += (dlc + header.length);
                    if (readIndex >= capacity)
                        readIndex = offset;
                } else {
                    // There is corruption in the file pointers
                    // Let's drop data until write index
                    count = 0;
                    readIndex = writeIndex;
                }
            } catch (EOFException e) {
                // Pointer reached end-of-file
                // Reset the index
                isMetaDataUpdated = true;
                readIndex = offset;
            } catch (IOException e) {
                errorCode = ERROR_CODE_IO_ERROR;
                exception = e;
            }
        }
    }

    @Override
    public boolean isEmpty() {
        errorCode = ERROR_CODE_OK;
        exception = null;
        return getUsedSpace() >= header.length;
    }

    @Override
    public long count() {
        errorCode = ERROR_CODE_OK;
        exception = null;
        return count;
    }

    @Override
    public long capacity() {
        errorCode = ERROR_CODE_OK;
        exception = null;
        return capacity;
    }

    @Override
    public long usage() {
        errorCode = ERROR_CODE_OK;
        exception = null;
        return getUsedSpace();
    }

    @Override
    public long free() {
        errorCode = ERROR_CODE_OK;
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
        file.getFD().sync();
        file.close();
    }
}
