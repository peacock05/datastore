package io.github.peacock05.datastore;


import java.io.*;

/**
 * FileDataStoreStack implements a persistent queue that allows data to be read, write and remove in LIFO order
 * <p>
 * Persistent storage format
 * ----------------------------------------------------------------------------
 * | Magic number
 * | Top Index
 * | 0
 * | Count
 * | Hash
 * |
 * | Magic number
 * | Top Index
 * | 0
 * | Count
 * | Hash
 * |
 * | Data 0, Data 1, ..... Data N, 0x5b77f49e, Data length, ~Data length, Data Hash
 */
public class FileDataStoreStack implements DataStore {

    private final static int ERROR_CODE_OK = 0;
    private final static int ERROR_CODE_IO_ERROR = 1;
    private final static int ERROR_CODE_LN_ERROR = 2;
    private final static int MAGIC_NUMBER = 0x30A1B608;
    private final static int FRAME_IDENTIFIER = 0x5b77f49e;
    private final RandomAccessFile file;
    private final byte[] metaBlock, dataBlockHeader, backUpBlockHeader;
    private final long capacity;
    private final int offset;
    private final RandomAccessFile backUpFile;
    private long topIndex;
    private long count;
    private boolean isMetaBlockUpdated;
    private boolean isBackUpUpdated;
    private int errorCode;
    private Exception exception;

    /**
     * Create the file based persistent data store to read, write and delete the data in LIFO order.
     *
     * @param queueName Name of the queue
     * @param directory Directory to store the file
     * @param limit     Maximum amount of space.
     * @throws IOException Upon error in creating, reading or writing to the file.
     */
    public FileDataStoreStack(String queueName, String directory, long limit) throws IOException {
        metaBlock = new byte[32];
        dataBlockHeader = new byte[16];
        backUpBlockHeader = new byte[16];
        capacity = limit;
        offset = metaBlock.length * 2;
        file = new RandomAccessFile(new File(directory, queueName + ".fifo"), "rw");
        backUpFile = new RandomAccessFile(new File(directory, queueName + ".bkp"), "rw");
        if (!readMetaData()) {
            topIndex = 0;
            writeMetaData();
        }
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
                    topIndex = DataStoreUtil.getLong(metaBlock, 8);
                    DataStoreUtil.getLong(metaBlock, 16); // Reserved
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
        DataStoreUtil.putLong(topIndex, metaBlock, 8);
        DataStoreUtil.putLong(0, metaBlock, 16);
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
        if ((topIndex + flc) < capacity) {
            DataStoreUtil.putInt(FRAME_IDENTIFIER, dataBlockHeader, 0);
            DataStoreUtil.putInt(len, dataBlockHeader, 4);
            DataStoreUtil.putInt(~len, dataBlockHeader, 8);
            int hash = DataStoreUtil.getHashCode(b, off, len);
            DataStoreUtil.putInt(hash, dataBlockHeader, 12);

            try {
                file.seek(topIndex + offset);
                file.write(b, off, len);
                file.write(dataBlockHeader);
                status = true;
                isMetaBlockUpdated = true;
                count++;
                topIndex += flc;

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


    private void writeBackUp(byte[] b, int off, int len) {

        try {
            DataStoreUtil.putInt(FRAME_IDENTIFIER, backUpBlockHeader, 0);
            DataStoreUtil.putInt(len, backUpBlockHeader, 4);
            DataStoreUtil.putInt(~len, backUpBlockHeader, 8);
            int hash = DataStoreUtil.getHashCode(b, off, len);
            DataStoreUtil.putInt(hash, backUpBlockHeader, 12);
            backUpFile.seek(0);
            backUpFile.write(dataBlockHeader);
            backUpFile.write(b, off, len);
        } catch (IOException ignore) {

        }
    }

    private int readBackUp(byte[] b, int off, int len) {
        int size = -1;
        try {
            backUpFile.seek(0);
            backUpFile.readFully(backUpBlockHeader);
            int fid = DataStoreUtil.getInt(backUpBlockHeader, 0);
            int dlc = DataStoreUtil.getInt(backUpBlockHeader, 4);
            int negated = DataStoreUtil.getInt(backUpBlockHeader, 8);
            int hash = DataStoreUtil.getInt(backUpBlockHeader, 12);
            if (fid == FRAME_IDENTIFIER && (dlc == (~negated))) {
                if (len >= dlc) {
                    backUpFile.readFully(b, off, dlc);
                    if (DataStoreUtil.getHashCode(b, off, dlc) == hash) {
                        size = dlc;
                    }
                }
            }
        } catch (IOException ignore) {
        }
        return size;
    }

    private int readBackUpLength() {
        int size = -1;
        try {
            backUpFile.seek(0);
            backUpFile.readFully(backUpBlockHeader);
            int fid = DataStoreUtil.getInt(backUpBlockHeader, 0);
            int dlc = DataStoreUtil.getInt(backUpBlockHeader, 4);
            int negated = DataStoreUtil.getInt(backUpBlockHeader, 8);
            if (fid == FRAME_IDENTIFIER && (dlc == (~negated))) {
                size = dlc;
            }
        } catch (IOException ignore) {
        }
        return size;
    }

    private boolean removeBackUp() {
        boolean status = false;
        try {
            backUpFile.seek(0);
            backUpFile.readFully(backUpBlockHeader);
            int fid = DataStoreUtil.getInt(backUpBlockHeader, 0);
            if(fid == FRAME_IDENTIFIER){
                backUpFile.seek(0);
                backUpFile.writeInt(0);
                status = true;
            }
        } catch (IOException ignore) {
        }
        return status;
    }

    @Override
    public synchronized int read(byte[] b, int off, int len) {
        errorCode = ERROR_CODE_OK;
        exception = null;
        int size = readBackUp(b, off, len);
        if (size < 0 && (topIndex >= (dataBlockHeader.length))) {
            try {
                size = 0;
                long headerSeek = topIndex - dataBlockHeader.length;
                file.seek(headerSeek + offset);
                file.readFully(dataBlockHeader);
                int fid = DataStoreUtil.getInt(dataBlockHeader, 0);
                int dlc = DataStoreUtil.getInt(dataBlockHeader, 4);
                int negated = DataStoreUtil.getInt(dataBlockHeader, 8);
                int hash = DataStoreUtil.getInt(dataBlockHeader, 12);
                if (fid == FRAME_IDENTIFIER && (dlc == (~negated))) {
                    long dataSeek = topIndex - dataBlockHeader.length - dlc;
                    if (len >= dlc && dataSeek >= 0) {
                        file.seek(dataSeek + offset);
                        file.readFully(b, off, dlc);
                        if (DataStoreUtil.getHashCode(b, off, dlc) == hash) {
                            writeBackUp(b, off, dlc);
                            topIndex = dataSeek;
                            isMetaBlockUpdated = true;
                            isBackUpUpdated = true;
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
        errorCode = ERROR_CODE_OK;
        exception = null;
        int size = readBackUpLength();
        if (size < 0 && topIndex >= dataBlockHeader.length) {
            try {
                long headerSeek = topIndex - dataBlockHeader.length;
                file.seek(headerSeek + offset);
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

        if(isBackUpUpdated){
            isBackUpUpdated = false;
            try {
                backUpFile.getFD().sync();
            } catch (IOException e) {
                errorCode = ERROR_CODE_IO_ERROR;
                exception = e;
            }
        }
        return status;
    }

    @Override
    public synchronized void remove() {
        errorCode = ERROR_CODE_OK;
        exception = null;
        boolean backRemoved = removeBackUp();
        if(backRemoved){
            isBackUpUpdated = true;
        }else{
            if (topIndex >= dataBlockHeader.length) {
                try {
                    long headerSeek = topIndex - dataBlockHeader.length;
                    file.seek(headerSeek + offset);
                    file.readFully(dataBlockHeader);
                    int fid = DataStoreUtil.getInt(dataBlockHeader, 0);
                    int dlc = DataStoreUtil.getInt(dataBlockHeader, 4);
                    int negated = DataStoreUtil.getInt(dataBlockHeader, 8);
                    if (fid == FRAME_IDENTIFIER && (dlc == (~negated))) {
                        count = count > 0 ? count - 1 : 0;
                        topIndex = topIndex - dataBlockHeader.length - dlc;
                        if (topIndex < 0)
                            topIndex = 0;

                    } else {
                        count = 0;
                        topIndex = 0;
                        errorCode = ERROR_CODE_LN_ERROR;
                        exception = null;
                    }
                } catch (IOException e) {
                    count = 0;
                    topIndex = 0;
                    errorCode = ERROR_CODE_IO_ERROR;
                    exception = e;
                }
            }
        }

    }

    @Override
    public synchronized boolean isEmpty() {
        errorCode = ERROR_CODE_OK;
        exception = null;
        return readBackUpLength() < 0 && (topIndex < dataBlockHeader.length);
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
        return topIndex;
    }

    @Override
    public synchronized long free() {
        errorCode = ERROR_CODE_OK;
        exception = null;
        return (capacity - topIndex);
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
    public synchronized void close() {
        try {
            file.close();
        } catch (IOException ignored) {

        }
        try {
            backUpFile.close();
        } catch (IOException ignored) {

        }
    }
}
