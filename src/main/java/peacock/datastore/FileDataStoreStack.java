package peacock.datastore;


import java.io.*;

import static peacock.datastore.DataStoreUtil.*;

/**
 * FileDataStoreStack implements a persistent queue that allows data to be read, write and remove in LIFO order
 * <p>
 * Persistent storage format
 * ----------------------------------------------------------------------------
 * | Magic number
 * | Head Index
 * | 0
 * | Count
 * | Hash
 * |
 * | Magic number
 * | Head Index
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
    private final byte[] metaData, header;
    private final long capacity;
    private final int offset;
    private long headIndex;
    private long count;
    private boolean isMetaDataUpdated;
    private int errorCode;
    private Exception exception;
    private final File backUpFile;
    private boolean backUpStatus;
    private int backUpLength;
    private int backUpHash;

    /**
     * Create the file based persistent data store to read, write and delete the data in LIFO order.
     * @param queueName Name of the queue
     * @param directory Directory to store the file
     * @param limit Maximum amount of space.
     * @throws IOException Upon error in creating, reading or writing to the file.
     */
    public FileDataStoreStack(String queueName, String directory, long limit) throws IOException {
        metaData = new byte[32];
        header = new byte[16];
        capacity = limit;
        offset = metaData.length * 2;
        backUpFile = new File(directory,queueName+".bkp");
        file = new RandomAccessFile(new File(directory, queueName + ".fifo"), "rw");
        if (!readMetaData()) {
            headIndex = offset;
            writeMetaData();
        }
    }

    private long getUsedSpace() {
        // | ---offset---headIndex---limit-|
        return headIndex - offset;
    }


    private long getFreeSpace() {
        // | ---offset---headIndex---limit-|
        return capacity - headIndex;
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
                    headIndex = getLong(metaData, 8);
                    getLong(metaData, 16); // Reserved
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
        putLong(headIndex, metaData, 8);
        putLong(0, metaData, 16);
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
                file.seek(headIndex);
                file.write(b, off, len);
                file.write(header);
                status = true;
                isMetaDataUpdated = true;
                count++;
                headIndex += flc;

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


    private void writeBackUp(byte[] b, int off, int len){
        try(FileOutputStream fos = new FileOutputStream(backUpFile)){
            fos.write(b,off,len);
        } catch (IOException ignore) {

        }
    }

    private int readBackUp(byte[] b, int off, int len){
        int size = -1;
        try(FileInputStream fis = new FileInputStream(backUpFile)){
            size = fis.read(b,off,len);
        }catch (IOException ignore){
        }
        return size;
    }

    @Override
    public int read(byte[] b, int off, int len) {
        int size = -1;
        errorCode = ERROR_CODE_OK;
        exception = null;
        if(backUpStatus){
            if(readBackUp(b,off,len) == backUpLength){
                if (getHashCode(b, off, backUpLength) == backUpHash) {
                    size = backUpLength;
                }
            }
        }else{
            if (getUsedSpace() >= header.length) {
                try {
                    long headerSeek = headIndex - header.length;
                    file.seek(headerSeek);
                    file.readFully(header);
                    int fid = getInt(header, 0);
                    int dlc = getInt(header, 4);
                    int negated = getInt(header, 8);
                    int hash = getInt(header, 12);
                    if (fid == FRAME_IDENTIFIER && (dlc == (~negated))) {
                        long dataSeek = headIndex - header.length - dlc;
                        if (len >= dlc && dataSeek >= header.length) {
                            file.seek(dataSeek);
                            file.readFully(b, off, dlc);
                            if (getHashCode(b, off, dlc) == hash) {
                                writeBackUp(b,off,dlc);
                                headIndex = dataSeek;
                                backUpLength = dlc;
                                backUpHash = hash;
                                backUpStatus = true;
                                isMetaDataUpdated = true;
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
        if(backUpStatus){
            size = backUpLength;
        }else{
            if (getUsedSpace() >= header.length) {
                try {
                    long headerSeek = headIndex - header.length;
                    file.seek(headerSeek);
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
        if(backUpStatus){
            count = count > 0? count - 1: 0;
            backUpStatus = false;
        }else{
            if (getUsedSpace() >= header.length) {
                try {
                    long headerSeek = headIndex - header.length;
                    file.seek(headerSeek);
                    file.readFully(header);
                    int fid = getInt(header, 0);
                    int dlc = getInt(header, 4);
                    int negated = getInt(header, 8);
                    if (fid == FRAME_IDENTIFIER && (dlc == (~negated))) {
                        count = count > 0? count - 1: 0;
                        headIndex = headIndex - header.length - dlc;
                        if(headIndex < offset)
                            headIndex = offset;

                    }else{
                        count = 0;
                        headIndex = offset;
                        errorCode = ERROR_CODE_LN_ERROR;
                        exception = null;
                    }
                } catch (IOException e) {
                    count = 0;
                    headIndex = offset;
                    errorCode = ERROR_CODE_IO_ERROR;
                    exception = e;
                }
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
