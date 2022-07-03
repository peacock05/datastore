package io.github.peacock05.datastore;

/**
 * Common utility functions used by {@link FileDataStoreQueue} and {@link FileDataStoreStack}
 */
class DataStoreUtil {

    private DataStoreUtil(){

    }

    /**
     * Reads integer from array
     * @param b the array
     * @param off the start offset in the array
     * @return integer value
     */
    public static int getInt(byte[] b, int off) {
        int ch1 = ((int)b[off]&0xFF);
        int ch2 = ((int)b[off+1]&0xFF);
        int ch3 = ((int)b[off+2]&0xFF);
        int ch4 = ((int)b[off+3]&0xFF);

        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4));
    }

    /**
     * Reads {@code long} from array
     * @param b the array
     * @param off the start offset in the array
     * @return long value
     */
    public static long getLong(byte[] b, int off) {
        return ((long)(getInt(b,off)) << 32) + (getInt(b,off+4) & 0xFFFFFFFFL);
    }


    /**
     * Save integer value into the array
     * @param v the integer value to store
     * @param b the array
     * @param off the start offset in the array.
     */
    public static void putInt(int v, byte[] b, int off) {

        b[off] = (byte)((v >>> 24) & 0xFF);
        b[off+1] = (byte)((v >>> 16) & 0xFF);
        b[off+2] = (byte)((v >>>  8) & 0xFF);
        b[off+3] = (byte)((v) & 0xFF);
    }

    /**
     * Save long value into the array
     * @param v the long value to store
     * @param b the array
     * @param off the start offset in the array.
     */
    public static void putLong(long v, byte[] b, int off) {
        b[off] = (byte)((int)(v >>> 56) & 0xFF);
        b[off+1] = (byte)((int)(v >>> 48) & 0xFF);
        b[off+2] = (byte)((int)(v >>> 40) & 0xFF);
        b[off+3] = (byte)((int)(v >>> 32) & 0xFF);
        b[off+4] = (byte)((int)(v >>> 24) & 0xFF);
        b[off+5] = (byte)((int)(v >>> 16) & 0xFF);
        b[off+6] = (byte)((int)(v >>>  8) & 0xFF);
        b[off+7] = (byte)((int)(v) & 0xFF);
    }

    /**
     * Computes hash of the array
     * @param a the array
     * @param off the start offset of the array
     * @param len the length of the data
     * @return hash value
     */
    public static int getHashCode(byte[] a, int off, int len) {
        int result = 1;
        for (int i = off; i < (off + len); i++) {
            result = 31 * result + a[i];
        }

        return result;
    }
}
