package peacock.datastore;

class DataStoreUtil {

    private DataStoreUtil(){

    }

    public static int getInt(byte[] b, int off) {
        int ch1 = ((int)b[off]&0xFF);
        int ch2 = ((int)b[off+1]&0xFF);
        int ch3 = ((int)b[off+2]&0xFF);
        int ch4 = ((int)b[off+3]&0xFF);

        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4));
    }

    public static long getLong(byte[] b, int off) {
        return ((long)(getInt(b,off)) << 32) + (getInt(b,off+4) & 0xFFFFFFFFL);
    }


    public static void putInt(int v, byte[] b, int off) {

        b[off] = (byte)((v >>> 24) & 0xFF);
        b[off+1] = (byte)((v >>> 16) & 0xFF);
        b[off+2] = (byte)((v >>>  8) & 0xFF);
        b[off+3] = (byte)((v) & 0xFF);
    }

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

    public static int getHashCode(byte[] a, int off, int len) {
        if (a == null)
            return 0;
        int result = 1;
        for (int i = off; i < (off + len); i++) {
            result = 31 * result + a[i];
        }

        return result;
    }
}
