package io.github.peacock05.datastore;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static io.github.peacock05.datastore.DataStoreUtil.*;

public class DataStoreUtilTest {

    @Test
    public void testIntConversion(){
        byte[]   header = new byte[16];
        putInt(0x5b77f49e, header, 0);
        putInt(16, header, 4);
        putInt(~16, header, 8);
        putInt(0x34719e13, header, 12);

        Assertions.assertEquals(0x5b77f49e,getInt(header,0));
        Assertions.assertEquals(16,getInt(header,4));
        Assertions.assertEquals(~16,getInt(header,8));
        Assertions.assertEquals(0x34719e13,getInt(header,12));
    }

    @Test
    public void testLongConversion(){
        byte[]   header = new byte[16];
        putLong(0x5b77f49e, header, 0);
        putLong(0x34719e13, header, 8);

        Assertions.assertEquals(0x5b77f49e,getLong(header,0));
        Assertions.assertEquals(0x34719e13,getLong(header,8));
    }
}
