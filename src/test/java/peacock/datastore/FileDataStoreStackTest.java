package peacock.datastore;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

public class FileDataStoreStackTest {

    @Test
    public void testDataStoreStack() throws Exception {

        DataStore store = new FileDataStoreStack("testQueue",
                Files.createTempDirectory("datastore").toString(),5_000_000);
        Assertions.assertNotNull(store,"Store cannot be empty");
        Assertions.assertFalse(store.isEmpty(),"Data store must be empty");
        Assertions.assertEquals(5_000_000,store.capacity());
        Assertions.assertEquals(0,store.usage());
        Assertions.assertTrue(store.free()>4999100,"Free can't be less this size");
        ByteBuffer testData = ByteBuffer.allocate(120);

        testData.putFloat(23.43f);
        testData.putInt(98);
        testData.putLong(9841893746890L);
        testData.flip();
        int l1 = testData.remaining();
        Assertions.assertTrue(store.write(testData.array(),testData.position(),testData.remaining()),
                "Write do not fail here");
        Assertions.assertTrue(store.isEmpty(),"Data store must not be empty");
        Assertions.assertEquals(1,store.count());

        testData.clear();
        testData.putFloat(29.83f);
        testData.putInt(98345);
        testData.putLong(1498794656586L);
        testData.putFloat(78.83f);
        testData.putInt(8767);
        testData.flip();
        int l2 = testData.remaining();
        Assertions.assertTrue(store.write(testData.array(),testData.position(),testData.remaining()),
                "Write do not fail here");
        Assertions.assertTrue(store.isEmpty(),"Data store must not be empty");
        Assertions.assertEquals(2,store.count());

        Assertions.assertEquals(0,store.getErrorCode());
        Assertions.assertNull(store.getException());

        Assertions.assertEquals(l2,store.readLength());
        Assertions.assertEquals(l2,store.readLength()); // Reading n+1 times should give same value
        Assertions.assertTrue(store.sync());

        Assertions.assertEquals(l2,store.read(testData.array()));
        testData.clear();
        Assertions.assertEquals(29.83f,testData.getFloat());
        Assertions.assertEquals(98345,testData.getInt());
        Assertions.assertEquals(1498794656586L,testData.getLong());
        Assertions.assertEquals(78.83f,testData.getFloat());
        Assertions.assertEquals(8767,testData.getInt());

        Assertions.assertEquals(l2,store.read(testData.array()));
        testData.clear();
        Assertions.assertEquals(29.83f,testData.getFloat());
        Assertions.assertEquals(98345,testData.getInt());
        Assertions.assertEquals(1498794656586L,testData.getLong());
        Assertions.assertEquals(78.83f,testData.getFloat());
        Assertions.assertEquals(8767,testData.getInt());

        store.remove();
        Assertions.assertEquals(l1,store.readLength());
        Assertions.assertEquals(l1,store.readLength()); // Reading n+1 times should give same value

        Assertions.assertEquals(l1,store.read(testData.array()));
        testData.clear();
        Assertions.assertEquals(23.43f,testData.getFloat());
        Assertions.assertEquals(98,testData.getInt());
        Assertions.assertEquals(9841893746890L,testData.getLong());

        Assertions.assertEquals(l1,store.read(testData.array()));
        testData.clear();
        Assertions.assertEquals(23.43f,testData.getFloat());
        Assertions.assertEquals(98,testData.getInt());
        Assertions.assertEquals(9841893746890L,testData.getLong());

        store.remove();
        Assertions.assertFalse(store.isEmpty(),"Data store must be empty");

        byte[] testExpectedString = "DataStore is simple and yet very powerful".getBytes(StandardCharsets.UTF_8);
        byte[] testActualString = new byte[testExpectedString.length];
        Assertions.assertTrue(store.write(testExpectedString));
        Assertions.assertEquals(testExpectedString.length,store.read(testActualString));
        Assertions.assertArrayEquals(testExpectedString,testActualString);

        store.remove();
        byte[] testExpectedArray = new byte[]{0,1,2,3,4,5,6,7,8,9,11,22,33,44,55,66,77,88,99};
        byte[] testActualArray = new byte[]  {0,1,2,3,4,5,6,7,8,9, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        Assertions.assertTrue(store.write(testExpectedArray,10,9));
        Assertions.assertEquals(9,store.read(testActualArray,10,9));
        Assertions.assertArrayEquals(testExpectedArray,testActualArray);

        store.close();
    }
}
