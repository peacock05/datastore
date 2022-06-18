package peacock.datastore;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;

public class FileDataStoreQueueTest {

    @Test
    public void testDataStoreQueue() throws IOException {


        DataStore store = new FileDataStoreQueue("testQueue",
                Files.createTempDirectory("datastore"),5_000_000);
        Assertions.assertNotNull(store,"Store cannot be empty");
        Assertions.assertFalse(store.isEmpty(),"Data store must be empty");
        Assertions.assertEquals(5_000_000,store.capacity());
        Assertions.assertEquals(0,store.usage());
        Assertions.assertEquals(4999920,store.free());
        ByteBuffer testData = ByteBuffer.allocate(120);
        testData.putFloat(23.43f);
        testData.putInt(98);
        testData.putLong(9841893746890L);
        testData.flip();
        int l1 = testData.remaining();
        Assertions.assertTrue(store.write(testData),"Write do not fail here");
        Assertions.assertTrue(store.isEmpty(),"Data store must not be empty");

        testData.clear();
        testData.putFloat(29.83f);
        testData.putInt(98345);
        testData.putLong(1498794656586L);
        testData.putFloat(78.83f);
        testData.putInt(8767);
        testData.flip();
        int l2 = testData.remaining();
        Assertions.assertTrue(store.write(testData),"Write do not fail here");
        Assertions.assertTrue(store.isEmpty(),"Data store must not be empty");
        Assertions.assertEquals(2,store.size());
        Assertions.assertTrue(store.sync());
        Assertions.assertEquals(DataStore.NO_ERROR,store.getErrorCode());
        Assertions.assertNull(store.getException());


        Assertions.assertEquals(l1,store.readLength());
        Assertions.assertEquals(l1,store.readLength()); // Reading n+1 times should give same value

        testData.clear();
        Assertions.assertTrue(store.read(testData),"Read do not fail here");
        testData.flip();
        Assertions.assertEquals(l1,testData.remaining());
        Assertions.assertEquals(23.43f,testData.getFloat());
        Assertions.assertEquals(98,testData.getInt());
        Assertions.assertEquals(9841893746890L,testData.getLong());

        testData.clear();
        testData.putInt(98);
        Assertions.assertTrue(store.read(testData),"Read do not fail here");
        testData.flip();
        Assertions.assertEquals(98,testData.getInt());
        Assertions.assertEquals(l1,testData.remaining());
        Assertions.assertEquals(23.43f,testData.getFloat());
        Assertions.assertEquals(98,testData.getInt());
        Assertions.assertEquals(9841893746890L,testData.getLong());

        store.remove();
        Assertions.assertEquals(l2,store.readLength());
        Assertions.assertEquals(l2,store.readLength()); // Reading n+1 times should give same value

        testData.clear();
        Assertions.assertTrue(store.read(testData),"Read do not fail here");
        testData.flip();
        Assertions.assertEquals(l2,testData.remaining());
        Assertions.assertEquals(29.83f,testData.getFloat());
        Assertions.assertEquals(98345,testData.getInt());
        Assertions.assertEquals(1498794656586L,testData.getLong());
        Assertions.assertEquals(78.83f,testData.getFloat());
        Assertions.assertEquals(8767,testData.getInt());


        testData.clear();
        testData.putInt(98);
        Assertions.assertTrue(store.read(testData),"Read do not fail here");
        testData.flip();
        Assertions.assertEquals(98,testData.getInt());
        Assertions.assertEquals(l2,testData.remaining());
        Assertions.assertEquals(29.83f,testData.getFloat());
        Assertions.assertEquals(98345,testData.getInt());
        Assertions.assertEquals(1498794656586L,testData.getLong());
        Assertions.assertEquals(78.83f,testData.getFloat());
        Assertions.assertEquals(8767,testData.getInt());

        store.remove();
        Assertions.assertFalse(store.isEmpty(),"Data store must be empty");
    }
}
