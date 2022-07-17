package io.github.peacock05.datastore;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Random;

public class FileDataStoreQueueTest {

    @Test
    public void testFifoDataStore() throws Exception {

        DataStore store = new FileDataStoreQueue("testQueue",
                Files.createTempDirectory("datastore").toString(),5_000_000);
        Assertions.assertNotNull(store,"Store cannot be empty");
        Assertions.assertTrue(store.isEmpty(),"Data store must be empty");
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
        Assertions.assertFalse(store.isEmpty(),"Data store must not be empty");
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
        Assertions.assertFalse(store.isEmpty(),"Data store must not be empty");
        Assertions.assertEquals(2,store.count());

        Assertions.assertEquals(0,store.getErrorCode());
        Assertions.assertNull(store.getException());

        Assertions.assertEquals(l1,store.readLength());
        Assertions.assertEquals(l1,store.readLength()); // Reading n+1 times should give same value
        Assertions.assertTrue(store.sync());

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
        Assertions.assertEquals(l2,store.readLength());
        Assertions.assertEquals(l2,store.readLength()); // Reading n+1 times should give same value


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
        Assertions.assertTrue(store.isEmpty(),"Data store must be empty");

        byte[] testExpectedString = "DataStore is simple and yet very powerful".getBytes(StandardCharsets.UTF_8);
        byte[] testActualString = new byte[testExpectedString.length];
        Assertions.assertTrue(store.write(testExpectedString));
        Assertions.assertEquals(testExpectedString.length,store.read(testActualString));
        Assertions.assertArrayEquals(testExpectedString,testActualString);
        store.remove();

        byte[] testExpectedArray = new byte[]{1,2,3,4,5,6,7,8,9,11,22,33,44,55,66,77,88,99};
        byte[] testActualArray = new byte[]{1,2,3,4,5,6,7,8,9,0,0,0,0,0,0,0,0,0};
        Assertions.assertTrue(store.write(testExpectedArray,9,9));
        Assertions.assertEquals(9,store.read(testActualArray,9,9));
        Assertions.assertArrayEquals(testExpectedArray,testActualArray);
        store.remove();

        store.close();
    }

    @Test
    public void testPersistence() throws IOException {

        Path tempDir = Files.createTempDirectory("datastore");
        Random random = new Random();
        byte[] testData1 = new byte[512], readData1 = new byte[512];
        byte[] testData2 = new byte[342], readData2 = new byte[342];
        random.nextBytes(testData1);
        random.nextBytes(testData2);

        try(DataStore store = new FileDataStoreQueue("testQueue", tempDir.toString(),5_000_000)){
            Assertions.assertTrue(store.isEmpty(),"Data store must not be empty");
            Assertions.assertTrue(store.write(testData1), "Write do not fail here");
            Assertions.assertTrue(store.write(testData2), "Write do not fail here");
            store.sync();
        } catch (Exception e) {
           Assertions.fail(e);
        }

        try(DataStore store = new FileDataStoreQueue("testQueue", tempDir.toString(),5_000_000)){
            Assertions.assertFalse(store.isEmpty(),"Data store must not be empty");
            Assertions.assertEquals(readData1.length,store.read(readData1));
            Assertions.assertArrayEquals(testData1,readData1);
            store.remove();
            Assertions.assertEquals(readData2.length,store.read(readData2));
            Assertions.assertArrayEquals(testData2,readData2);
            store.remove();
            store.sync();
        } catch (Exception e) {
            Assertions.fail(e);
        }

    }

    @Test
    public void testRollOver() throws IOException {

        Path tempDir = Files.createTempDirectory("datastore");
        byte[] testData = new byte[512];
        int writeCount = 0;
        int readCount = 0;
        for (int i = 0; i < 2; i++) {
            try(DataStore store = new FileDataStoreQueue("testQueue", tempDir.toString(),5_000_000)){

                for(;;){
                    Arrays.fill(testData,(byte)writeCount);
                    if(!store.write(testData))
                        break;
                    writeCount++;
                }
                store.sync();
            } catch (Exception e) {
                Assertions.fail(e);
            }

            try(DataStore store = new FileDataStoreQueue("testQueue", tempDir.toString(),5_000_000)){

                while (store.read(testData) == testData.length) {

                    for (byte value : testData) {
                        Assertions.assertEquals((byte) readCount, value);
                    }
                    readCount++;
                    store.remove();
                }

                store.sync();
            } catch (Exception e) {
                Assertions.fail(e);
            }
        }

        // For const data length.
        // Capacity = Meta Data length * 2 + Count*(header length + Data length)
        // Count = (Capacity - Meta Data length * 2) / (header length + Data length)
        // Count = (5_000_000 - 32*2)/(16+512)
        // Count = 9,469.5757

        Assertions.assertEquals(18938,writeCount);
        Assertions.assertEquals(18938,readCount);
    }

}
