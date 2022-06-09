package peacock.datastore;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;

public class FileDataStoreQueueTest {

    @Test
    public void testDataStoreQueue() throws IOException {

        DataStore store = new FileDataStoreQueue("testQueue",
                Files.createTempDirectory("datastore"),5_000_000);
        Assertions.assertNotNull(store,"Store cannot be empty");
    }
}
