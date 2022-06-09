package peacock.datastore;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;

public class FileDataStoreStackTest {

    @Test
    public void testDataStoreStack() throws IOException {

        DataStore store = new FileDataStoreStack("testQueue",
                Files.createTempDirectory("datastore"),5_000_000);
        Assertions.assertNotNull(store,"Store cannot be empty");
    }
}
