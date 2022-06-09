# Datastore

Datastore project provides Java library for reading and writing data in FIFO and LIFO order from the files.

# Usage

```java

import peacock.datastore.DataStore;
import peacock.datastore.FileDataStoreStack;

import java.nio.ByteBuffer;
import java.nio.file.Paths;

public class MainApp {
    public static void main(String[] args) {
        try (DataStore store = new FileDataStoreStack("backup", Paths.get("database"), 5_000_000)) {
            
            // Prepare the data to write
            ByteBuffer data = ByteBuffer.allocate(1024);
            data.putFloat(23.78);
            data.putInt(34654);
            data.flip();
            
            // Write data to file in LIFO order
            store.write(data);
            
            // Prepare second data
            data.reset();
            data.putFloat(23.78);
            data.putInt(34654);
            data.flip();
            
            // Write data to file in LIFO order
            store.write(data);
        }
    }
}

```