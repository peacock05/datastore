# Datastore

Datastore project provides Java library to read, write and delete arrays in FIFO and LIFO order. It uses file to
back up the arrays

Provide simple `write()` `read()` and `remove()` method to store, read and delete the array.

### Requirements
#### Minimum Java version
- Datastore 1.0 and newer: Java 8

### Usage

#### Example using FIFO queue

```java

import DataStore;

import java.nio.ByteBuffer;

public class MainApp {
    public static void main(String[] args) {

        try (DataStore store = new FileDataStoreQueue("backup", "/database", 5000000)) {

            // Prepare 1st data
            ByteBuffer data = ByteBuffer.allocate(120);
            data.putFloat(23.43f);
            data.putInt(98);
            data.putLong(9841893746890L);
            data.flip();

            // Write 1st data
            store.write(data.array(), data.position(), data.remaining());

            // Prepare second data
            data.reset();
            data.putFloat(29.83f);
            data.putInt(98345);
            data.putLong(1498794656586L);
            data.putFloat(78.83f);
            data.putInt(8767);
            data.flip();

            // Write 2nd data 
            store.write(data.array(), data.position(), data.remaining());

            // Read 1st data 
            store.read(data.array());

            // Remove 1st data
            store.remove();

            // Read 2nd data
            store.read(data.array());

            // Remove 2nd data
            store.remove();

        }
    }
}

```

#### Example using LIFO queue

```java
import DataStore;
import FileDataStoreStack;

import java.nio.ByteBuffer;

public class MainApp {
    public static void main(String[] args) {

        try (DataStore store = new FileDataStoreStack("backup", "/database", 5000000)) {

            // Prepare 1st data
            ByteBuffer data = ByteBuffer.allocate(120);
            data.putFloat(23.43f);
            data.putInt(98);
            data.putLong(9841893746890L);
            data.flip();

            // Write 1st data
            store.write(data.array(), data.position(), data.remaining());

            // Prepare second data
            data.reset();
            data.putFloat(29.83f);
            data.putInt(98345);
            data.putLong(1498794656586L);
            data.putFloat(78.83f);
            data.putInt(8767);
            data.flip();

            // Write 2nd data 
            store.write(data.array(), data.position(), data.remaining());

            // Read 2nd data 
            store.read(data.array());

            // Remove 2nd data
            store.remove();

            // Read 1st data
            store.read(data.array());

            // Remove 1st data
            store.remove();

        }
    }
}
```

### License

Distributed under the MIT License. See `LICENSE.txt` for more information.