# Datastore

Datastore project provides Java library to read, write and delete arrays in FIFO and LIFO order. It uses file to
back up the arrays

Provide simple `write()` `read()` and `remove()` method to store, read and delete the array.

### Download

[Datastore jar downloads](https://github.com/peacock05?tab=packages&repo_name=datastore) are available from GitHub Packages.

### Requirements
#### Minimum Java version
- Datastore 1.0 and newer: Java 8

### Usage

#### Example using FIFO queue

```java

import io.github.peacock05.datastore.DataStore;
import io.github.peacock05.datastore.FileDataStoreQueue;

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
            // Write 1st data to disk
            store.sync(); 
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
            // Write 2nd data to disk
            store.sync();
            
            // Read 1st data 
            store.read(data.array());

            // Remove 1st data
            store.remove();
            // Remove 1st data from disk
            store.sync();
            
            // Read 2nd data
            store.read(data.array());

            // Remove 2nd data
            store.remove();
            // Remove 2nd data from disk
            store.sync();

        } catch (IOException e){
            // Unable to create the queue.
            e.printStackTrace();
        }
    }
}

```

#### Example using LIFO queue

```java
import io.github.peacock05.datastore.DataStore;
import io.github.peacock05.datastore.FileDataStoreStack;

import java.io.IOException;
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
            // Write 1st data to disk
            store.sync();
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
            // Write 2nd data to disk
            store.sync();

            // Read 2nd data 
            store.read(data.array());

            // Remove 2nd data
            store.remove();
            // Remove 2nd data to disk
            store.sync();
            // Read 1st data
            store.read(data.array());

            // Remove 1st data
            store.remove();
            // Remove 2nd data to disk
            store.sync();

        } catch (IOException e){
            // Unable to create the queue.
            e.printStackTrace();
        }
    }
}
```

### Developer Guide

The Developer guide is available at [https://github.com/peacock05/datastore/wiki/Developer-Guide](https://github.com/peacock05/datastore/wiki/Developer-Guide)


### License

Distributed under the MIT License. See `LICENSE.txt` for more information. 
