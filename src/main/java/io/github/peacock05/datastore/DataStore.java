package io.github.peacock05.datastore;

/**
 * DataStore interface provides access to read, write, remove byte[] data from
 * persistent queue
 */
public interface DataStore extends AutoCloseable{

    /**
     * Writes {@code len} bytes from the specified byte array
     * starting at offset {@code off} to this data store.
     *
     * @param      b     the data.
     * @param      off   the start offset in the data.
     * @param      len   the number of bytes to write.
     * @return     {@code false}  if an I/O error occurs.
     */
    boolean write(byte[] b, int off, int len);

    /**
     * Writes {@code b.length} bytes from the specified byte array
     * to this data store.
     *
     * @param      b   the data.
     * @return     {@code false}  if an I/O error occurs.
     */
    boolean write(byte[] b);

    /**
     * Reads up to {@code len} bytes of data from this data store into an
     * array of bytes. This method blocks until all bytes are read
     *
     * @param      b     the buffer into which the data is read.
     * @param      off   the start offset in array {@code b}
     *                   at which the data is written.
     * @param      len   the maximum number of bytes read.
     * @return     the total number of bytes read into the buffer, or
     *             {@code -1} if there is no more elements in the data store.
     */
    int	read(byte[] b, int off, int len);

    /**
     * Reads up to {@code b.length} bytes of data from this data store
     * into an array of bytes. This method blocks until all bytes are read
     *
     * @param      b   the buffer into which the data is read.
     * @return     the total number of bytes read into the buffer, or
     *             {@code -1} if there is no more elements in the data store.
     *
     */
    int	read(byte[] b);

    /**
     * Returns the size of the array stored in the data store.
     * @return     the size of the array stored in the data store, or
     *             {@code -1} if there is no more elements in the data store.
     */
    int readLength();

    /**
     * Store pointers and data to the disk.
     */
    boolean sync();

    /**
     * Remove the element from the data store.
     */
    void remove();

    /**
     * Check if the data store is empty
     * @return {@code true}  if the data store is empty
     */
    boolean isEmpty();

    /**
     * Get the numbers of elements stored into the data store.
     * @return Number of elements stored in the data store.
     */
    long count();

    /**
     * Get the capacity of the data store
     * @return capacity
     */
    long capacity();

    /**
     * Get the amount of storage used by this data store.
     * @return Amount of storage used by this data store in bytes.
     */
    long usage();

    /**
     * Get the amount of free space available in this data store.
     * @return Amount of space left by this data store in bytes.
     */
    long free();

    /**
     * Get the error code if any in the  last operation of data store.
     * @return Non-zero value in case of error in the last operation
     */
    int getErrorCode();

    /**
     * Get the exception if any in the last operation of data store.
     * @return {@code Exception} of the last operation, {@code null}
     * in case of no exception.
     */
    Exception getException();
}
