package ru.otus.btree.lib.v1.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;

public class StorageMangerList {
    public static final int PAGE_SIZE = 4096;
    private final FileChannel fileChannel;
    private StorageMangerHeader header;

    public StorageMangerList(FileChannel fileChannel) {
        this.fileChannel = Objects.requireNonNull(fileChannel, "fileChannel must not be null");
        this.header = loadHeader();
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    public StorageMangerHeader getHeader() {
        return header;
    }

    public void setHeader(StorageMangerHeader header) {
        this.header = Objects.requireNonNull(header, "header must not be null");
    }

    /**
     * Returns the number of records in the list.
     *
     * @return the record count
     */
    public int getSize() {
        return (int) header.getSize();
    }

    /**
     * Gets a storage entity by index.
     * Loads the entity from file if it exists.
     *
     * @param recordIndex the index of the record to load
     * @return the StorageManagerEntity, or null if not found
     */
    public StorageManagerEntity getEntity(int recordIndex) {
        return loadRecord(recordIndex);
    }

    /**
     * Saves a storage entity to the file.
     * Uses entity.getPosition() as the record index and savePageRecord to persist the entity.
     * If the entity's ID exceeds current header size, updates header size and saves it.
     *
     * @param entity the entity to save
     */
    public void setEntity(StorageManagerEntity entity) {
        Objects.requireNonNull(entity, "entity must not be null");

        // Check if entity ID exceeds current header size
        int id = entity.getId();
        long position = entity.getPosition();
        if (id >= header.getSize()) {
            header.setSize(id + 1);
            header.setFileSize(position + entity.getSize());
            saveHeader();
        }

        savePageRecord(entity);
    }


    /**
     * Loads header from the file channel.
     * Reads and deserializes the first page (PAGE_SIZE).
     * If file is empty, creates a new header with size = 0, saves it and returns.
     *
     * @return the loaded or newly created StorageMangerHeader
     */
    private StorageMangerHeader loadHeader() {
        try {
            // Check if file is empty
            if (fileChannel.size() == 0) {
                // Create new header with size = 0
                StorageMangerHeader newHeader = new StorageMangerHeader();
                newHeader.setSize(0);
                newHeader.setFileSize(0);

                // Serialize and save to file
                byte[] headerData = StorageMangerHeader.serialize(newHeader);
                ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE);
                buffer.put(headerData);
                buffer.clear();

                fileChannel.position(0);
                while (buffer.hasRemaining()) {
                    fileChannel.write(buffer);
                }

                return newHeader;
            }

            // Read header from first page
            ByteBuffer buffer = readPage(0);
            int bytesRead = buffer.remaining();

            if (bytesRead <= 0) {
                // Empty read, create new header
                StorageMangerHeader newHeader = new StorageMangerHeader();
                newHeader.setSize(0);
                newHeader.setFileSize(0);
                return newHeader;
            }

            byte[] headerData = new byte[bytesRead];
            buffer.get(headerData);

            // Deserialize header
            StorageMangerHeader loadedHeader = StorageMangerHeader.deserialize(headerData);
            if (loadedHeader == null) {
                // Failed to deserialize, create new header
                loadedHeader = new StorageMangerHeader();
                loadedHeader.setSize(0);
            }

            return loadedHeader;
        } catch (IOException e) {
            throw new RuntimeException("Failed to load header", e);
        }
    }

    /**
     * Loads a single page record from the file channel by index.
     *
     * @param recordIndex the index of the record to load
     * @return the loaded StorageManagerEntity, or null if record doesn't exist
     */
    private StorageManagerEntity loadRecord(int recordIndex) {
        try {
            long offset = calculateOffset(recordIndex);
            long fileSize = fileChannel.size();

            if (offset + StorageManagerEntity.RECORD_SIZE > fileSize) {
                return null;
            }

            int pageIndex = (int) (offset / PAGE_SIZE);
            int positionInPage = (int) (offset % PAGE_SIZE);
            byte[] recordData = new byte[StorageManagerEntity.RECORD_SIZE];

            if (positionInPage + StorageManagerEntity.RECORD_SIZE <= PAGE_SIZE) {
                // Record fits entirely within one page
                ByteBuffer buffer = readPage(pageIndex);
                if (buffer.remaining() < positionInPage + StorageManagerEntity.RECORD_SIZE) {
                    throw new IOException("Incomplete data read from page " + pageIndex);
                }
                buffer.position(positionInPage);
                buffer.get(recordData, 0, StorageManagerEntity.RECORD_SIZE);
            } else {
                // Record spans across two pages
                int bytesFromFirstPage = PAGE_SIZE - positionInPage;
                int bytesFromSecondPage = StorageManagerEntity.RECORD_SIZE - bytesFromFirstPage;

                // Read first page
                ByteBuffer buffer1 = readPage(pageIndex);
                if (buffer1.remaining() < bytesFromFirstPage) {
                    throw new IOException("Incomplete data in first page " + pageIndex);
                }
                buffer1.position(positionInPage);
                buffer1.get(recordData, 0, bytesFromFirstPage);

                // Read second page
                ByteBuffer buffer2 = readPage(pageIndex + 1);
                if (buffer2.remaining() < bytesFromSecondPage) {
                    throw new IOException("Incomplete data in second page " + (pageIndex + 1));
                }
                buffer2.get(recordData, bytesFromFirstPage, bytesFromSecondPage);
            }

            return StorageManagerEntity.deserialize(recordData);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load page record", e);
        }
    }

    /**
     * Calculates the offset for a record in the file.
     *
     * @param recordIndex the index of the record
     * @return the calculated offset
     */
    private long calculateOffset(int recordIndex) {
        return PAGE_SIZE + (long) recordIndex * StorageManagerEntity.RECORD_SIZE;
    }

    /**
     * Reads a single page into a ByteBuffer from the file channel.
     *
     * @param pageIndex the index of the page to read
     * @return ByteBuffer containing the page data
     * @throws IOException if an I/O error occurs
     */
    private ByteBuffer readPage(int pageIndex) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE);
        fileChannel.position((long) pageIndex * PAGE_SIZE);
        int bytesRead = fileChannel.read(buffer);

        if (bytesRead < 0) {
            throw new IOException("Failed to read page " + pageIndex + ": end of file reached");
        }

        buffer.flip();
        return buffer;
    }

    /**
     * Saves a page record to the file channel.
     * Uses entity.getPosition() as the record index.
     * Data is written block by block. If the record spans across page boundaries,
     * both pages are written to complete the record.
     *
     * @param entity the entity to save
     */
    private void savePageRecord(StorageManagerEntity entity) {
        Objects.requireNonNull(entity, "entity must not be null");

        try {
            byte[] recordData = StorageManagerEntity.serialize(entity);
            long offset = calculateOffset((int) entity.getPosition());

            int pageIndex = (int) (offset / PAGE_SIZE);
            int positionInPage = (int) (offset % PAGE_SIZE);

            // Ensure required pages exist
            long requiredSize = (long) (pageIndex + 1) * PAGE_SIZE;
            if (positionInPage + StorageManagerEntity.RECORD_SIZE > PAGE_SIZE) {
                // Record spans to next page
                requiredSize = (long) (pageIndex + 2) * PAGE_SIZE;
            }
            ensureFileSize(requiredSize);

            if (positionInPage + StorageManagerEntity.RECORD_SIZE <= PAGE_SIZE) {
                // Record fits entirely within one page
                ByteBuffer buffer = readPage(pageIndex);
                if (buffer.remaining() < positionInPage) {
                    throw new IOException("Incomplete page data at page " + pageIndex);
                }
                buffer.position(positionInPage);
                buffer.put(recordData, 0, StorageManagerEntity.RECORD_SIZE);

                buffer.flip();
                writePage(pageIndex, buffer);
            } else {
                // Record spans across two pages
                int bytesInFirstPage = PAGE_SIZE - positionInPage;
                int bytesInSecondPage = StorageManagerEntity.RECORD_SIZE - bytesInFirstPage;

                // Read and update first page
                ByteBuffer buffer1 = readPage(pageIndex);
                if (buffer1.remaining() < positionInPage) {
                    throw new IOException("Incomplete page data at page " + pageIndex);
                }
                buffer1.position(positionInPage);
                buffer1.put(recordData, 0, bytesInFirstPage);

                buffer1.flip();
                writePage(pageIndex, buffer1);

                // Read and update second page
                ByteBuffer buffer2 = readPage(pageIndex + 1);
                buffer2.put(recordData, bytesInFirstPage, bytesInSecondPage);

                buffer2.flip();
                writePage(pageIndex + 1, buffer2);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to save page record", e);
        }
    }

    /**
     * Ensures the file is at least the specified size by extending it if necessary.
     *
     * @param requiredSize the minimum required file size
     * @throws IOException if an I/O error occurs
     */
    private void ensureFileSize(long requiredSize) throws IOException {
        long currentSize = fileChannel.size();
        if (currentSize < requiredSize) {
            fileChannel.position(currentSize);
            ByteBuffer emptyPage = ByteBuffer.allocate(PAGE_SIZE);
            while (fileChannel.position() < requiredSize) {
                emptyPage.clear();
                while (emptyPage.hasRemaining()) {
                    fileChannel.write(emptyPage);
                }
            }
        }
    }

    /**
     * Writes a ByteBuffer to the specified page in the file channel.
     *
     * @param pageIndex the index of the page to write
     * @param buffer    the buffer containing data to write
     * @throws IOException if an I/O error occurs
     */
    private void writePage(int pageIndex, ByteBuffer buffer) throws IOException {
        Objects.requireNonNull(buffer, "buffer must not be null");

        fileChannel.position((long) pageIndex * PAGE_SIZE);
        while (buffer.hasRemaining()) {
            fileChannel.write(buffer);
        }
    }

    /**
     * Saves the header to the file channel.
     * Serializes the header and writes it to the first page.
     */
    private void saveHeader() {
        try {
            byte[] headerData = StorageMangerHeader.serialize(header);
            ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE);
            buffer.put(headerData);
            buffer.flip();

            fileChannel.position(0);
            while (buffer.hasRemaining()) {
                fileChannel.write(buffer);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to save header", e);
        }
    }
}
