package ru.otus.btree.lib.v1.btree;

import ru.otus.btree.lib.api.array.IArray;
import ru.otus.btree.lib.v1.array.SingleArray;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;

/**
 * Manages a list of page entities using SingleArray.
 * Provides operations to add, remove, find and manage pages.
 */
public class PageManagerList {
    public static final int PAGE_SIZE = 4096;
    private final IArray<PageManagerEntity> pages;
    private final FileChannel fileChannel;
    private PageManagerHeader header;


    public PageManagerList(FileChannel fileChannel) {
        this.fileChannel = Objects.requireNonNull(fileChannel, "fileChannel must not be null");
        this.pages = new SingleArray<>(0);
        this.header = loadHeader(fileChannel);
    }

    public PageManagerHeader getHeader() {
        return header;
    }

    public void setHeader(PageManagerHeader header) {
        this.header = Objects.requireNonNull(header, "header must not be null");
    }

    /**
     * Returns the page size.
     *
     * @return page size in bytes
     */
    public static int getPageSize() {
        return PAGE_SIZE;
    }

    /**
     * Returns the number of pages in the list.
     *
     * @return the page count
     */
    public int getSize() {
        return header.getSize;
    }

    /**
     * Loads header from the file channel.
     * Reads and deserializes the first page (PAGE_SIZE).
     * If file is empty, creates a new header with size = 0, saves it and returns.
     *
     * @param fileChannel the file channel to read from
     * @return the loaded or newly created PageManagerHeader
     */
    private PageManagerHeader loadHeader(FileChannel fileChannel) {
        Objects.requireNonNull(fileChannel, "fileChannel must not be null");

        try {
            // Check if file is empty
            if (fileChannel.size() == 0) {
                // Create new header with size = 0
                PageManagerHeader newHeader = new PageManagerHeader();
                newHeader.setSize(0);

                // Serialize and save to file
                byte[] headerData = PageManagerHeader.serialize(newHeader);
                ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE);
                buffer.put(headerData);
                buffer.flip();

                fileChannel.position(0);
                fileChannel.write(buffer);

                return newHeader;
            }

            // Read header from first page
            ByteBuffer buffer = readPage(fileChannel, 0);
            int bytesRead = buffer.remaining();

            if (bytesRead <= 0) {
                // Empty read, create new header
                PageManagerHeader newHeader = new PageManagerHeader();
                newHeader.setSize(0);
                return newHeader;
            }

            byte[] headerData = new byte[bytesRead];
            buffer.get(headerData);

            // Deserialize header
            PageManagerHeader loadedHeader = PageManagerHeader.deserialize(headerData);
            if (loadedHeader == null) {
                // Failed to deserialize, create new header
                loadedHeader = new PageManagerHeader();
                loadedHeader.setSize(0);
            }

            return loadedHeader;
        } catch (IOException e) {
            throw new RuntimeException("Failed to load header", e);
        }
    }

    /**
     * Loads a single page record from the file channel by index.
     * Data is read block by block. If the record spans across page boundaries,
     * the next block is loaded to complete the record.
     *
     * @param fileChannel the file channel to read from
     * @param recordIndex the index of the record to load
     * @return the loaded PageManagerEntity, or null if record doesn't exist
     * @throws IOException if an I/O error occurs
     */
    public PageManagerEntity loadPageRecord(FileChannel fileChannel, int recordIndex) {
        Objects.requireNonNull(fileChannel, "fileChannel must not be null");

        try {
            long offset = PAGE_SIZE + (long) recordIndex * PageManagerEntity.RECORD_SIZE;
            long fileSize = fileChannel.size();

            if (offset + PageManagerEntity.RECORD_SIZE > fileSize) {
                return null;
            }

            int pageIndex = (int) (offset / PAGE_SIZE);
            int positionInPage = (int) (offset % PAGE_SIZE);
            byte[] recordData = new byte[PageManagerEntity.RECORD_SIZE];

            if (positionInPage + PageManagerEntity.RECORD_SIZE <= PAGE_SIZE) {
                // Record fits entirely within one page
                ByteBuffer buffer = readPage(fileChannel, pageIndex);
                if (buffer.remaining() < positionInPage + PageManagerEntity.RECORD_SIZE) {
                    throw new IOException("Incomplete data read from page " + pageIndex);
                }
                buffer.position(positionInPage);
                buffer.get(recordData, 0, PageManagerEntity.RECORD_SIZE);
            } else {
                // Record spans across two pages
                int bytesFromFirstPage = PAGE_SIZE - positionInPage;
                int bytesFromSecondPage = PageManagerEntity.RECORD_SIZE - bytesFromFirstPage;

                // Read first page
                ByteBuffer buffer1 = readPage(fileChannel, pageIndex);
                if (buffer1.remaining() < bytesFromFirstPage) {
                    throw new IOException("Incomplete data in first page " + pageIndex);
                }
                buffer1.position(positionInPage);
                buffer1.get(recordData, 0, bytesFromFirstPage);

                // Read second page
                ByteBuffer buffer2 = readPage(fileChannel, pageIndex + 1);
                if (buffer2.remaining() < bytesFromSecondPage) {
                    throw new IOException("Incomplete data in second page " + (pageIndex + 1));
                }
                buffer2.get(recordData, bytesFromFirstPage, bytesFromSecondPage);
            }

            return PageManagerEntity.deserialize(recordData);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load page record", e);
        }
    }

    /**
     * Reads a single page into a ByteBuffer from the file channel.
     * The buffer is allocated with PAGE_SIZE and filled with data from the file.
     *
     * @param fileChannel the file channel to read from
     * @param pageIndex the index of the page to read
     * @return ByteBuffer containing the page data
     * @throws IOException if an I/O error occurs
     */
    private ByteBuffer readPage(FileChannel fileChannel, int pageIndex) throws IOException {
        Objects.requireNonNull(fileChannel, "fileChannel must not be null");

        ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE);
        fileChannel.position((long) pageIndex * PAGE_SIZE);
        int bytesRead = fileChannel.read(buffer);

        if (bytesRead < 0) {
            throw new IOException("Failed to read page " + pageIndex + ": end of file reached");
        }

        buffer.flip();
        return buffer;
    }
}
