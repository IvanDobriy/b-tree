package ru.otus.btree.lib.v1.btree;

/**
 * Header for PageManager containing metadata about page allocation.
 * Stores information like next available page ID, total pages, etc.
 */
public class PageManagerHeader {
    private long size;

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    /**
     * Serializes a PageManagerHeader to a byte array.
     *
     * @param header the header to serialize
     * @return byte array containing serialized data
     */
    public static byte[] serialize(PageManagerHeader header) {
        if (header == null) {
            return new byte[0];
        }

        try (java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
             java.io.DataOutputStream dos = new java.io.DataOutputStream(baos)) {

            dos.writeLong(header.size);

            dos.flush();
            return baos.toByteArray();
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to serialize PageManagerHeader", e);
        }
    }

    /**
     * Deserializes a byte array to a PageManagerHeader.
     *
     * @param data the byte array to deserialize
     * @return the deserialized PageManagerHeader, or null if data is null or empty
     */
    public static PageManagerHeader deserialize(byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }

        try (java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(data);
             java.io.DataInputStream dis = new java.io.DataInputStream(bais)) {

            PageManagerHeader header = new PageManagerHeader();
            header.size = dis.readLong();

            return header;
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to deserialize PageManagerHeader", e);
        }
    }
}
