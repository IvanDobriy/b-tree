package ru.otus.btree.data.storage;

/**
 * Header for StorageEntity containing metadata about entity records.
 * Stores information like total number of entities.
 */
public class StorageEntityHeader {
    private int size;

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    /**
     * Serializes a StorageEntityHeader to a byte array.
     *
     * @param header the header to serialize
     * @return byte array containing serialized data
     */
    public static byte[] serialize(StorageEntityHeader header) {
        if (header == null) {
            return new byte[0];
        }

        try (java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
             java.io.DataOutputStream dos = new java.io.DataOutputStream(baos)) {

            dos.writeInt(header.size);

            dos.flush();
            return baos.toByteArray();
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to serialize StorageEntityHeader", e);
        }
    }

    /**
     * Deserializes a byte array to a StorageEntityHeader.
     *
     * @param data the byte array to deserialize
     * @return the deserialized StorageEntityHeader, or null if data is null or empty
     */
    public static StorageEntityHeader deserialize(byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }

        try (java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(data);
             java.io.DataInputStream dis = new java.io.DataInputStream(bais)) {

            StorageEntityHeader header = new StorageEntityHeader();
            header.size = dis.readInt();

            return header;
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to deserialize StorageEntityHeader", e);
        }
    }
}
