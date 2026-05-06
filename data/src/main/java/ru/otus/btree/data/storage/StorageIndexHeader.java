package ru.otus.btree.data.storage;

/**
 * Header for StorageIndex containing metadata about index records.
 * Stores information like total number of indexes.
 */
public class StorageIndexHeader {
    private int size;

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    /**
     * Serializes a StorageIndexHeader to a byte array.
     *
     * @param header the header to serialize
     * @return byte array containing serialized data
     */
    public static byte[] serialize(StorageIndexHeader header) {
        if (header == null) {
            return new byte[0];
        }

        try (java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
             java.io.DataOutputStream dos = new java.io.DataOutputStream(baos)) {

            dos.writeInt(header.size);

            dos.flush();
            return baos.toByteArray();
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to serialize StorageIndexHeader", e);
        }
    }

    /**
     * Deserializes a byte array to a StorageIndexHeader.
     *
     * @param data the byte array to deserialize
     * @return the deserialized StorageIndexHeader, or null if data is null or empty
     */
    public static StorageIndexHeader deserialize(byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }

        try (java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(data);
             java.io.DataInputStream dis = new java.io.DataInputStream(bais)) {

            StorageIndexHeader header = new StorageIndexHeader();
            header.size = dis.readInt();

            return header;
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to deserialize StorageIndexHeader", e);
        }
    }
}
