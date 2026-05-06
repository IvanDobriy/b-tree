package ru.otus.btree.data.storage;

/**
 * Entity class for storing Index state.
 * Used for serialization and deserialization of storage index information.
 */
public class StorageIndex {
    // Size of serialized record: int (4) + boolean (1) + char[64] (128) + char[64] (128) = 261 bytes
    public static final int RECORD_SIZE = 261;
    public static final int MAX_NAME_LENGTH = 64;

    private int id;
    private boolean isUsed;
    private String entityName;
    private String fieldName;

    public StorageIndex() {
    }

    public StorageIndex(int id, boolean isUsed, String entityName, String fieldName) {
        this.id = id;
        this.isUsed = isUsed;
        setEntityName(entityName);
        setFieldName(fieldName);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public boolean isUsed() {
        return isUsed;
    }

    public void setUsed(boolean used) {
        isUsed = used;
    }

    public String getEntityName() {
        return entityName;
    }

    public void setEntityName(String entityName) {
        if (entityName != null && entityName.length() > MAX_NAME_LENGTH) {
            throw new IllegalArgumentException("Entity name must not exceed " + MAX_NAME_LENGTH + " characters");
        }
        this.entityName = entityName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        if (fieldName != null && fieldName.length() > MAX_NAME_LENGTH) {
            throw new IllegalArgumentException("Field name must not exceed " + MAX_NAME_LENGTH + " characters");
        }
        this.fieldName = fieldName;
    }

    /**
     * Serializes a StorageIndex to a byte array.
     *
     * @param index the index to serialize
     * @return byte array containing serialized data
     */
    public static byte[] serialize(StorageIndex index) {
        if (index == null) {
            return new byte[0];
        }

        try (java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
             java.io.DataOutputStream dos = new java.io.DataOutputStream(baos)) {

            dos.writeInt(index.id);
            dos.writeBoolean(index.isUsed);

            String entityName = index.entityName != null ? index.entityName : "";
            for (int i = 0; i < MAX_NAME_LENGTH; i++) {
                dos.writeChar(i < entityName.length() ? entityName.charAt(i) : '\0');
            }

            String fieldName = index.fieldName != null ? index.fieldName : "";
            for (int i = 0; i < MAX_NAME_LENGTH; i++) {
                dos.writeChar(i < fieldName.length() ? fieldName.charAt(i) : '\0');
            }

            dos.flush();
            return baos.toByteArray();
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to serialize StorageIndex", e);
        }
    }

    /**
     * Deserializes a byte array to a StorageIndex.
     *
     * @param data the byte array to deserialize
     * @return the deserialized StorageIndex, or null if data is null or empty
     */
    public static StorageIndex deserialize(byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }

        try (java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(data);
             java.io.DataInputStream dis = new java.io.DataInputStream(bais)) {

            StorageIndex index = new StorageIndex();
            index.id = dis.readInt();
            index.isUsed = dis.readBoolean();

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < MAX_NAME_LENGTH; i++) {
                char c = dis.readChar();
                if (c != '\0') {
                    sb.append(c);
                }
            }
            index.entityName = sb.length() > 0 ? sb.toString() : null;

            sb = new StringBuilder();
            for (int i = 0; i < MAX_NAME_LENGTH; i++) {
                char c = dis.readChar();
                if (c != '\0') {
                    sb.append(c);
                }
            }
            index.fieldName = sb.length() > 0 ? sb.toString() : null;

            return index;
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to deserialize StorageIndex", e);
        }
    }
}
