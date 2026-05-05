package ru.otus.btree.data.storage;

/**
 * Entity class for storing Storage state.
 * Used for serialization and deserialization of storage entity information.
 */
public class StorageEntity {
    // Size of serialized record: int (4) + boolean (1) + char[64] (128) = 133 bytes
    public static final int RECORD_SIZE = 133;
    public static final int MAX_NAME_LENGTH = 64;

    private int id;
    private boolean isUsed;
    private String name;

    public StorageEntity() {
    }

    public StorageEntity(int id, boolean isUsed, String name) {
        this.id = id;
        this.isUsed = isUsed;
        setName(name);
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

    public String getName() {
        return name;
    }

    public void setName(String name) {
        if (name != null && name.length() > MAX_NAME_LENGTH) {
            throw new IllegalArgumentException("Name must not exceed " + MAX_NAME_LENGTH + " characters");
        }
        this.name = name;
    }

    /**
     * Serializes a StorageEntity to a byte array.
     *
     * @param entity the entity to serialize
     * @return byte array containing serialized data
     */
    public static byte[] serialize(StorageEntity entity) {
        if (entity == null) {
            return new byte[0];
        }

        try (java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
             java.io.DataOutputStream dos = new java.io.DataOutputStream(baos)) {

            dos.writeInt(entity.id);
            dos.writeBoolean(entity.isUsed);

            String name = entity.name != null ? entity.name : "";
            for (int i = 0; i < MAX_NAME_LENGTH; i++) {
                dos.writeChar(i < name.length() ? name.charAt(i) : '\0');
            }

            dos.flush();
            return baos.toByteArray();
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to serialize StorageEntity", e);
        }
    }

    /**
     * Deserializes a byte array to a StorageEntity.
     *
     * @param data the byte array to deserialize
     * @return the deserialized StorageEntity, or null if data is null or empty
     */
    public static StorageEntity deserialize(byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }

        try (java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(data);
             java.io.DataInputStream dis = new java.io.DataInputStream(bais)) {

            StorageEntity entity = new StorageEntity();
            entity.id = dis.readInt();
            entity.isUsed = dis.readBoolean();

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < MAX_NAME_LENGTH; i++) {
                char c = dis.readChar();
                if (c == '\0') {
                    break;
                }
                sb.append(c);
            }
            entity.name = sb.length() > 0 ? sb.toString() : null;

            return entity;
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to deserialize StorageEntity", e);
        }
    }
}
