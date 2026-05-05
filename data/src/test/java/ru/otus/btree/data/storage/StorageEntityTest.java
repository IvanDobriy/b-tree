package ru.otus.btree.data.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class StorageEntityTest {

    @Test
    public void testSerializeDeserialize() {
        StorageEntity original = new StorageEntity(1, true, "user");

        byte[] serialized = StorageEntity.serialize(original);
        StorageEntity deserialized = StorageEntity.deserialize(serialized);

        assertNotNull(deserialized);
        assertEquals(1, deserialized.getId());
        assertTrue(deserialized.isUsed());
        assertEquals("user", deserialized.getName());
    }

    @Test
    public void testSerializeNullEntity() {
        byte[] serialized = StorageEntity.serialize(null);
        assertEquals(0, serialized.length);
    }

    @Test
    public void testDeserializeNullData() {
        StorageEntity result = StorageEntity.deserialize(null);
        assertNull(result);
    }

    @Test
    public void testDeserializeEmptyData() {
        StorageEntity result = StorageEntity.deserialize(new byte[0]);
        assertNull(result);
    }

    @Test
    public void testSerializeDeserializeWithNullName() {
        StorageEntity original = new StorageEntity(2, false, null);

        byte[] serialized = StorageEntity.serialize(original);
        StorageEntity deserialized = StorageEntity.deserialize(serialized);

        assertNotNull(deserialized);
        assertEquals(2, deserialized.getId());
        assertFalse(deserialized.isUsed());
        assertNull(deserialized.getName());
    }

    @Test
    public void testRoundTripMultipleEntities() {
        StorageEntity[] entities = {
            new StorageEntity(0, true, "entity0"),
            new StorageEntity(Integer.MAX_VALUE, false, "entity1"),
            new StorageEntity(42, true, null),
            new StorageEntity(-1, false, "max-length-64-characters-1234567890123456789012345678901234")
        };

        for (StorageEntity original : entities) {
            byte[] serialized = StorageEntity.serialize(original);
            assertEquals(StorageEntity.RECORD_SIZE, serialized.length);
            StorageEntity deserialized = StorageEntity.deserialize(serialized);

            assertNotNull(deserialized);
            assertEquals(original.getId(), deserialized.getId());
            assertEquals(original.isUsed(), deserialized.isUsed());
            assertEquals(original.getName(), deserialized.getName());
        }
    }

    @Test
    public void testSetNameExceedsMaxLength() {
        String longName = "a".repeat(StorageEntity.MAX_NAME_LENGTH + 1);
        StorageEntity entity = new StorageEntity();
        assertThrows(IllegalArgumentException.class, () -> entity.setName(longName));
    }

    @Test
    public void testSetNameMaxLength() {
        String maxName = "a".repeat(StorageEntity.MAX_NAME_LENGTH);
        StorageEntity entity = new StorageEntity();
        assertDoesNotThrow(() -> entity.setName(maxName));
        assertEquals(maxName, entity.getName());
    }

    @Test
    public void testDefaultConstructor() {
        StorageEntity entity = new StorageEntity();
        assertEquals(0, entity.getId());
        assertFalse(entity.isUsed());
        assertNull(entity.getName());
    }

    @Test
    public void testSettersAndGetters() {
        StorageEntity entity = new StorageEntity();
        entity.setId(100);
        entity.setUsed(true);
        entity.setName("test-name");

        assertEquals(100, entity.getId());
        assertTrue(entity.isUsed());
        assertEquals("test-name", entity.getName());
    }

    @Test
    public void testConstructorWithParameters() {
        StorageEntity entity = new StorageEntity(7, true, "constructor-test");
        assertEquals(7, entity.getId());
        assertTrue(entity.isUsed());
        assertEquals("constructor-test", entity.getName());
    }

    @Test
    public void testConstructorRejectsLongName() {
        String longName = "b".repeat(StorageEntity.MAX_NAME_LENGTH + 5);
        assertThrows(IllegalArgumentException.class, () -> new StorageEntity(1, false, longName));
    }

    @Test
    public void testRecordSize() {
        assertEquals(133, StorageEntity.RECORD_SIZE);
    }
}
