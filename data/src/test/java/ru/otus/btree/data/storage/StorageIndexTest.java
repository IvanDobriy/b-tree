package ru.otus.btree.data.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class StorageIndexTest {

    @Test
    public void testSerializeDeserialize() {
        StorageIndex original = new StorageIndex(1, true, "users", "name");

        byte[] serialized = StorageIndex.serialize(original);
        StorageIndex deserialized = StorageIndex.deserialize(serialized);

        assertNotNull(deserialized);
        assertEquals(1, deserialized.getId());
        assertTrue(deserialized.isUsed());
        assertEquals("users", deserialized.getEntityName());
        assertEquals("name", deserialized.getFieldName());
    }

    @Test
    public void testSerializeNullIndex() {
        byte[] serialized = StorageIndex.serialize(null);
        assertEquals(0, serialized.length);
    }

    @Test
    public void testDeserializeNullData() {
        StorageIndex result = StorageIndex.deserialize(null);
        assertNull(result);
    }

    @Test
    public void testDeserializeEmptyData() {
        StorageIndex result = StorageIndex.deserialize(new byte[0]);
        assertNull(result);
    }

    @Test
    public void testSerializeDeserializeWithNullNames() {
        StorageIndex original = new StorageIndex(2, false, null, null);

        byte[] serialized = StorageIndex.serialize(original);
        StorageIndex deserialized = StorageIndex.deserialize(serialized);

        assertNotNull(deserialized);
        assertEquals(2, deserialized.getId());
        assertFalse(deserialized.isUsed());
        assertNull(deserialized.getEntityName());
        assertNull(deserialized.getFieldName());
    }

    @Test
    public void testRoundTripMultipleIndexes() {
        StorageIndex[] indexes = {
            new StorageIndex(0, true, "entity0", "field0"),
            new StorageIndex(Integer.MAX_VALUE, false, "entity1", "field1"),
            new StorageIndex(42, true, null, "field2"),
            new StorageIndex(-1, false, "max-length-64-characters-1234567890123456789012345678901234", "max-length-64-characters-1234567890123456789012345678901234")
        };

        for (StorageIndex original : indexes) {
            byte[] serialized = StorageIndex.serialize(original);
            assertEquals(StorageIndex.RECORD_SIZE, serialized.length);
            StorageIndex deserialized = StorageIndex.deserialize(serialized);

            assertNotNull(deserialized);
            assertEquals(original.getId(), deserialized.getId());
            assertEquals(original.isUsed(), deserialized.isUsed());
            assertEquals(original.getEntityName(), deserialized.getEntityName());
            assertEquals(original.getFieldName(), deserialized.getFieldName());
        }
    }

    @Test
    public void testSetEntityNameExceedsMaxLength() {
        String longName = "a".repeat(StorageIndex.MAX_NAME_LENGTH + 1);
        StorageIndex index = new StorageIndex();
        assertThrows(IllegalArgumentException.class, () -> index.setEntityName(longName));
    }

    @Test
    public void testSetFieldNameExceedsMaxLength() {
        String longName = "a".repeat(StorageIndex.MAX_NAME_LENGTH + 1);
        StorageIndex index = new StorageIndex();
        assertThrows(IllegalArgumentException.class, () -> index.setFieldName(longName));
    }

    @Test
    public void testSetNamesMaxLength() {
        String maxName = "a".repeat(StorageIndex.MAX_NAME_LENGTH);
        StorageIndex index = new StorageIndex();
        assertDoesNotThrow(() -> index.setEntityName(maxName));
        assertDoesNotThrow(() -> index.setFieldName(maxName));
        assertEquals(maxName, index.getEntityName());
        assertEquals(maxName, index.getFieldName());
    }

    @Test
    public void testDefaultConstructor() {
        StorageIndex index = new StorageIndex();
        assertEquals(0, index.getId());
        assertFalse(index.isUsed());
        assertNull(index.getEntityName());
        assertNull(index.getFieldName());
    }

    @Test
    public void testSettersAndGetters() {
        StorageIndex index = new StorageIndex();
        index.setId(100);
        index.setUsed(true);
        index.setEntityName("test-entity");
        index.setFieldName("test-field");

        assertEquals(100, index.getId());
        assertTrue(index.isUsed());
        assertEquals("test-entity", index.getEntityName());
        assertEquals("test-field", index.getFieldName());
    }

    @Test
    public void testConstructorWithParameters() {
        StorageIndex index = new StorageIndex(7, true, "constructor-entity", "constructor-field");
        assertEquals(7, index.getId());
        assertTrue(index.isUsed());
        assertEquals("constructor-entity", index.getEntityName());
        assertEquals("constructor-field", index.getFieldName());
    }

    @Test
    public void testConstructorRejectsLongEntityName() {
        String longName = "b".repeat(StorageIndex.MAX_NAME_LENGTH + 5);
        assertThrows(IllegalArgumentException.class, () -> new StorageIndex(1, false, longName, "field"));
    }

    @Test
    public void testConstructorRejectsLongFieldName() {
        String longName = "b".repeat(StorageIndex.MAX_NAME_LENGTH + 5);
        assertThrows(IllegalArgumentException.class, () -> new StorageIndex(1, false, "entity", longName));
    }

    @Test
    public void testRecordSize() {
        assertEquals(261, StorageIndex.RECORD_SIZE);
    }
}
