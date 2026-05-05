package ru.otus.btree.data.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class StorageEntityHeaderTest {

    @Test
    public void testSerializeDeserialize() {
        StorageEntityHeader original = new StorageEntityHeader();
        original.setSize(1024);

        byte[] serialized = StorageEntityHeader.serialize(original);
        StorageEntityHeader deserialized = StorageEntityHeader.deserialize(serialized);

        assertNotNull(deserialized);
        assertEquals(1024, deserialized.getSize());
    }

    @Test
    public void testSerializeNullHeader() {
        byte[] serialized = StorageEntityHeader.serialize(null);
        assertEquals(0, serialized.length);
    }

    @Test
    public void testDeserializeNullData() {
        StorageEntityHeader result = StorageEntityHeader.deserialize(null);
        assertNull(result);
    }

    @Test
    public void testDeserializeEmptyData() {
        StorageEntityHeader result = StorageEntityHeader.deserialize(new byte[0]);
        assertNull(result);
    }

    @Test
    public void testRoundTripMultipleSizes() {
        int[] sizes = {0, 1, 1024, Integer.MAX_VALUE};

        for (int size : sizes) {
            StorageEntityHeader original = new StorageEntityHeader();
            original.setSize(size);

            byte[] serialized = StorageEntityHeader.serialize(original);
            StorageEntityHeader deserialized = StorageEntityHeader.deserialize(serialized);

            assertNotNull(deserialized);
            assertEquals(size, deserialized.getSize());
        }
    }
}
