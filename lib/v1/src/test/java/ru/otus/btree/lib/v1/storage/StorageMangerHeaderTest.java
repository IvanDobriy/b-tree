package ru.otus.btree.lib.v1.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class StorageMangerHeaderTest {

    @Test
    public void testSerializeDeserialize() {
        StorageMangerHeader original = new StorageMangerHeader();
        original.setSize(1024L);
        original.setFileSize(2048L);

        byte[] serialized = StorageMangerHeader.serialize(original);
        StorageMangerHeader deserialized = StorageMangerHeader.deserialize(serialized);

        assertNotNull(deserialized);
        assertEquals(1024L, deserialized.getSize());
        assertEquals(2048L, deserialized.getFileSize());
    }

    @Test
    public void testSerializeNullHeader() {
        byte[] serialized = StorageMangerHeader.serialize(null);
        assertEquals(0, serialized.length);
    }

    @Test
    public void testDeserializeNullData() {
        StorageMangerHeader result = StorageMangerHeader.deserialize(null);
        assertNull(result);
    }

    @Test
    public void testDeserializeEmptyData() {
        StorageMangerHeader result = StorageMangerHeader.deserialize(new byte[0]);
        assertNull(result);
    }

    @Test
    public void testFileSizeRoundTrip() {
        StorageMangerHeader original = new StorageMangerHeader();
        original.setSize(100L);
        original.setFileSize(500L);

        byte[] serialized = StorageMangerHeader.serialize(original);
        StorageMangerHeader deserialized = StorageMangerHeader.deserialize(serialized);

        assertNotNull(deserialized);
        assertEquals(100L, deserialized.getSize());
        assertEquals(500L, deserialized.getFileSize());
    }

    @Test
    public void testRoundTripMultipleSizes() {
        long[] sizes = {0L, 1L, 1024L, Long.MAX_VALUE};

        for (long size : sizes) {
            StorageMangerHeader original = new StorageMangerHeader();
            original.setSize(size);
            original.setFileSize(size * 2);

            byte[] serialized = StorageMangerHeader.serialize(original);
            StorageMangerHeader deserialized = StorageMangerHeader.deserialize(serialized);

            assertNotNull(deserialized);
            assertEquals(size, deserialized.getSize());
            assertEquals(size * 2, deserialized.getFileSize());
        }
    }

    @Test
    public void testConstructorWithParameter() {
        StorageMangerHeader header = new StorageMangerHeader(2048L);
        assertEquals(2048L, header.getSize());
        assertEquals(0L, header.getFileSize());
    }
}
