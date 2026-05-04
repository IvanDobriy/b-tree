package ru.otus.btree.lib.v1.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class StorageManagerTest {

    @TempDir
    Path tempDir;

    private Path tempFile;
    private FileChannel fileChannel;

    @BeforeEach
    public void setUp() throws IOException {
        tempFile = tempDir.resolve(UUID.randomUUID().toString() + ".dat");
        fileChannel = FileChannel.open(tempFile, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
    }

    @AfterEach
    public void tearDown() throws IOException {
        if (fileChannel != null && fileChannel.isOpen()) {
            fileChannel.close();
        }
    }

    @Test
    public void testConstructorWithNullThrowsException() {
        assertThrows(NullPointerException.class, () -> new StorageManager(null));
    }

    @Test
    public void testGetPageSize() {
        StorageManager manager = new StorageManager(fileChannel);
        assertEquals(4096, manager.getPageSize());
    }

    @Test
    public void testAllocatePositionFirstTime() {
        StorageManager manager = new StorageManager(fileChannel);
        long position = manager.allocatePosition(4096);
        assertEquals(0L, position);
    }

    @Test
    public void testAllocatePositionWithDifferentSize() {
        StorageManager manager = new StorageManager(fileChannel);
        long position = manager.allocatePosition(2048);
        assertEquals(0L, position);
    }

    @Test
    public void testAllocatePositionMultipleTimes() {
        StorageManager manager = new StorageManager(fileChannel);
        long position = manager.allocatePosition(1);
        assertEquals(0L, position);
        position = manager.allocatePosition(2);
        assertEquals(1L, position);
        position = manager.allocatePosition(3);
        assertEquals(3L, position);
    }

    @Test
    public void testReuseDeletedEntity() throws IOException {
        // Setup: create a deleted entity directly via StorageMangerList
        StorageMangerList list = new StorageMangerList(fileChannel);
        StorageManagerEntity entity = new StorageManagerEntity();
        entity.setId(0L);
        entity.setSize(4096);
        entity.setUsed(false);
        list.setEntity(entity);

        // Recreate StorageManager — it should detect the deleted entity
        StorageManager manager = new StorageManager(fileChannel);
        long position = manager.allocatePosition(2048);

        // Position should be reused from deleted entity (id = 0)
        assertEquals(0L, position);

        // Verify entity was reused with new size
        StorageManagerEntity reused = list.getEntity(0);
        assertNotNull(reused);
        assertTrue(reused.isUsed());
        assertEquals(2048, reused.getSize());
    }

    @Test
    public void testReuseDeletedEntityReturnsIdAsPosition() throws IOException {
        // Setup: create a deleted entity with id=1
        StorageMangerList list = new StorageMangerList(fileChannel);
        StorageManagerEntity entity = new StorageManagerEntity();
        entity.setId(1L);
        entity.setSize(4096);
        entity.setUsed(false);
        list.setEntity(entity);

        // Recreate StorageManager
        StorageManager manager = new StorageManager(fileChannel);
        long position = manager.allocatePosition(512);

        // Position equals the reused id
        assertEquals(1L, position);

        StorageManagerEntity reused = list.getEntity(1);
        assertNotNull(reused);
        assertTrue(reused.isUsed());
        assertEquals(512, reused.getSize());
    }
}
