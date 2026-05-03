package ru.otus.btree.lib.v1.btree;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import ru.otus.btree.lib.api.btree.EType;
import ru.otus.btree.lib.api.btree.Element;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FileBTreeTest {

    @TempDir
    Path tempDir;

    @Test
    public void testInsertAndSearch() throws Exception {
        File pageTempFile = tempDir.resolve("page-manager-btree-test.tmp").toFile();
        File nodeTempFile = tempDir.resolve("node-btree-test.tmp").toFile();
        try (RandomAccessFile pageRaf = new RandomAccessFile(pageTempFile, "rw");
             FileChannel pageChannel = pageRaf.getChannel();
             RandomAccessFile nodeRaf = new RandomAccessFile(nodeTempFile, "rw");
             FileChannel nodeChannel = nodeRaf.getChannel()) {

            FileBTree bTree = new FileBTree(pageChannel, nodeChannel, 5);

            Element key = new Element("key", EType.STRING, "value");
            bTree.insert("key", new ru.otus.btree.lib.v1.btree.Entity() {{
                set(key);
            }});

            assertNotNull(bTree.search(key));
        }
    }

    @Test
    public void testDelete() throws Exception {
        File pageTempFile = tempDir.resolve("page-manager-btree-delete-test.tmp").toFile();
        File nodeTempFile = tempDir.resolve("node-btree-delete-test.tmp").toFile();
        try (RandomAccessFile pageRaf = new RandomAccessFile(pageTempFile, "rw");
             FileChannel pageChannel = pageRaf.getChannel();
             RandomAccessFile nodeRaf = new RandomAccessFile(nodeTempFile, "rw");
             FileChannel nodeChannel = nodeRaf.getChannel()) {

            FileBTree bTree = new FileBTree(pageChannel, nodeChannel, 5);

            Element keyA = new Element("key", EType.STRING, "A");
            Element keyB = new Element("key", EType.STRING, "B");
            Element keyC = new Element("key", EType.STRING, "C");

            bTree.insert("key", new ru.otus.btree.lib.v1.btree.Entity() {{
                set(keyA);
            }});
            bTree.insert("key", new ru.otus.btree.lib.v1.btree.Entity() {{
                set(keyB);
            }});
            bTree.insert("key", new ru.otus.btree.lib.v1.btree.Entity() {{
                set(keyC);
            }});

            assertNotNull(bTree.search(keyB));
            bTree.delete(keyB);
            assertNull(bTree.search(keyB), "Key B should be deleted");
            assertNotNull(bTree.search(keyA), "Key A should still exist");
            assertNotNull(bTree.search(keyC), "Key C should still exist");
        }
    }

    @Test
    public void testInsert1000Delete200() throws Exception {
        File pageTempFile = tempDir.resolve("page-manager-btree-many-test.tmp").toFile();
        File nodeTempFile = tempDir.resolve("node-btree-many-test.tmp").toFile();
        try (RandomAccessFile pageRaf = new RandomAccessFile(pageTempFile, "rw");
             FileChannel pageChannel = pageRaf.getChannel();
             RandomAccessFile nodeRaf = new RandomAccessFile(nodeTempFile, "rw");
             FileChannel nodeChannel = nodeRaf.getChannel()) {

            FileBTree bTree = new FileBTree(pageChannel, nodeChannel, 5);

            // Insert 1000 elements
            for (int i = 0; i < 1000; i++) {
                final int value = i;
                bTree.insert("key", new ru.otus.btree.lib.v1.btree.Entity() {{
                    set(new Element("key", EType.INTEGER, value));
                }});
            }

            // Delete 200 elements (even numbers from 0 to 398)
            for (int i = 0; i < 200; i++) {
                bTree.delete(new Element("key", EType.INTEGER, i * 2));
            }

            // Verify deleted elements are gone
            for (int i = 0; i < 200; i++) {
                assertNull(bTree.search(new Element("key", EType.INTEGER, i * 2)),
                        "Deleted key " + (i * 2) + " should not be found");
            }

            // Verify remaining elements exist
            for (int i = 0; i < 1000; i++) {
                if (i % 2 != 0 || i >= 400) {
                    assertNotNull(bTree.search(new Element("key", EType.INTEGER, i)),
                            "Key " + i + " should still exist");
                }
            }
        }
    }
}
