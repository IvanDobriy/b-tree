package ru.otus.btree.lib.v1.btree;

import ru.otus.btree.lib.api.array.IArray;
import ru.otus.btree.lib.v1.array.SingleArray;

import java.nio.channels.FileChannel;
import java.util.Objects;

/**
 * Manages a list of page entities using SingleArray.
 * Provides operations to add, remove, find and manage pages.
 */
public class PageManagerList {
    private final IArray<PageManagerEntity> pages;
    private final FileChannel fileChannel;

    public PageManagerList(FileChannel fileChannel) {
        this.fileChannel = Objects.requireNonNull(fileChannel, "fileChannel must not be null");
        this.pages = new SingleArray<>(0);
    }
}
