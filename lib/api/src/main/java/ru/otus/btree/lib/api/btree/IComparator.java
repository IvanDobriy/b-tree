package ru.otus.btree.lib.api.btree;

public interface IComparator <T> {
    int compare(T el1, T el2);
}
