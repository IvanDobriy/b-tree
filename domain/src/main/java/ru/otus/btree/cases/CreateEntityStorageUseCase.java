package ru.otus.btree.cases;

import ru.otus.btree.command.Interaction;
import ru.otus.btree.domain.IStorage;

import java.util.Objects;

public class CreateEntityStorageUseCase {
    private final IStorage storage;
    public CreateEntityStorageUseCase(IStorage storage){
        this.storage = Objects.requireNonNull(storage, "storage is null");
    }
    void execute(Interaction interaction){

    }
}
