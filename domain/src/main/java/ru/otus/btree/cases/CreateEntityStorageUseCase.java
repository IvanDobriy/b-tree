package ru.otus.btree.cases;

import ru.otus.btree.command.Interaction;
import ru.otus.btree.domain.IStorage;

import java.util.Objects;

public class CreateEntityStorageUseCase {
    private final IStorage storage;
    public CreateEntityStorageUseCase(IStorage storage){
        this.storage = Objects.requireNonNull(storage, "storage is null");
    }
    public void execute(Interaction interaction) {
        interaction.write("Enter entity storage name:");
        String name = interaction.read();
        if (name == null || name.isBlank()) {
            interaction.write("Error: name cannot be empty");
            return;
        }
        storage.createEntityStorage(name.trim());
        interaction.write("Entity storage '" + name.trim() + "' created successfully.");
    }
}
