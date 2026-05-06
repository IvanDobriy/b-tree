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
        interaction.write("Enter entity storage name (or !stop to cancel):");
        while (true) {
            String name = interaction.read();
            if (name == null) {
                interaction.write("Error: input is null. Please try again or enter !stop.");
                continue;
            }
            name = name.trim();
            if ("!stop".equals(name)) {
                interaction.write("Operation cancelled.");
                return;
            }
            if (name.isBlank()) {
                interaction.write("Error: name cannot be empty. Please try again or enter !stop.");
                continue;
            }
            try {
                storage.createEntityStorage(name);
                interaction.write("Entity storage '" + name + "' created successfully.");
            } catch (Exception e) {
                interaction.write("Error: failed to create storage '" + name + "': " + e.getMessage());
            }
            return;
        }
    }
}
