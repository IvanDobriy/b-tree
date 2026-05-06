package ru.otus.btree.cases;

import ru.otus.btree.command.Interaction;
import ru.otus.btree.domain.IStorage;

import java.util.Objects;

public class CreateIndexUseCase {
    private final IStorage storage;

    public CreateIndexUseCase(IStorage storage) {
        this.storage = Objects.requireNonNull(storage, "storage is null");
    }

    public void execute(Interaction interaction) {
        String entityName = readEntityName(interaction);
        if (entityName == null) {
            return;
        }

        String fieldName = readFieldName(interaction);
        if (fieldName == null) {
            return;
        }

        storage.createIndex(entityName, fieldName);
        interaction.write("Index on field '" + fieldName + "' created successfully for storage '" + entityName + "'.");
    }

    private String readEntityName(Interaction interaction) {
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
                return null;
            }
            if (name.isBlank()) {
                interaction.write("Error: name cannot be empty. Please try again or enter !stop.");
                continue;
            }
            return name;
        }
    }

    private String readFieldName(Interaction interaction) {
        interaction.write("Enter field name for index (or !stop to cancel):");
        while (true) {
            String name = interaction.read();
            if (name == null) {
                interaction.write("Error: input is null. Please try again or enter !stop.");
                continue;
            }
            name = name.trim();
            if ("!stop".equals(name)) {
                interaction.write("Operation cancelled.");
                return null;
            }
            if (name.isBlank()) {
                interaction.write("Error: field name cannot be empty. Please try again or enter !stop.");
                continue;
            }
            return name;
        }
    }
}
