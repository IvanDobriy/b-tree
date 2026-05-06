package ru.otus.btree.cases;

import ru.otus.btree.command.Interaction;
import ru.otus.btree.domain.IStorage;
import ru.otus.btree.lib.api.array.IArray;
import ru.otus.btree.lib.api.btree.Element;
import ru.otus.btree.lib.api.btree.IEntity;

import java.util.Objects;

public class GetEntityUseCase {
    private final IStorage storage;

    public GetEntityUseCase(IStorage storage) {
        this.storage = Objects.requireNonNull(storage, "storage is null");
    }

    public void execute(Interaction interaction) {
        String storageName = readStorageName(interaction);
        if (storageName == null) {
            return;
        }

        Integer position = readPosition(interaction);
        if (position == null) {
            return;
        }

        IEntity entity = storage.getEntity(storageName, position);
        if (entity == null) {
            interaction.write("Error: entity not found at position " + position + " in storage '" + storageName + "'.");
            return;
        }

        IArray<Element> elements = entity.toArray();
        if (elements == null || elements.size() == 0) {
            interaction.write("Entity at position " + position + " has no fields.");
            return;
        }

        interaction.write("Entity at position " + position + " from storage '" + storageName + "':");
        for (int i = 0; i < elements.size(); i++) {
            Element element = elements.get(i);
            interaction.write("  " + element.getName() + " (" + element.getType() + "): " + element.getValue());
        }
    }

    private String readStorageName(Interaction interaction) {
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

    private Integer readPosition(Interaction interaction) {
        interaction.write("Enter entity position (integer) (or !stop to cancel):");
        while (true) {
            String valueStr = interaction.read();
            if (valueStr == null) {
                interaction.write("Error: input is null. Please try again or enter !stop.");
                continue;
            }
            valueStr = valueStr.trim();
            if ("!stop".equals(valueStr)) {
                interaction.write("Operation cancelled.");
                return null;
            }
            if (valueStr.isBlank()) {
                interaction.write("Error: position cannot be empty. Please try again or enter !stop.");
                continue;
            }
            try {
                return Integer.parseInt(valueStr);
            } catch (NumberFormatException e) {
                interaction.write("Error: invalid integer '" + valueStr + "'. Please try again or enter !stop.");
            }
        }
    }
}
