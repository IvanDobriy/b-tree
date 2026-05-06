package ru.otus.btree.cases;

import ru.otus.btree.command.Interaction;
import ru.otus.btree.domain.IStorage;
import ru.otus.btree.lib.api.btree.EType;
import ru.otus.btree.lib.api.btree.Element;
import ru.otus.btree.lib.v1.btree.Entity;

import java.util.Objects;

public class SetEntityUseCase {
    private final IStorage storage;

    public SetEntityUseCase(IStorage storage) {
        this.storage = Objects.requireNonNull(storage, "storage is null");
    }

    public void execute(Interaction interaction) {
        String storageName = readStorageName(interaction);
        if (storageName == null) {
            return;
        }

        Entity entity = new Entity();
        boolean hasFields = false;

        interaction.write("Enter field name (or !done to finish):");
        while (true) {
            String fieldName = interaction.read();
            if (fieldName == null) {
                interaction.write("Error: input is null. Please try again.");
                continue;
            }
            fieldName = fieldName.trim();
            if ("!done".equals(fieldName)) {
                break;
            }
            if (fieldName.isBlank()) {
                interaction.write("Error: field name cannot be empty. Please try again.");
                continue;
            }

            EType type = readFieldType(interaction);
            if (type == null) {
                return;
            }

            Object value = readFieldValue(interaction, type);
            if (value == null) {
                return;
            }

            entity.set(new Element(fieldName, type, value));
            hasFields = true;
            break;
        }

        if (!hasFields) {
            interaction.write("Error: entity must have at least one field. Operation cancelled.");
            return;
        }

        storage.setEntity(storageName, entity);
        interaction.write("Entity saved successfully to storage '" + storageName + "'.");
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

    private EType readFieldType(Interaction interaction) {
        interaction.write("Enter field type (string/integer) (or !stop to cancel):");
        while (true) {
            String typeStr = interaction.read();
            if (typeStr == null) {
                interaction.write("Error: input is null. Please try again or enter !stop.");
                continue;
            }
            typeStr = typeStr.trim();
            if ("!stop".equals(typeStr)) {
                interaction.write("Operation cancelled.");
                return null;
            }
            if ("string".equalsIgnoreCase(typeStr)) {
                return EType.STRING;
            }
            if ("integer".equalsIgnoreCase(typeStr)) {
                return EType.INTEGER;
            }
            interaction.write("Error: unknown type '" + typeStr + "'. Please enter 'string' or 'integer' or !stop.");
        }
    }

    private Object readFieldValue(Interaction interaction, EType type) {
        interaction.write("Enter field value (or !stop to cancel):");
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
            if (type == EType.STRING) {
                return valueStr;
            }
            if (type == EType.INTEGER) {
                try {
                    return Integer.parseInt(valueStr);
                } catch (NumberFormatException e) {
                    interaction.write("Error: invalid integer '" + valueStr + "'. Please try again or enter !stop.");
                    continue;
                }
            }
            return valueStr;
        }
    }
}
