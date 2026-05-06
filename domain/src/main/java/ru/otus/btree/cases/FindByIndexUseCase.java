package ru.otus.btree.cases;

import ru.otus.btree.command.Interaction;
import ru.otus.btree.domain.IStorage;
import ru.otus.btree.lib.api.array.IArray;
import ru.otus.btree.lib.api.btree.EType;
import ru.otus.btree.lib.api.btree.Element;
import ru.otus.btree.lib.api.btree.IEntity;
import ru.otus.btree.lib.api.storage.Result;

import java.util.Objects;

public class FindByIndexUseCase {
    private final IStorage storage;

    public FindByIndexUseCase(IStorage storage) {
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

        EType type = readFieldType(interaction);
        if (type == null) {
            return;
        }

        Object value = readFieldValue(interaction, type);
        if (value == null) {
            return;
        }

        Element element = new Element(fieldName, type, value);
        IArray<Result> results;
        try {
            results = storage.findByIndex(entityName, element);
        } catch (Exception e) {
            interaction.write("Error: failed to search by index in storage '" + entityName + "': " + e.getMessage());
            return;
        }

        if (results == null || results.size() == 0) {
            interaction.write("No results found.");
            return;
        }

        interaction.write("Found " + results.size() + " result(s):");
        for (int i = 0; i < results.size(); i++) {
            Result result = results.get(i);
            interaction.write("Result " + (i + 1) + " at position " + result.getPosition() + ":");
            IEntity entity = result.getData();
            if (entity == null) {
                interaction.write("  (no data)");
                continue;
            }
            IArray<Element> elements = entity.toArray();
            if (elements == null || elements.size() == 0) {
                interaction.write("  (empty entity)");
                continue;
            }
            for (int j = 0; j < elements.size(); j++) {
                Element e = elements.get(j);
                interaction.write("  " + e.getName() + " (" + e.getType() + "): " + e.getValue());
            }
        }
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
        interaction.write("Enter field name for search (or !stop to cancel):");
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
