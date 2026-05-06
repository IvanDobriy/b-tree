package ru.otus.btree.cases;

import ru.otus.btree.command.Interaction;
import ru.otus.btree.domain.IStorage;
import ru.otus.btree.lib.api.btree.EType;
import ru.otus.btree.lib.api.btree.Element;
import ru.otus.btree.lib.v1.btree.Entity;

import java.util.Objects;
import java.util.Random;

public class GenerateUserDataUseCase {
    private final IStorage storage;
    private final Random random;

    private static final String[] NAMES = {
            "Ivan", "Maria", "Alexey", "Olga", "Dmitry", "Anna", "Sergey", "Elena",
            "Pavel", "Natalia", "Andrey", "Tatiana", "Mikhail", "Irina", "Viktor", "Svetlana"
    };

    private static final String[] SURNAMES = {
            "Ivanov", "Petrov", "Sidorov", "Smirnov", "Kuznetsov", "Popov", "Vasiliev", "Sokolov",
            "Mikhailov", "Novikov", "Fedorov", "Morozov", "Volkov", "Alekseev", "Lebedev", "Semenov"
    };

    private static final String[] PROFESSIONS = {
            "Engineer", "Doctor", "Teacher", "Driver", "Programmer", "Chef", "Artist", "Manager",
            "Lawyer", "Accountant", "Designer", "Scientist", "Musician", "Writer", "Architect", "Nurse"
    };

    public GenerateUserDataUseCase(IStorage storage) {
        this.storage = Objects.requireNonNull(storage, "storage is null");
        this.random = new Random();
    }

    public void execute(Interaction interaction) {
        String storageName = readStorageName(interaction);
        if (storageName == null) {
            return;
        }

        Integer count = readUserCount(interaction);
        if (count == null) {
            return;
        }

        for (int i = 0; i < count; i++) {
            Entity entity = new Entity();
            entity.set(new Element("id", EType.INTEGER, i + 1));
            entity.set(new Element("name", EType.STRING, randomName()));
            entity.set(new Element("sName", EType.STRING, randomSurname()));
            entity.set(new Element("age", EType.INTEGER, randomAge()));
            entity.set(new Element("profession", EType.STRING, randomProfession()));
            storage.setEntity(storageName, entity);
        }

        interaction.write("Generated and saved " + count + " user(s) to storage '" + storageName + "'.");
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

    private Integer readUserCount(Interaction interaction) {
        interaction.write("Enter number of users to generate (positive integer) (or !stop to cancel):");
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
                interaction.write("Error: count cannot be empty. Please try again or enter !stop.");
                continue;
            }
            try {
                int count = Integer.parseInt(valueStr);
                if (count <= 0) {
                    interaction.write("Error: count must be positive. Please try again or enter !stop.");
                    continue;
                }
                return count;
            } catch (NumberFormatException e) {
                interaction.write("Error: invalid integer '" + valueStr + "'. Please try again or enter !stop.");
            }
        }
    }

    private String randomName() {
        return NAMES[random.nextInt(NAMES.length)];
    }

    private String randomSurname() {
        return SURNAMES[random.nextInt(SURNAMES.length)];
    }

    private int randomAge() {
        return 18 + random.nextInt(48);
    }

    private String randomProfession() {
        return PROFESSIONS[random.nextInt(PROFESSIONS.length)];
    }

}
