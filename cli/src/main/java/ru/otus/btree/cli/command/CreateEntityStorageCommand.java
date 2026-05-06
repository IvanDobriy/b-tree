package ru.otus.btree.cli.command;

import ru.otus.btree.cases.CreateEntityStorageUseCase;
import ru.otus.btree.command.Interaction;
import ru.otus.btree.domain.IStorage;

import java.util.Objects;

public class CreateEntityStorageCommand implements ICommand {
    private final ICommand nextCommand;
    private final CreateEntityStorageUseCase useCase;

    public CreateEntityStorageCommand(ICommand nextCommand, IStorage storage) {
        this.nextCommand = Objects.requireNonNull(nextCommand, "nextCommand is null");
        useCase = new CreateEntityStorageUseCase(storage);
    }

    @Override
    public ICommand execute(Interaction interaction) {
        useCase.execute(interaction);
        return nextCommand;
    }
}
