package ru.otus.btree.cli.command;

import ru.otus.btree.cases.GetHelpUseCase;
import ru.otus.btree.command.Interaction;

public class HelpCommand implements ICommand {
    private final GetHelpUseCase getHelpUseCase;

    public HelpCommand() {
        getHelpUseCase = new GetHelpUseCase();
    }

    @Override
    public String execute(Interaction interaction) {
        return getHelpUseCase.execute();
    }
}
