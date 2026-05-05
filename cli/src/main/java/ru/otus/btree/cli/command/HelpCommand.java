package ru.otus.btree.cli.command;

import ru.otus.btree.cases.GetHelpUseCase;

public class HelpCommand implements ICommand {
    private final GetHelpUseCase getHelpUseCase;

    public HelpCommand() {
        getHelpUseCase = new GetHelpUseCase();
    }

    @Override
    public String execute() {
        return getHelpUseCase.execute();
    }
}
