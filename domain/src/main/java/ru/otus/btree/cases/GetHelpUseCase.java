package ru.otus.btree.cases;

import ru.otus.btree.command.Interaction;

public class GetHelpUseCase {
    public void execute(Interaction interaction){
        interaction.write("this is a help");
    }
}
