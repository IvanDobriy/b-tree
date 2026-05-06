package ru.otus.btree.cases;

import ru.otus.btree.command.Interaction;

public class GreetingUseCase {
    public void execute(Interaction interaction){
        interaction.write("this is a greeting");
    }
}
