package ru.otus.btree.cases;

import ru.otus.btree.command.Interaction;

public class GetHelpUseCase {
    public void execute(Interaction interaction) {
        interaction.write("B-Tree CLI — a file-persisted B-tree storage application.");
        interaction.write("Supported commands:");
        interaction.write("  help           — show this help message");
        interaction.write("  create-storage — create a new entity storage");
        interaction.write("  create-index   — create an index on a field");
        interaction.write("  set-entity     — save an entity to storage");
        interaction.write("  get-entity     — retrieve an entity from storage");
        interaction.write("  find-by-index  — search entities by indexed field");
        interaction.write("  exit           — close the application");
    }
}
