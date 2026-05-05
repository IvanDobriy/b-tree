package ru.otus.btree.cli;

import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import java.util.logging.Logger;

public class BTreeCLI {
    private Logger logger = Logger.getLogger(this.getClass().getName());
    private void run(String[] args){
        try {


            // Create a terminal
            Terminal terminal = TerminalBuilder.builder()
                    .color(true)
                    .system(true)
                    .build();
            // Create line reader
            LineReader reader = LineReaderBuilder.builder()
                    .terminal(terminal)
                    .build();

            while (true){
                reader.zeroOut();
                String line = reader.readLine("JLine > ");
                System.out.println("You entered: " + line);
            }
            // Prompt and read input

            // Print the result
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        BTreeCLI app = new BTreeCLI();
        app.run(args);
    }
}
