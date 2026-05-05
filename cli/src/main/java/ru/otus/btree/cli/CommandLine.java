package ru.otus.btree.cli;

import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import java.util.logging.Logger;

public class CommandLine {
    private final Logger logger = Logger.getLogger(this.getClass().getName());
    private Terminal terminal;
    private LineReader reader;

    public CommandLine() {
        try {
            this.terminal = TerminalBuilder.builder()
                    .color(true)
                    .system(true)
                    .build();
            this.reader = LineReaderBuilder.builder()
                    .terminal(terminal)
                    .build();

        } catch (Exception e) {
            logger.warning(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    String readLine(){
        return reader.readLine();
    }
    void writeMsg(String msg){
        System.out.println(msg);
    }
}
