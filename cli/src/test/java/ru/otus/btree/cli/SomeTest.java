package ru.otus.btree.cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.logging.Logger;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SomeTest {
    private Logger logger = Logger.getLogger(this.getClass().getName());
    @Test
    void positiveTest() {
        logger.info("hello, world");
    }
}
