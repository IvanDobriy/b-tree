package ru.otus.btree.cases;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.ArgumentCaptor;
import ru.otus.btree.command.Interaction;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class GetHelpUseCaseTest {

    @Test
    public void testHelpOutput() {
        Interaction interaction = mock(Interaction.class);

        GetHelpUseCase useCase = new GetHelpUseCase();
        useCase.execute(interaction);

        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(interaction, atLeast(8)).write(captor.capture());
        List<String> messages = captor.getAllValues();

        assertTrue(messages.stream().anyMatch(m -> m.contains("B-Tree CLI")),
                "Expected help to contain application name");
        assertTrue(messages.stream().anyMatch(m -> m.contains("help")),
                "Expected help to contain 'help' command");
        assertTrue(messages.stream().anyMatch(m -> m.contains("create-storage")),
                "Expected help to contain 'create-storage' command");
        assertTrue(messages.stream().anyMatch(m -> m.contains("create-index")),
                "Expected help to contain 'create-index' command");
        assertTrue(messages.stream().anyMatch(m -> m.contains("set-entity")),
                "Expected help to contain 'set-entity' command");
        assertTrue(messages.stream().anyMatch(m -> m.contains("get-entity")),
                "Expected help to contain 'get-entity' command");
        assertTrue(messages.stream().anyMatch(m -> m.contains("find-by-index")),
                "Expected help to contain 'find-by-index' command");
        assertTrue(messages.stream().anyMatch(m -> m.contains("exit")),
                "Expected help to contain 'exit' command");
    }
}
