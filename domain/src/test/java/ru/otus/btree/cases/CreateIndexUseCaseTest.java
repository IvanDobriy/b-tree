package ru.otus.btree.cases;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.ArgumentCaptor;
import ru.otus.btree.command.Interaction;
import ru.otus.btree.domain.IStorage;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CreateIndexUseCaseTest {

    @Test
    public void testCreateIndexSuccess() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn("users", "name");

        CreateIndexUseCase useCase = new CreateIndexUseCase(storage);
        useCase.execute(interaction);

        verify(storage).createIndex("users", "name");
        assertWritten(interaction, "Index on field 'name' created successfully for storage 'users'.");
    }

    @Test
    public void testCancelAtEntityName() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn("!stop");

        CreateIndexUseCase useCase = new CreateIndexUseCase(storage);
        useCase.execute(interaction);

        verify(storage, never()).createIndex(any(), any());
        assertWritten(interaction, "Operation cancelled.");
    }

    @Test
    public void testCancelAtFieldName() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn("users", "!stop");

        CreateIndexUseCase useCase = new CreateIndexUseCase(storage);
        useCase.execute(interaction);

        verify(storage, never()).createIndex(any(), any());
        assertWritten(interaction, "Operation cancelled.");
    }

    @Test
    public void testRetryBlankEntityNameThenSuccess() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn("", "  ", "users", "name");

        CreateIndexUseCase useCase = new CreateIndexUseCase(storage);
        useCase.execute(interaction);

        verify(storage).createIndex("users", "name");
        assertWritten(interaction, "Error: name cannot be empty");
    }

    @Test
    public void testRetryNullEntityNameThenSuccess() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn(null, "users", "name");

        CreateIndexUseCase useCase = new CreateIndexUseCase(storage);
        useCase.execute(interaction);

        verify(storage).createIndex("users", "name");
        assertWritten(interaction, "Error: input is null");
    }

    @Test
    public void testRetryBlankFieldNameThenSuccess() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn("users", "", "  ", "name");

        CreateIndexUseCase useCase = new CreateIndexUseCase(storage);
        useCase.execute(interaction);

        verify(storage).createIndex("users", "name");
        assertWritten(interaction, "Error: field name cannot be empty");
    }

    @Test
    public void testRetryNullFieldNameThenSuccess() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn("users", null, "name");

        CreateIndexUseCase useCase = new CreateIndexUseCase(storage);
        useCase.execute(interaction);

        verify(storage).createIndex("users", "name");
        assertWritten(interaction, "Error: input is null");
    }

    private void assertWritten(Interaction interaction, String text) {
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(interaction, atLeastOnce()).write(captor.capture());
        List<String> messages = captor.getAllValues();
        assertTrue(messages.stream().anyMatch(m -> m.contains(text)),
                "Expected interaction to contain: " + text);
    }
}
