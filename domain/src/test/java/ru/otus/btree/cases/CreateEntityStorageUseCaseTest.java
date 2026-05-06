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
public class CreateEntityStorageUseCaseTest {

    @Test
    public void testCreateStorageSuccess() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn("users");

        CreateEntityStorageUseCase useCase = new CreateEntityStorageUseCase(storage);
        useCase.execute(interaction);

        verify(storage).createEntityStorage("users");
        assertWritten(interaction, "Entity storage 'users' created successfully.");
    }

    @Test
    public void testCancelWithStop() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn("!stop");

        CreateEntityStorageUseCase useCase = new CreateEntityStorageUseCase(storage);
        useCase.execute(interaction);

        verify(storage, never()).createEntityStorage(any());
        assertWritten(interaction, "Operation cancelled.");
    }

    @Test
    public void testRetryOnBlankInput() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn("", "  ", "valid");

        CreateEntityStorageUseCase useCase = new CreateEntityStorageUseCase(storage);
        useCase.execute(interaction);

        verify(storage).createEntityStorage("valid");
        assertWritten(interaction, "Error: name cannot be empty");
    }

    @Test
    public void testRetryOnNullInput() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn(null, "backup");

        CreateEntityStorageUseCase useCase = new CreateEntityStorageUseCase(storage);
        useCase.execute(interaction);

        verify(storage).createEntityStorage("backup");
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
