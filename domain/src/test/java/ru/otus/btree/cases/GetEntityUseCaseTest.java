package ru.otus.btree.cases;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.ArgumentCaptor;
import ru.otus.btree.command.Interaction;
import ru.otus.btree.domain.IStorage;
import ru.otus.btree.lib.api.array.IArray;
import ru.otus.btree.lib.api.btree.EType;
import ru.otus.btree.lib.api.btree.Element;
import ru.otus.btree.lib.api.btree.IEntity;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class GetEntityUseCaseTest {

    @Test
    public void testSuccess() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn("users", "5");

        IEntity entity = mock(IEntity.class);
        IArray<Element> elements = mock(IArray.class);
        Element nameElement = new Element("name", EType.STRING, "John");
        Element ageElement = new Element("age", EType.INTEGER, 30);

        when(storage.getEntity("users", 5)).thenReturn(entity);
        when(entity.toArray()).thenReturn(elements);
        when(elements.size()).thenReturn(2);
        when(elements.get(0)).thenReturn(nameElement);
        when(elements.get(1)).thenReturn(ageElement);

        GetEntityUseCase useCase = new GetEntityUseCase(storage);
        useCase.execute(interaction);

        verify(storage).getEntity("users", 5);
        assertWritten(interaction, "Entity at position 5 from storage 'users':");
        assertWritten(interaction, "name (STRING): John");
        assertWritten(interaction, "age (INTEGER): 30");
    }

    @Test
    public void testSuccessNoFields() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn("users", "0");

        IEntity entity = mock(IEntity.class);
        IArray<Element> elements = mock(IArray.class);

        when(storage.getEntity("users", 0)).thenReturn(entity);
        when(entity.toArray()).thenReturn(elements);
        when(elements.size()).thenReturn(0);

        GetEntityUseCase useCase = new GetEntityUseCase(storage);
        useCase.execute(interaction);

        assertWritten(interaction, "Entity at position 0 has no fields.");
    }

    @Test
    public void testEntityNotFound() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn("users", "10");

        when(storage.getEntity("users", 10)).thenReturn(null);

        GetEntityUseCase useCase = new GetEntityUseCase(storage);
        useCase.execute(interaction);

        verify(storage).getEntity("users", 10);
        assertWritten(interaction, "Error: entity not found at position 10 in storage 'users'.");
    }

    @Test
    public void testCancelAtStorageName() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn("!stop");

        GetEntityUseCase useCase = new GetEntityUseCase(storage);
        useCase.execute(interaction);

        verify(storage, never()).getEntity(any(), anyInt());
        assertWritten(interaction, "Operation cancelled.");
    }

    @Test
    public void testRetryBlankStorageNameThenSuccess() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn("", "  ", "users", "1");

        IEntity entity = mock(IEntity.class);
        IArray<Element> elements = mock(IArray.class);
        when(storage.getEntity("users", 1)).thenReturn(entity);
        when(entity.toArray()).thenReturn(elements);
        when(elements.size()).thenReturn(0);

        GetEntityUseCase useCase = new GetEntityUseCase(storage);
        useCase.execute(interaction);

        verify(storage).getEntity("users", 1);
        assertWritten(interaction, "Error: name cannot be empty");
    }

    @Test
    public void testRetryNullStorageNameThenSuccess() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn(null, "users", "2");

        IEntity entity = mock(IEntity.class);
        IArray<Element> elements = mock(IArray.class);
        when(storage.getEntity("users", 2)).thenReturn(entity);
        when(entity.toArray()).thenReturn(elements);
        when(elements.size()).thenReturn(0);

        GetEntityUseCase useCase = new GetEntityUseCase(storage);
        useCase.execute(interaction);

        verify(storage).getEntity("users", 2);
        assertWritten(interaction, "Error: input is null");
    }

    @Test
    public void testCancelAtPosition() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn("users", "!stop");

        GetEntityUseCase useCase = new GetEntityUseCase(storage);
        useCase.execute(interaction);

        verify(storage, never()).getEntity(any(), anyInt());
        assertWritten(interaction, "Operation cancelled.");
    }

    @Test
    public void testRetryBlankPositionThenSuccess() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn("users", "", "  ", "3");

        IEntity entity = mock(IEntity.class);
        IArray<Element> elements = mock(IArray.class);
        when(storage.getEntity("users", 3)).thenReturn(entity);
        when(entity.toArray()).thenReturn(elements);
        when(elements.size()).thenReturn(0);

        GetEntityUseCase useCase = new GetEntityUseCase(storage);
        useCase.execute(interaction);

        verify(storage).getEntity("users", 3);
        assertWritten(interaction, "Error: position cannot be empty");
    }

    @Test
    public void testRetryInvalidPositionThenSuccess() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn("users", "abc", "4");

        IEntity entity = mock(IEntity.class);
        IArray<Element> elements = mock(IArray.class);
        when(storage.getEntity("users", 4)).thenReturn(entity);
        when(entity.toArray()).thenReturn(elements);
        when(elements.size()).thenReturn(0);

        GetEntityUseCase useCase = new GetEntityUseCase(storage);
        useCase.execute(interaction);

        verify(storage).getEntity("users", 4);
        assertWritten(interaction, "Error: invalid integer 'abc'");
    }

    private void assertWritten(Interaction interaction, String text) {
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(interaction, atLeastOnce()).write(captor.capture());
        List<String> messages = captor.getAllValues();
        assertTrue(messages.stream().anyMatch(m -> m.contains(text)),
                "Expected interaction to contain: " + text);
    }
}
