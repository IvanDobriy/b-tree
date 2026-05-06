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
import ru.otus.btree.lib.api.storage.Result;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FindByIndexUseCaseTest {

    @Test
    public void testSuccess() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn("users", "name", "string", "John");

        IEntity entity = mock(IEntity.class);
        IArray<Element> elements = mock(IArray.class);
        when(elements.size()).thenReturn(1);
        when(elements.get(0)).thenReturn(new Element("name", EType.STRING, "John"));
        when(entity.toArray()).thenReturn(elements);

        Result result = new Result(entity, 5);
        IArray<Result> results = mock(IArray.class);
        when(results.size()).thenReturn(1);
        when(results.get(0)).thenReturn(result);

        when(storage.findByIndex(eq("users"), any(Element.class))).thenReturn(results);

        FindByIndexUseCase useCase = new FindByIndexUseCase(storage);
        useCase.execute(interaction);

        assertWritten(interaction, "Found 1 result(s):");
        assertWritten(interaction, "name (STRING): John");
    }

    @Test
    public void testNoResults() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn("users", "name", "string", "John");
        when(storage.findByIndex(any(), any())).thenReturn(null);

        FindByIndexUseCase useCase = new FindByIndexUseCase(storage);
        useCase.execute(interaction);

        assertWritten(interaction, "No results found.");
    }

    @Test
    public void testEmptyResults() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn("users", "name", "string", "John");

        IArray<Result> results = mock(IArray.class);
        when(results.size()).thenReturn(0);
        when(storage.findByIndex(any(), any())).thenReturn(results);

        FindByIndexUseCase useCase = new FindByIndexUseCase(storage);
        useCase.execute(interaction);

        assertWritten(interaction, "No results found.");
    }

    @Test
    public void testCancelAtEntityName() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn("!stop");

        FindByIndexUseCase useCase = new FindByIndexUseCase(storage);
        useCase.execute(interaction);

        verify(storage, never()).findByIndex(any(), any());
        assertWritten(interaction, "Operation cancelled.");
    }

    @Test
    public void testCancelAtFieldName() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn("users", "!stop");

        FindByIndexUseCase useCase = new FindByIndexUseCase(storage);
        useCase.execute(interaction);

        verify(storage, never()).findByIndex(any(), any());
        assertWritten(interaction, "Operation cancelled.");
    }

    @Test
    public void testCancelAtFieldType() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn("users", "name", "!stop");

        FindByIndexUseCase useCase = new FindByIndexUseCase(storage);
        useCase.execute(interaction);

        verify(storage, never()).findByIndex(any(), any());
        assertWritten(interaction, "Operation cancelled.");
    }

    @Test
    public void testCancelAtFieldValue() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn("users", "name", "string", "!stop");

        FindByIndexUseCase useCase = new FindByIndexUseCase(storage);
        useCase.execute(interaction);

        verify(storage, never()).findByIndex(any(), any());
        assertWritten(interaction, "Operation cancelled.");
    }

    @Test
    public void testRetryBlankEntityNameThenSuccess() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn("", "  ", "users", "name", "string", "John");

        IArray<Result> results = mock(IArray.class);
        when(results.size()).thenReturn(0);
        when(storage.findByIndex(any(), any())).thenReturn(results);

        FindByIndexUseCase useCase = new FindByIndexUseCase(storage);
        useCase.execute(interaction);

        assertWritten(interaction, "Error: name cannot be empty");
    }

    @Test
    public void testRetryUnknownTypeThenSuccess() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn("users", "name", "foo", "string", "John");

        IArray<Result> results = mock(IArray.class);
        when(results.size()).thenReturn(0);
        when(storage.findByIndex(any(), any())).thenReturn(results);

        FindByIndexUseCase useCase = new FindByIndexUseCase(storage);
        useCase.execute(interaction);

        assertWritten(interaction, "Error: unknown type 'foo'");
    }

    @Test
    public void testRetryInvalidIntegerThenSuccess() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn("users", "age", "integer", "abc", "30");

        IArray<Result> results = mock(IArray.class);
        when(results.size()).thenReturn(0);
        when(storage.findByIndex(any(), any())).thenReturn(results);

        FindByIndexUseCase useCase = new FindByIndexUseCase(storage);
        useCase.execute(interaction);

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
