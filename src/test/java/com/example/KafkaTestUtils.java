package com.example;

import com.example.model.OrderWindow;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaTestUtils {

    /**
     * Creates a mock KeyValueIterator for testing
     */
    public static Answer<KeyValueIterator<String, OrderWindow>> createMockIterator(List<KeyValue<String, OrderWindow>> keyValues) {
        return new Answer<KeyValueIterator<String, OrderWindow>>() {
            @Override
            public KeyValueIterator<String, OrderWindow> answer(InvocationOnMock invocation) throws Throwable {
                return new TestKeyValueIterator(keyValues);
            }
        };
    }

    /**
     * Test implementation of KeyValueIterator
     */
    private static class TestKeyValueIterator implements KeyValueIterator<String, OrderWindow> {
        private final Iterator<KeyValue<String, OrderWindow>> iterator;
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private KeyValue<String, OrderWindow> peekedValue = null;

        public TestKeyValueIterator(List<KeyValue<String, OrderWindow>> keyValues) {
            this.iterator = keyValues.iterator();
        }

        @Override
        public boolean hasNext() {
            return peekedValue != null || iterator.hasNext();
        }

        @Override
        public KeyValue<String, OrderWindow> next() {
            if (peekedValue != null) {
                KeyValue<String, OrderWindow> result = peekedValue;
                peekedValue = null;
                return result;
            }
            return iterator.next();
        }

        @Override
        public String peekNextKey() {
            if (peekedValue == null && iterator.hasNext()) {
                peekedValue = iterator.next();
            }
            return peekedValue != null ? peekedValue.key : null;
        }

        @Override
        public void close() {
            closed.set(true);
        }

        public boolean isClosed() {
            return closed.get();
        }
    }
}