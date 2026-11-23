package com.lmax.disruptor;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LowLatencyProducerWaitStrategyTest
{
    @Test
    public void shouldReturnImmediatelyWhenCapacityIsAvailable()
    {
        final Sequence gatingSequence = new Sequence(10L);
        final LowLatencyProducerWaitStrategy strategy = new LowLatencyProducerWaitStrategy();

        final long result = strategy.waitForCapacity(
            10L, 10L, new Sequence[]{gatingSequence});

        assertThat(result, is(10L));
    }

    @Test
    public void shouldWaitUntilGatingSequenceAdvances() throws Exception
    {
        final Sequence gatingSequence = new Sequence(0L);
        final LowLatencyProducerWaitStrategy strategy = new LowLatencyProducerWaitStrategy();
        final ExecutorService executor = Executors.newSingleThreadExecutor();

        try
        {
            final Future<Long> waiting = executor.submit(
                () -> strategy.waitForCapacity(5L, 10L, new Sequence[]{gatingSequence}));

            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1L));
            assertFalse(waiting.isDone());

            gatingSequence.set(5L);

            assertThat(waiting.get(1L, TimeUnit.SECONDS), is(5L));
        }
        finally
        {
            executor.shutdownNow();
        }
    }

    @Test
    public void shouldRejectNegativeSettings()
    {
        assertThrows(
            IllegalArgumentException.class,
            () -> new LowLatencyProducerWaitStrategy(-1, 0, 1L));
        assertThrows(
            IllegalArgumentException.class,
            () -> new LowLatencyProducerWaitStrategy(0, -1, 1L));
        assertThrows(
            IllegalArgumentException.class,
            () -> new LowLatencyProducerWaitStrategy(0, 0, -1L));
    }
}
