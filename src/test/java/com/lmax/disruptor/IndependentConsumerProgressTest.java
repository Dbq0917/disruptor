package com.lmax.disruptor;

import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.LockSupport;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IndependentConsumerProgressTest
{
    @Test
    @Timeout(10)
    public void shouldDeliverEachEventToOnlyOneWorker() throws Exception
    {
        final int eventCount = 1_000;
        final AtomicIntegerArray deliveries = new AtomicIntegerArray(eventCount);
        final CountDownLatch complete = new CountDownLatch(eventCount);
        final WorkHandler<AtomicInteger> first = event -> deliver(event, deliveries, complete);
        final WorkHandler<AtomicInteger> second = event -> deliver(event, deliveries, complete);

        final WorkerPool<AtomicInteger> pool = new WorkerPool<>(
            AtomicInteger::new, new FatalExceptionHandler(), first, second);
        final ExecutorService executor = Executors.newFixedThreadPool(2, DaemonThreadFactory.INSTANCE);

        try
        {
            final RingBuffer<AtomicInteger> ringBuffer = pool.start(executor);
            publish(ringBuffer, eventCount);

            assertTrue(complete.await(5, TimeUnit.SECONDS));
            for (int i = 0; i < eventCount; i++)
            {
                assertThat(deliveries.get(i), is(1));
            }
        }
        finally
        {
            pool.halt();
            executor.shutdownNow();
        }
    }

    @Test
    @Timeout(10)
    public void shouldPreserveOrderWithOneWorker() throws Exception
    {
        final int eventCount = 1_000;
        final List<Integer> processed = new ArrayList<>();
        final CountDownLatch complete = new CountDownLatch(eventCount);
        final WorkHandler<AtomicInteger> handler = event ->
        {
            synchronized (processed)
            {
                processed.add(event.get());
            }
            complete.countDown();
        };

        final WorkerPool<AtomicInteger> pool =
            new WorkerPool<>(AtomicInteger::new, new FatalExceptionHandler(), handler);
        final ExecutorService executor = Executors.newSingleThreadExecutor(DaemonThreadFactory.INSTANCE);

        try
        {
            final RingBuffer<AtomicInteger> ringBuffer = pool.start(executor);
            publish(ringBuffer, eventCount);

            assertTrue(complete.await(5, TimeUnit.SECONDS));
            for (int i = 0; i < eventCount; i++)
            {
                assertThat(processed.get(i), is(i));
            }
        }
        finally
        {
            pool.halt();
            executor.shutdownNow();
        }
    }

    @Test
    @Timeout(10)
    public void shouldProcessEventsPublishedBeforeStart() throws Exception
    {
        final int eventCount = 50;
        final AtomicIntegerArray deliveries = new AtomicIntegerArray(eventCount);
        final CountDownLatch complete = new CountDownLatch(eventCount);
        final WorkHandler<AtomicInteger> handler = event -> deliver(event, deliveries, complete);

        final RingBuffer<AtomicInteger> ringBuffer = RingBuffer.createMultiProducer(
            AtomicInteger::new, 1024, new BlockingWaitStrategy());
        final WorkerPool<AtomicInteger> pool = new WorkerPool<>(
            ringBuffer, ringBuffer.newBarrier(), new FatalExceptionHandler(), handler);
        ringBuffer.addGatingSequences(pool.getWorkerSequences());
        final ExecutorService executor = Executors.newSingleThreadExecutor(DaemonThreadFactory.INSTANCE);

        try
        {
            publish(ringBuffer, eventCount);
            pool.start(executor);
            assertTrue(complete.await(5, TimeUnit.SECONDS));
        }
        finally
        {
            pool.halt();
            executor.shutdownNow();
        }
    }

    @Test
    @Timeout(10)
    public void shouldAllowFastWorkerToProgressIndependentlyOfSlowWorker() throws Exception
    {
        final int eventCount = 500;
        final AtomicInteger slowCount = new AtomicInteger();
        final AtomicInteger fastCount = new AtomicInteger();
        final CountDownLatch complete = new CountDownLatch(eventCount);

        final WorkHandler<AtomicInteger> slow = event ->
        {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(3));
            slowCount.incrementAndGet();
            complete.countDown();
        };
        final WorkHandler<AtomicInteger> fast = event ->
        {
            fastCount.incrementAndGet();
            complete.countDown();
        };

        final WorkerPool<AtomicInteger> pool =
            new WorkerPool<>(AtomicInteger::new, new FatalExceptionHandler(), slow, fast);
        final ExecutorService executor = Executors.newFixedThreadPool(2, DaemonThreadFactory.INSTANCE);

        try
        {
            final RingBuffer<AtomicInteger> ringBuffer = pool.start(executor);
            publish(ringBuffer, eventCount);

            assertTrue(complete.await(5, TimeUnit.SECONDS));
            assertEquals(eventCount, slowCount.get() + fastCount.get());
            assertTrue(fastCount.get() > slowCount.get());
        }
        finally
        {
            pool.halt();
            executor.shutdownNow();
        }
    }

    @Test
    @Timeout(10)
    public void shouldCreateWorkerPoolThroughDisruptorDsl() throws Exception
    {
        final int eventCount = 100;
        final AtomicInteger count = new AtomicInteger();
        final CountDownLatch complete = new CountDownLatch(eventCount);
        final WorkHandler<AtomicInteger> first = event ->
        {
            count.incrementAndGet();
            complete.countDown();
        };
        final WorkHandler<AtomicInteger> second = event ->
        {
            count.incrementAndGet();
            complete.countDown();
        };

        final Disruptor<AtomicInteger> disruptor = new Disruptor<>(
            AtomicInteger::new,
            1024,
            DaemonThreadFactory.INSTANCE,
            ProducerType.MULTI,
            new BusySpinWaitStrategy());

        try
        {
            disruptor.handleEventsWithWorkerPool(first, second);
            final RingBuffer<AtomicInteger> ringBuffer = disruptor.start();
            publish(ringBuffer, eventCount);

            assertTrue(complete.await(5, TimeUnit.SECONDS));
            assertThat(count.get(), is(eventCount));
        }
        finally
        {
            disruptor.shutdown();
        }
    }

    @Test
    @Timeout(10)
    public void shouldChainHandlerAfterWorkerPool() throws Exception
    {
        final int eventCount = 100;
        final AtomicInteger workerCount = new AtomicInteger();
        final AtomicInteger downstreamCount = new AtomicInteger();
        final CountDownLatch complete = new CountDownLatch(eventCount);
        final WorkHandler<AtomicInteger> first = event -> workerCount.incrementAndGet();
        final WorkHandler<AtomicInteger> second = event -> workerCount.incrementAndGet();

        final Disruptor<AtomicInteger> disruptor = new Disruptor<>(
            AtomicInteger::new,
            1024,
            DaemonThreadFactory.INSTANCE,
            ProducerType.MULTI,
            new BusySpinWaitStrategy());

        try
        {
            disruptor.handleEventsWithWorkerPool(first, second).then((event, sequence, endOfBatch) ->
            {
                downstreamCount.incrementAndGet();
                complete.countDown();
            });
            final RingBuffer<AtomicInteger> ringBuffer = disruptor.start();
            publish(ringBuffer, eventCount);

            assertTrue(complete.await(5, TimeUnit.SECONDS));
            assertThat(workerCount.get(), is(eventCount));
            assertThat(downstreamCount.get(), is(eventCount));
        }
        finally
        {
            disruptor.shutdown();
        }
    }

    private static void deliver(
        final AtomicInteger event,
        final AtomicIntegerArray deliveries,
        final CountDownLatch complete)
    {
        deliveries.incrementAndGet(event.get());
        complete.countDown();
    }

    private static void publish(final RingBuffer<AtomicInteger> ringBuffer, final int eventCount)
    {
        for (int i = 0; i < eventCount; i++)
        {
            final long sequence = ringBuffer.next();
            ringBuffer.get(sequence).set(i);
            ringBuffer.publish(sequence);
        }
    }
}
