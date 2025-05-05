package datawave.common.util.concurrent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

public class BoundedBlockingQueueTest {

    private static final Logger log = LogManager.getLogger(BoundedBlockingQueueTest.class);

    @Test
    public void testQueueBounds() throws Exception {
        BoundedBlockingQueue<String> bbq = new BoundedBlockingQueue<>(5, new LinkedList<>());

        bbq.put("one");
        bbq.put("two");
        bbq.put("three");
        bbq.put("four");
        bbq.put("five");

        try {
            assertFalse(bbq.add("dropped data"));
        } catch (IllegalStateException e) {
            assertEquals("Queue full", e.getLocalizedMessage());
        }
        assertEquals(5, bbq.size());

        assertFalse(bbq.offer("more dropped data"));
        assertEquals(5, bbq.size());

        assertFalse(bbq.offer("more dropped data", 100, TimeUnit.MILLISECONDS));
        assertEquals(5, bbq.size());

        Thread.currentThread().interrupt();
        try {
            bbq.put("even more dropped data");
        } catch (InterruptedException e) {
            assertEquals(5, bbq.size());
        }
        assertEquals(5, bbq.size());
    }

    @Test
    public void testConcurrencyLargeQueue() throws Exception {
        testConcurrencyInternal(100, 10, 10, 60);
    }

    @Test
    public void testConcurrencySmallQueue() throws Exception {
        testConcurrencyInternal(20, 10, 10, 60);
    }

    /**
     * This test creates a thread for each major queue operation (i.e. add, offer, offer w/timeout, put, poll, poll w/timeout, take, remove) and an additional
     * thread for the size and remaining capacity operations.
     * <p>
     * Initially, we create a queue of size maxCapacity, and sequentially add numInitialEntries elements to that queue.
     * <p>
     * The producer operations/threads (i.e. add, offer, offer w/timeout, and put) will all add entriesPerOperation elements to the queue each.
     * <p>
     * The consumer operations/threads (i.e. poll, poll w/timeout, take, and remove) will all remove entriesPerOperation elements from the queue each.
     * <p>
     * Each of these threads is run concurrently, so each operation will need to rely on the BoundedBlockingQueue locks in order to control access to the
     * internal queue.
     * <p>
     * If all the threads have finished, then we should be left with numInitialEntries elements in the queue.
     */
    private void testConcurrencyInternal(final int maxCapacity, final int numInitialEntries, final int entriesPerOperation, final int maxTimeoutSeconds)
                    throws Exception {
        final BoundedBlockingQueue<String> bbq = new BoundedBlockingQueue<>(maxCapacity, new PriorityQueue<>());
        final List<Runnable> runnables = new ArrayList<>();

        for (int i = 0; i < numInitialEntries; i++)
            bbq.add("initial " + i);

        // adds 10 entries
        runnables.add(() -> {
            int i = 0;
            try {
                for (i = 0; i < entriesPerOperation; i++) {
                    try {
                        bbq.add("add " + i);
                    } catch (IllegalStateException e) {
                        i--;
                    }
                }
            } finally {
                assertEquals(entriesPerOperation, i);
            }
        });

        // adds 10 entries
        runnables.add(() -> {
            int i = 0;
            try {
                for (i = 0; i < entriesPerOperation; i++)
                    if (!bbq.offer("offer " + i))
                        i--;
            } finally {
                assertEquals(entriesPerOperation, i);
            }
        });

        // adds 10 entries
        runnables.add(() -> {
            int i = 0;
            try {
                for (i = 0; i < entriesPerOperation; i++)
                    if (!bbq.offer("offer timeout " + i, 100, TimeUnit.MILLISECONDS))
                        i--;
            } catch (InterruptedException e) {
                log.error("e: ", e);
            } finally {
                assertEquals(entriesPerOperation, i);
            }
        });

        // adds 10 entries
        runnables.add(() -> {
            int i = 0;
            try {
                for (i = 0; i < entriesPerOperation; i++)
                    bbq.put("put " + i);
            } catch (InterruptedException e) {
                log.error("e: ", e);
            } finally {
                assertEquals(entriesPerOperation, i);
            }
        });

        // removes 10 entries
        runnables.add(() -> {
            int i = 0;
            try {
                for (i = 0; i < entriesPerOperation; i++)
                    if (bbq.poll() == null)
                        i--;
            } finally {
                assertEquals(entriesPerOperation, i);
            }
        });

        // removes 10 entries
        runnables.add(() -> {
            int i = 0;
            try {
                for (i = 0; i < entriesPerOperation; i++)
                    if (bbq.poll(100, TimeUnit.MILLISECONDS) == null)
                        i--;
            } catch (InterruptedException e) {
                log.error("e: ", e);
            } finally {
                assertEquals(entriesPerOperation, i);
            }
        });

        // removes 10 entries
        runnables.add(() -> {
            int i = 0;
            try {
                for (i = 0; i < entriesPerOperation; i++) {
                    bbq.take();
                }
            } catch (InterruptedException e) {
                log.error("e: ", e);
            } finally {
                assertEquals(entriesPerOperation, i);
            }
        });

        // removes 10 entries
        runnables.add(() -> {
            int i = 0;
            try {
                for (i = 0; i < entriesPerOperation; i++) {
                    String peek = bbq.peek();
                    if (!(bbq.contains(peek) && bbq.remove(peek)))
                        i--;
                }
            } finally {
                assertEquals(entriesPerOperation, i);
            }
        });

        // performs additional operations while reading and writing to the queue
        runnables.add(() -> {
            int i = 0;
            try {
                for (i = 0; i < entriesPerOperation; i++) {
                    int size = bbq.size();
                    assertTrue(size <= maxCapacity);
                    int cap = bbq.remainingCapacity();
                    assertTrue(cap >= 0 && cap <= maxCapacity);
                }
            } finally {
                assertEquals(entriesPerOperation, i);
            }
        });

        assertConcurrent("BoundedBlockingQueue concurrency test", runnables, maxTimeoutSeconds);

        Iterator<String> iter = bbq.iterator();
        int numLeft = 0;
        while (iter.hasNext()) {
            iter.next();
            numLeft++;
        }

        assertEquals(numInitialEntries, numLeft);

        List<String> drain = new ArrayList<>();
        bbq.drainTo(drain);

        assertEquals(numInitialEntries, drain.size());
        assertEquals(0, bbq.size());
    }

    /**
     * This is a helper method which will allow us to run all of our threads simultaneously, thus increasing the chance that we will encounter a concurrency
     * problem if there is one.
     * <p>
     * All the runnables that are passed in will block, and will not begin execution until the afterInitBlocker CountDownLatch has been activated. This should
     * ensure that all the threads begin execution simultaneously.
     */
    public static void assertConcurrent(final String message, final List<? extends Runnable> runnables, final int maxTimeoutSeconds)
                    throws InterruptedException {
        final int numThreads = runnables.size();
        final List<Throwable> exceptions = Collections.synchronizedList(new ArrayList<>());
        final ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);
        try {
            final CountDownLatch allExecutorThreadsReady = new CountDownLatch(numThreads);
            final CountDownLatch afterInitBlocker = new CountDownLatch(1);
            final CountDownLatch allDone = new CountDownLatch(numThreads);
            for (final Runnable submittedTestRunnable : runnables) {
                threadPool.submit(() -> {
                    allExecutorThreadsReady.countDown();
                    try {
                        afterInitBlocker.await();
                        submittedTestRunnable.run();
                    } catch (final Throwable e) {
                        exceptions.add(e);
                    } finally {
                        allDone.countDown();
                    }
                });
            }
            // wait until all threads are ready
            assertTrue(allExecutorThreadsReady.await(runnables.size() * 10L, TimeUnit.MILLISECONDS),
                            "Timeout initializing threads! Perform long lasting initializations before passing runnables to assertConcurrent");
            // start all test runners
            afterInitBlocker.countDown();
            assertTrue(allDone.await(maxTimeoutSeconds, TimeUnit.SECONDS), message + " timeout! More than" + maxTimeoutSeconds + "seconds");
        } finally {
            threadPool.shutdownNow();
        }
        assertTrue(exceptions.isEmpty(), message + " failed with exception(s)" + exceptions);
    }
}
