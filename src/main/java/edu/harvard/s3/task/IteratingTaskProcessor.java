/**
 * Copyright (c) 2021 President and Fellows of Harvard College
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.harvard.s3.task;

import static java.util.concurrent.Executors.newFixedThreadPool;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

/**
 * Concurrently process tasks at parallelism level until iterator completes.
 */
@Slf4j
public class IteratingTaskProcessor<T extends ProcessTask> {

    private final long parallelism;

    private final Iterator<T> iterator;

    private final Callback callback;

    private final ExecutorService executor;

    private final AtomicInteger count;

    private final AtomicInteger total;

    /**
     * Iterating task processor constructor.
     *
     * @param parallelism parallelism desired for processing
     * @param iterator    iterator of process tasks
     * @param callback    callback for when iterator completes
     */
    public IteratingTaskProcessor(int parallelism, Iterator<T> iterator, Callback callback) {
        this.parallelism = parallelism;
        this.iterator = iterator;
        this.callback = callback;
        this.executor = newFixedThreadPool(parallelism);
        this.count = new AtomicInteger(0);
        this.total = new AtomicInteger(0);
    }

    /**
     * Start iterating task processor.
     */
    public void start() {
        int i = 0;
        while (this.iterator.hasNext() && i++ < parallelism) {
            submit(this.iterator.next());
        }
    }

    /**
     * Submit task to executor service.
     *
     * @param task process task to submit to executor service
     */
    public void submit(ProcessTask task) {
        log.info("submitting task {}: {}", this.count.incrementAndGet(), task.id());
        CompletableFuture.supplyAsync(() -> task.execute(), executor)
            .thenAccept(t -> {
                try {
                    complete(t);
                } catch (InterruptedException e) {
                    log.error("failed to complete task", e);
                }
            });
    }

    private synchronized void complete(ProcessTask task) throws InterruptedException {
        log.info("completing task {}: {} - {}", this.count.getAndDecrement(), task.id(), this.total.incrementAndGet());
        task.complete();
        if (this.iterator.hasNext()) {
            submit(this.iterator.next());
        } else {
            shutdown();
        }
    }

    private void shutdown() throws InterruptedException {
        log.info("shutting down task processor");
        executor.shutdown();
        while (this.count.get() > 0 && !executor.awaitTermination(15, TimeUnit.SECONDS)) {}
        this.callback.complete();
    }

}
