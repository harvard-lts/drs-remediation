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

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import lombok.extern.slf4j.Slf4j;

/**
 * Process task queue to execute queue of tasks with provided parallelism.
 */
@Slf4j
public class ProcessTaskQueue {

    private final long parallelism;

    private final Callback callback;

    private final ExecutorService executor;

    private final BlockingQueue<ProcessTask> inProcess;

    private final BlockingQueue<ProcessTask> inWait;

    /**
     * Process task queue constructor.
     *
     * @param parallelism parallelism desired for queue processing
     * @param callback    callback for when queue completes
     */
    public ProcessTaskQueue(int parallelism, Callback callback) {
        this.parallelism = parallelism;
        this.callback = callback;
        this.executor = newFixedThreadPool(parallelism);
        this.inProcess = new ArrayBlockingQueue<>(parallelism);
        this.inWait = new ArrayBlockingQueue<>(98304);
    }

    /**
     * Queue up process task.
     *
     * @param task process task to be queued
     */
    public synchronized void submit(ProcessTask task) {
        log.info("submitting task {}", task.id());
        if (inProcess.size() < this.parallelism) {
            inProcess.add(task);
            start(task);
        } else {
            log.info("task {} queued in wait at position {}", task.id(), inWait.size());
            inWait.add(task);
        }
    }

    private void start(ProcessTask task) {
        CompletableFuture.supplyAsync(() -> task.execute(), executor)
            .thenAccept(this::complete);
    }

    private synchronized void complete(ProcessTask task) {
        log.info("task {} complete", task.id());
        task.complete();
        inProcess.remove(task);
        ProcessTask nextTask = inWait.poll();
        if (Objects.nonNull(nextTask)) {
            log.info("task {} dequeued; {} remaining in wait", task.id(), inWait.size());
            inProcess.add(nextTask);
            start(nextTask);
        } else {
            log.info("in wait queue is empty; {} in process", inProcess.size());
            if (inProcess.isEmpty()) {
                try {
                    shutdown();
                } catch (InterruptedException e) {
                    throw new RuntimeException("Failed shutdown process queue", e);
                }
            }
        }
    }

    private void shutdown() throws InterruptedException {
        log.info("shutting down process task queue");
        executor.shutdown();
        this.callback.complete();
    }

}
