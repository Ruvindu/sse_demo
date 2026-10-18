
package com.dfn.sse_demo.controller;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.time.LocalTime;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@RestController
public class SseController {

    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(2);

    private final AtomicInteger eventCounter = new AtomicInteger();

    @GetMapping(
            value = "/api/events",
            produces = MediaType.TEXT_EVENT_STREAM_VALUE
    )
    public SseEmitter streamEvents() {
        // Keep the connection open until the client disconnects.
        SseEmitter emitter = new SseEmitter(0L);

        AtomicReference<ScheduledFuture<?>> taskRef =
                new AtomicReference<>();

        ScheduledFuture<?> task = scheduler.scheduleAtFixedRate(() -> {
            try {
                int id = eventCounter.incrementAndGet();

                emitter.send(SseEmitter.event()
                        .id(String.valueOf(id))
                        .name("message")
                        .data("Hello from SSE! Event " + id
                                + " at " + LocalTime.now()));

            } catch (IOException | IllegalStateException exception) {
                ScheduledFuture<?> currentTask = taskRef.get();

                if (currentTask != null) {
                    currentTask.cancel(false);
                }
            }
        }, 0, 2, TimeUnit.SECONDS);

        taskRef.set(task);

        emitter.onCompletion(() -> task.cancel(false));
        emitter.onTimeout(() -> task.cancel(false));
        emitter.onError(error -> task.cancel(false));

        return emitter;
    }

    @PreDestroy
    public void shutdownScheduler() {
        scheduler.shutdownNow();
    }
}
