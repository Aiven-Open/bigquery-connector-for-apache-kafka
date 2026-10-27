package com.wepay.kafka.connect.bigquery.threads;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.StringWriter;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class KcbqScheduledThreadPoolExecutorTest {

  @Test
  void textExecutor() throws InterruptedException {
    AtomicInteger ex1Counter = new AtomicInteger(0);
    AtomicInteger ex2Counter = new AtomicInteger(0);
    StringWriter writer = new StringWriter();
    KcbqScheduledThreadPoolExecutor executor = new KcbqScheduledThreadPoolExecutor();
    NamedRunnable ex1 =
        new NamedRunnable(
            () -> {
              synchronized (writer) {
                writer
                    .append("ex1 - execution ")
                    .append(Integer.toString(ex1Counter.getAndIncrement()))
                    .append(System.lineSeparator());
              }
            },
            "Ex1");

    NamedRunnable ex2 =
        new NamedRunnable(
            () -> {
              synchronized (writer) {
                writer
                    .append("ex2 - execution ")
                    .append(Integer.toString(ex2Counter.getAndIncrement()))
                    .append(System.lineSeparator());
              }
            },
            "Ex2");
    executor.scheduleWithFixedDelay(ex1, 0, 1, TimeUnit.SECONDS);
    executor.scheduleWithFixedDelay(ex2, 0, 1, TimeUnit.SECONDS);

    for (int i = 0; i < 10; i++) {
      synchronized (writer) {
        writer.append(executor.status()).append(System.lineSeparator());
      }
      Thread.sleep(1000);
    }

    synchronized (writer) {
      writer.append("Shutting down").append(System.lineSeparator());
    }
    executor.shutdown();

    String result = writer.toString();
    // we should have execution 9 for both executors but because of potential load issues we will
    // only check for 8
    assertThat(result).contains("ex1 - execution 8").contains("ex2 - execution 8");
  }

  @Test
  void textExecutorWithException() throws InterruptedException {
    AtomicInteger ex1Counter = new AtomicInteger(0);
    AtomicInteger ex2Counter = new AtomicInteger(0);
    StringWriter writer = new StringWriter();
    KcbqScheduledThreadPoolExecutor executor = new KcbqScheduledThreadPoolExecutor();
    NamedRunnable ex1 =
        new NamedRunnable(
            () -> {
              if (ex2Counter.get() == 5) {
                throw new RuntimeException("Thrown exception");
              }
              synchronized (writer) {
                writer
                    .append("ex1 - execution ")
                    .append(Integer.toString(ex1Counter.getAndIncrement()))
                    .append(System.lineSeparator());
              }
            },
            "Ex1");

    NamedRunnable ex2 =
        new NamedRunnable(
            () -> {
              synchronized (writer) {
                writer
                    .append("ex2 - execution ")
                    .append(Integer.toString(ex2Counter.getAndIncrement()))
                    .append(System.lineSeparator());
              }
            },
            "Ex2");
    executor.scheduleWithFixedDelay(ex1, 0, 1, TimeUnit.SECONDS);
    executor.scheduleWithFixedDelay(ex2, 0, 1, TimeUnit.SECONDS);

    for (int i = 0; i < 10; i++) {
      synchronized (writer) {
        writer.append(executor.status()).append(System.lineSeparator());
      }
      Thread.sleep(1000);
    }

    synchronized (writer) {
      writer.append("Shutting down").append(System.lineSeparator());
    }
    executor.shutdown();

    String result = writer.toString();
    // we should have execution 9 for both executors but because of potential load issues we will
    // only check for 8
    assertThat(result)
        .contains("ex1 - execution 4")
        .contains("ex2 - execution 9")
        .doesNotContain("ex1 - execution 5");
    assertThat(executor.getLastError()).isPresent();
    assertThat(executor.getLastError().get()).hasMessageContaining("Thrown exception");
  }
}
