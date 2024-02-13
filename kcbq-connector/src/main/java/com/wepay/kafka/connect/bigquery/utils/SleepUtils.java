package com.wepay.kafka.connect.bigquery.utils;

import java.util.concurrent.ThreadLocalRandom;

public final class SleepUtils {

    public static void waitRandomTime(Time time, long sleepMs, long jitterMs) throws InterruptedException {
        time.sleep(sleepMs + ThreadLocalRandom.current().nextLong(jitterMs));
    }
}
