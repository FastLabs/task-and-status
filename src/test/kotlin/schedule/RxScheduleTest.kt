package schedule

import org.junit.Test
import rx.Observable
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit.MILLISECONDS

class RxScheduleTest {

    @Test fun testRx() {
        val latch = CountDownLatch(1)
        Observable.just(CronSpec("0/2 * * * * ?"))
                .flatMap { spec ->
                    val cron = spec.newQuartzCron()
                    if (cron != null) {
                        Observable.just(cron)
                                .map { cronExpression ->
                                    cronExpression.getNextValidTimeAfter(Date())
                                }
                                .map { nextRunDate ->
                                    nextRunDate.time - Date().time
                                }
                                .flatMap { delay ->
                                    println(delay)
                                    Observable.timer(delay, MILLISECONDS) //this should run on a particular scheduler
                                }
                                .timestamp()
                                .repeat(10)

                    } else {
                        throw IllegalArgumentException("Unable to parse cron ${spec.cron}")
                    }
                }
                .subscribe(::println, ::println, { println("Completed"); latch.countDown() })

        latch.await()
    }
}