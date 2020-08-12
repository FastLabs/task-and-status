package schedule

import org.quartz.CronExpression
import java.text.ParseException
import java.time.ZoneId
import java.util.*


interface ScheduleAction

data class NoScheduleAction(val reason: String = "Default") : ScheduleAction

data class Schedule(val id: String, val cron: CronSpec, val action: ScheduleAction = NoScheduleAction())
data class CronSpec(val cron: String, val timeZone: TimeZone = TimeZone.getTimeZone(ZoneId.systemDefault()))

fun CronSpec.newQuartzCron(): CronExpression? {
    return try {
        val c = CronExpression(this.cron)
        with(c) { timeZone = this.timeZone }
        c
    } catch (e: ParseException) {
        null
    }
}