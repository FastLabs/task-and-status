package orchestration

import task.TaskInstance
import task.TaskStatus
import task.TaskStatus.COMPLETED
import task.TaskStatus.FAILED
import task.didChildrenComplete
import util.toJson
import java.util.*


/*Task related actions */
interface TaskAction

data class OrchestrateTaskAction(val action: TaskAction, val task: TaskInstance)
data class UnroutableAction(val reason: String, val event: OrchestrationEvent) : EventAction, TaskAction
data class NoTaskAction(val reason: String) : TaskAction
data class RouteTaskAction(val route: String, val reason: String? = null) : TaskAction
data class PersistTaskAction(val reason: String) : TaskAction
data class ProcessHierarchyAction(val event: OrchestrationEvent) : TaskAction

/*Event related actions*/
interface EventAction

data class OrchestrateEventAction(val event: OrchestrationEvent) : EventAction


fun TaskInstance.schedule(): OrchestrateTaskAction {
    val started = this.copy(status = TaskStatus.SCHEDULED, scheduleTime = Date())
    return OrchestrateTaskAction(action = this.taskSpec.taskAction, task = started)
}

fun TaskInstance.complete(): OrchestrateTaskAction {
    val endDate = Date()
    val completed = this.copy(status = COMPLETED, endTime = endDate, startTime = this.startTime ?: endDate)
    return OrchestrateTaskAction(action = PersistTaskAction("Task ${this.id} completed"), task = completed)
}

fun TaskInstance.fail(): OrchestrateTaskAction {
    val failed = this.copy(status = FAILED, endTime = Date())
    return OrchestrateTaskAction(action = PersistTaskAction("Task ${this.id} failed"), task = failed)
}

fun TaskInstance.noAction(reason: String = ""): OrchestrateTaskAction {
    return OrchestrateTaskAction(NoTaskAction(reason), this)
}

fun TaskInstance.orchestrate(event: OrchestrationEvent): OrchestrateTaskAction {
    return OrchestrateTaskAction(task = this, action = ProcessHierarchyAction(event))
}

fun TaskInstance.unroutable(event: OrchestrationEvent, reason: String = ""): OrchestrateTaskAction {
    return OrchestrateTaskAction(action = UnroutableAction(reason = reason, event = event), task = this)
}

fun TaskInstance.completeEvent(): OrchestrateEventAction? {
    return if (this.didChildrenComplete()) {
        OrchestrateEventAction(OrchestrationEvent(eventId = this.id, eventType = this.taskSpec.id, payload = this.attributes.toJson()))
    } else {
        null
    }
}

/*Orchestration even actions*/
fun OrchestrationEvent.unroutable(reason: String): UnroutableAction {
    return UnroutableAction(reason = reason, event = this)
}

fun OrchestrationEvent.matchTaskAction(): OrchestrateEventAction {
    return OrchestrateEventAction(this)
}


