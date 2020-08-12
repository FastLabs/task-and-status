package orchestration.rules

import orchestration.*
import task.*
import task.TaskStatus.*

/**
 * Checks if all actions are marked as complete
 * /?TODO: can be shortcuted until first non-complte action
 */
private fun List<OrchestrateTaskAction>.allComplete(): Boolean {
    return this.fold(true, { r, action ->

        r && action.task.isHierarchyComplete() && action.task.status == COMPLETED
    })
}

/**
 * Checks if there are any failures for a particular collection of actions
 * TODO: it is possible to shortcut the checks sql first failure
 */
private fun List<OrchestrateTaskAction>.anyFailure(): Boolean {
    return this.fold(false, { r, action ->
        r || action.task.status == FAILED
    })
}


/**
 * identify what sub-tasks can be scheduled:
 * first pending task always can be scheduled
 * others tasks only if are marked as parallels.
 * TODO: there are more complex scenarios such as first not marked as concurrent but the next one or viceversa, to cover these situations
 */
private fun TaskInstance.getPendingSubTasks(): List <TaskInstance> { //TODO: immutable collections
    return this.subTasks.filter { it.status == PENDING }
            .fold(mutableListOf(), { col, task ->
                when {
                    col.size == 0 -> col.add(task)
                    col.size > 0 && task.allDependenciesMeet() -> col.add(task)
                }
                col
            })
}

/**
 * When a task hierarchy instance is validated an existing task can be one of these statuses
 */
private val validStateToBe = setOf(COMPLETED, FAILED, PENDING)

private fun TaskInstance.isOrchestrationReady(event: OrchestrationEvent): Boolean {
    val specId = event.eventType
    val matched = this.findSubBySpecId(specId)
    return if (matched != null) validStateToBe.contains(matched.status) else this.taskSpec.dependsOn(setOf(specId))
}

/**
 * Tries to figure out the next actions for a particular task
 *
 */

fun TaskInstance.evaluateActions(event: OrchestrationEvent? = null): List<OrchestrateTaskAction> {
    if (event != null) {
        if (!isOrchestrationReady(event))
            return listOf(unroutable(event, "There is a in PROGRESS or SCHEDULED task instance that matches ${event.eventType} event"))
    } else {
        if (!isHierarchyComplete()) {
            return listOf(noAction("Hierarchy is not complete"))
        }
    }

    if (status == PENDING) {
        if (!allDependenciesMeet()) {                      //if dependencies are not meet no action required
            return listOf(noAction())
        }
        val subCompleted = didChildrenComplete()
        if (subCompleted) {
            if (taskSpec.taskAction is NoTaskAction) {
                return listOf(complete())
            } else {
                return listOf(schedule())

            }
        } else {
            val pendingSchedules = getPendingSubTasks()
            val results = pendingSchedules.fold(mutableListOf<OrchestrateTaskAction>(), { col, subTask ->
                col.addAll(subTask.evaluateActions(event))
                col
            })

            return when {
                results.size == 0 && didChildrenFail() -> listOf(fail())
                results.anyFailure() -> listOf(fail()) + results
                results.allComplete() -> listOf(complete()) + results
                else -> results.toList()
            }
        }
    }
    return listOf()
}
