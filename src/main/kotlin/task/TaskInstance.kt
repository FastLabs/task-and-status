package task

import task.TaskStatus.PENDING
import task.TaskStatus.SCHEDULED
import java.util.*


enum class TaskStatus {
    STARTED,
    FAILED,
    COMPLETED,
    SCHEDULED,
    PENDING
}


data class TaskAttribute(val definition: TaskAttributeDefinition,
                         val value: String? = null)


/**
 * A particular instance of a task
 *
 * potential list of attributes:
 * SOURCE_ID
 * SNAPSHOT_ID
 * ADJUSTMENT_ID
 * BUSINESS_DATE_BASELINE_ID
 *
 *
 */
data class TaskInstance(val id: String,
                        val status: TaskStatus = PENDING,
                        val taskSpec: TaskSpec,
                        val attributes: List<TaskAttribute> = listOf(),
                        val subTasks: List<TaskInstance> = listOf(),
                        val dependsOn: List<TaskDependency> = listOf(),
                        val scheduleTime: Date? = null,
                        val startTime: Date? = null,
                        val endTime: Date? = null) {

    operator fun get(name: String): TaskAttribute? {
        return attributes.find { it.definition.name == name }
    }
}


data class TaskDependency(val name: String, val completed: Boolean = false)

data class HierarchyMatch(val specMatch: TaskSpecMatch, val hierarchyRoot: TaskInstance? = null)


//TODO: check if not better to return TaskInstance instead of the match, at the moment the logic required
//checks only for TaskInstance, rename it as well maybe: buildHierarchy

fun HierarchyMatch.fill(attributes: Map<String, String>, matchDependencies: Set<String>): HierarchyMatch {
    return if (hierarchyRoot == null) {
        this.copy(hierarchyRoot = specMatch.newHierarchy(attributes, matchDependencies))
    } else {
        val hierarchy = this.specMatch.paths()
                .fold(hierarchyRoot) { hierarchy: TaskInstance, path: List<TaskSpec> ->
                    hierarchy.add(attributes, path, matchDependencies)
                }
        this.copy(hierarchyRoot = hierarchy)
    }
}

fun TaskInstance.findSub(predicate: (task: TaskInstance) -> Boolean): TaskInstance? {
    var res: TaskInstance? = null
    if (predicate(this)) {
        res = this
    } else {
        for (x in subTasks) {
            res = x.findSub(predicate)
            if (res != null)
                break
        }

    }
    return res
}

/**
 * Updates a hierarchy with a new node. Scans the hierarchy and if the task-speck is equal it replace the entire
 * branch.
 */
fun TaskInstance.updateSub(newSub: TaskInstance): TaskInstance {
    if (this.taskSpec == newSub.taskSpec) return newSub //else this.updateSub(newSub)
    return this.copy(subTasks = this.subTasks.map { it.updateSub(newSub) })

}

