package task

import orchestration.NoTaskAction
import orchestration.TaskAction


/**
 * Represents a task attribute
 * it can be mandatory or not, if the attribute is required and not present in request the task
 * instance may not be created
 * taskArgument flag indicates that the attribute is part of the task instance identification
 */

data class TaskAttributeDefinition(val name: String,
                                   val taskArgument: Boolean = false, //part of the task identification
                                   val mandatory: Boolean = false)

/**
 * A task spec is uniquely identifiable
 * A task may have a owner if is part of a complex stepped task, the owner task can have attributes that will be inherited
 * by the child tasks. The owner attributes will be used to lookup uniquely the task instance
 * A task may depend on the results of the execution of the next task
 * A task can have a set of attributes some of them mandatory to identify the task instances
 *
 * Additional task can have:
 */

data class TaskSpec(val id: String,
                    val description: String = "",
                    val taskAction: TaskAction = NoTaskAction("Action not assigned"),
                    val subTasks: Set<TaskSpec> = setOf(),
                    val preConditions: List<DependencyDefinition> = listOf(), //TODO: should it be map?
                    val attributes: Map<String, TaskAttributeDefinition> = mapOf()) {

    operator fun get(key: String): TaskAttributeDefinition {
        return attributes[key] ?: TaskAttributeDefinition(name = key)
    }

}

/**
 * A task spec match represents a root task and its matched sub-tasks
 *
 */
data class TaskSpecMatch(val root: TaskSpec, val matched: Set<TaskSpec> = setOf())

/**
 * Collects the path to a particular task specification
 */
fun TaskSpec.path(subSpec: TaskSpec): List<TaskSpec> {
    val result = mutableListOf<TaskSpec>()
    this.subTasks.forEach {
        val collected = it.path(subSpec)
        if (collected.size > 0) {
            result.add(this)
            result.addAll(collected)
        }
    }
    if (this == subSpec) {
        return listOf(this)
    }
    return result
}

//TODO: validate the necessity of this function
@Deprecated("seems that I wold not require this")
fun TaskSpec.containSub(spec: TaskSpec) : Boolean {
    return this == spec || this.subTasks.fold(false, { result, sub-> result || sub.containSub(spec)})

}

/**
 * Returns matched paths from the root task
 */
fun TaskSpecMatch.paths(): List<List<TaskSpec>> {
    return this.matched.map { root.path(it) }
}

data class DependencyDefinition(val name: String)


fun attrDef(name: String, taskAttribute: Boolean = false, mandatory: Boolean = false): Pair<String, TaskAttributeDefinition> {
    return Pair(name, TaskAttributeDefinition(name, taskAttribute, mandatory))
}



