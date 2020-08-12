package task

import orchestration.NoTaskAction
import orchestration.TaskAction


fun taskDef(init: TaskSpecBuilder.() -> Unit): TaskSpec {
    val builder = TaskSpecBuilder()
    builder.init()
    return builder.toTaskSpec()
}

fun attr(init: AttributeBuilder.() -> Unit): TaskAttributeDefinition {
    val builder = AttributeBuilder()
    builder.init()
    return builder.build()
}


class AttributeBuilder {
    var name = "no-name"
    var mandatory = false
    var taskArgument = false
    fun build(): TaskAttributeDefinition {
        return TaskAttributeDefinition(name = name, mandatory = mandatory, taskArgument = taskArgument)
    }
}

//TODO: review the task speck builder
class TaskSpecBuilder {
    var id = "UNKNOWN"
    var description = ""
    private val subTasks = mutableListOf<TaskSpecBuilder.() -> Unit>()
    private val conditions = mutableListOf<DependencyDefinition>()
    private var taskAction: TaskAction = NoTaskAction("Action Not Assigned")
    private var owner: TaskSpec? = null
    private val attributes = mutableListOf<TaskAttributeDefinition>()

    fun subTasks(vararg inits: TaskSpecBuilder.() -> Unit) {
        inits.forEach { subTasks.add(it) }
    }

    fun attributes(vararg a: TaskAttributeDefinition) {
        attributes.addAll(a.toList())
    }

    fun action(taskAction: TaskAction) {
        this.taskAction = taskAction
    }

    fun preConditions(vararg preCondition: DependencyDefinition) {
        preCondition.forEach { conditions.add(it) }
    }

    fun toTaskSpec(): TaskSpec {
        val sub = subTasks.map {
            val subBuilder = TaskSpecBuilder()
            subBuilder.apply(it)
            subBuilder.toTaskSpec()
        }.toSet()

        return TaskSpec(id = id, subTasks = sub,
                taskAction = taskAction,
                preConditions = conditions)
    }
}