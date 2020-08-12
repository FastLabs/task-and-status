package task

import task.TaskStatus.COMPLETED
import task.TaskStatus.FAILED

//TODO: split in task specs and task instances utility? Companion Objects?
/**
 * Creates a new task instance based on a task specification template, fills the attributes map and mark the dependencies as
 * satisfied in case it is provided
 */
fun TaskSpec.newTaskInstance(id: String,
                             attributes: Map<String, String> = mapOf(),
                             matchDependencies: Set<String> = setOf()): TaskInstance {
    val dependsOn = this.preConditions.map {
        TaskDependency(it.name, matchDependencies.contains(it.name))
    }

    val attrs = this.attributes.map { TaskAttribute(definition = it.value, value = attributes[it.value.name]) }
    return TaskInstance(id = id, taskSpec = this, dependsOn = dependsOn, attributes = attrs)
}

fun TaskSpec.newTaskInstance(attributes: Map<String, String> = mapOf(),
                             matchDependencies: Set<String> = setOf()): TaskInstance {
    //join all attributes that are marked as task attribute
    val keyArgs = this.attributes.filter { entry ->
        val taskAttribute = entry.value
        taskAttribute.taskArgument
    }.values.fold("", { prev, argumentDef ->
        "$prev-${attributes[argumentDef.name]}"
    })
    return this.newTaskInstance("${this.id}$keyArgs", attributes, matchDependencies)
}


private fun TaskSpec.getHierarchy(attributes: Map<String, String>, specs: Set<TaskSpec>, matchDependencies: Set<String>): TaskInstance {
    val subs = subTasks.filter { it.dependsOn(matchDependencies) }
            .map { it.getHierarchy(attributes, specs, matchDependencies) }

    if (specs.contains(this)) {
        return this.newTaskInstance(attributes, matchDependencies)
    }
    if (subs.size > 0) {
        return this.newTaskInstance(attributes, matchDependencies).copy(subTasks = subs)
    }
    return this.newTaskInstance(attributes, matchDependencies)
}

fun TaskSpecMatch.newHierarchy(attributes: Map<String, String> = mapOf(), matchDependencies: Set<String> = setOf()): TaskInstance {
    return root.getHierarchy(attributes, this.matched, matchDependencies)
}

/**
 * Checks if a specification matches any depencencies form the set
 */
fun TaskSpec.dependsOn(deps: Set<String>): Boolean {
    val preCond = this.hasDependency(deps)
    return if (!preCond) {
        return subTasks.find { it.dependsOn(deps) } != null
    } else {
        preCond
    }
}

fun TaskSpec.hasDependency(deps: Set<String>): Boolean {
    return preConditions.find { deps.contains(it.name) } != null
}

fun TaskSpec.collectDependent(deps: Set<String>, container: MutableSet<TaskSpec> = mutableSetOf()): Set<TaskSpec> {
    if (this.hasDependency(deps)) {
        container.add(this)
    }
    subTasks.forEach { it.collectDependent(deps, container) }
    return container
}

fun TaskSpec.collectDependent(depName: String, container: MutableSet<TaskSpec> = mutableSetOf()): Set<TaskSpec> {
    return this.collectDependent(setOf(depName), container)
}


/**
 * Selects the tasks arguments from a hash map and converts them to a collection of task attributes
 * a task argument is special case of an task attribute that participates in the task id generation
 */
fun TaskSpec.selectTaskArguments(attributes: Map<String, String>): Collection<TaskAttribute> {

    return this.attributes //collect task attributes
            .filter {
                val (attrName, attrDef) = it
                attrDef.taskArgument
            }
            .map {
                val (attrName, attrDef) = it
                TaskAttribute(definition = attrDef, value = attributes[attrName])
            }
}

fun TaskInstance.isHierarchyComplete() :Boolean {
    return subTasks.size == taskSpec.subTasks.size &&
            subTasks.fold(true, {completeFlag, subTask ->
                return completeFlag && subTask.isHierarchyComplete()
            })
}

/**
 * Checks if a hierarchy is fully in COMPLETED state with all the nodes
 */
fun TaskInstance.didChildrenComplete(): Boolean {
    return  isHierarchyComplete()  && subTasks.fold(true, { r, task ->
        r && task.status == COMPLETED
    })
}

/**
 * Returns the task or sub-task by specification id
 */
//TODO: think about maybe
fun TaskInstance.findSubBySpecId(specId: String): TaskInstance? {
    return findSub { it.taskSpec.id == specId }
}

fun TaskInstance.findSubByTaskId(taskId: String): TaskInstance? {
    return findSub { it.id == taskId }
}

/**
 * Checks if the task instance hierarchy contains a particular task id
 */
fun TaskInstance.containTask(taskId: String): Boolean {
    return findSubByTaskId(taskId) != null
}

fun TaskInstance.didChildrenFail(): Boolean { //TODO: review this
    val failed = this.subTasks.filter { it.status == FAILED }
    return failed.isNotEmpty()
}

/**
 * Return true if all task dependencies are meet
 */
fun TaskInstance.allDependenciesMeet(): Boolean {
    return this.dependsOn.fold(true, { r, dependency ->
        r && dependency.completed
    })
}

/**
 * Checks if a particular dependency is meet
 */
fun TaskInstance.dependencyMeet(depName: String): Boolean {
    val found = this.dependsOn.find { it.name == depName }
    return found?.completed ?: true
}

/**
 * Fills a path in a existing node
 * TODO: review this function: maybe with partial? the parameters are confusing when creating the instance
 * TODO: make sure it performs
 */
fun TaskInstance.add(attributes: Map<String, String>, path: List<TaskSpec>, matchDependencies: Set<String> = setOf()): TaskInstance {
    if (path.size <= 1) {
        return this
    }
    val (firstSpec) = path
    if (firstSpec == this.taskSpec) {
        val subs = subTasks
                .filter { it.taskSpec == firstSpec }
                .map { it.add(attributes, path.drop(1), matchDependencies) }
        if (subs.isNotEmpty()) {
            return this.copy(subTasks = subs)
        } else {
            val sibling = path[1]
            return this.copy(subTasks = this.subTasks + sibling.newTaskInstance(attributes, matchDependencies).add(attributes, path.drop(1), matchDependencies))
        }
    } else {//TODO: inherited attributes should extracted
        val newInstance = firstSpec.newTaskInstance(attributes, matchDependencies).add(attributes, path.drop(1), matchDependencies)
        return this.copy(subTasks = listOf(newInstance))
    }
}

/**
 * Checks if a particular task instance has the requested list of task attributes
 */
fun TaskInstance.matchAttributes(queryParams: Collection<TaskAttribute>): Boolean {
    val result = queryParams.fold(true, { r, s ->
        r && s.value == this[s.definition.name]?.value
    })
    return result
}
