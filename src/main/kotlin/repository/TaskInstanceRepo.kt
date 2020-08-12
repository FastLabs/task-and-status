package repository


import io.vertx.core.logging.LoggerFactory
import orchestration.verticle.OrchestrationEventVerticle
import rx.Single
import task.*
import task.TaskStatus.*


data class PersistResult(val code: String)
interface TaskInstanceRepo {


    fun getTaskInstances(specs: Set<TaskSpec>, statuses: Set<TaskStatus> = setOf(PENDING, SCHEDULED, STARTED)): Single<List<TaskInstance>>

    /**
     * Make sure that the root node of the hierarchy will be passed in the specs
     */
    fun matchTaskInstanceHierarchies(specs: Set<TaskSpecMatch>, attributes: Map<String, String>): Single<List<HierarchyMatch>> {

        val hierarchies = specs.map { it.root }.toSet()
        return getTaskInstances(hierarchies)
                .map { taskInstances ->
                    //TODO: use reduce
                    val result = mutableMapOf<TaskSpec, TaskInstance>()
                    taskInstances.forEach { taskInstance ->
                        val queryParams = taskInstance.taskSpec.selectTaskArguments(attributes)
                        if (taskInstance.matchAttributes(queryParams)) {
                            result.put(taskInstance.taskSpec, taskInstance)
                        }
                    }
                    result
                }
                .map { result ->
                    specs.map { HierarchyMatch(specMatch = it, hierarchyRoot = result[it.root]) }
                }
    }

    fun findTaskInstance(taskId: String): TaskInstance?

    fun saveInstance(tInstance: TaskInstance): Single<PersistResult> {
        return saveInstances(listOf(tInstance))
    }

    fun saveInstances(tInstances: List<TaskInstance>): Single<PersistResult>


    fun findPendingHierarchy(taskId: String): TaskInstance? //TODO: make this asynchronous
}

class InMemoryTaskInstanceRepo() : TaskInstanceRepo {

    var log = LoggerFactory.getLogger(OrchestrationEventVerticle::class.java)

    private var taskInstances = mutableMapOf<String, TaskInstance>()

    override fun findPendingHierarchy(taskId: String): TaskInstance? {
        return taskInstances.values.find { it.status == PENDING && it.containTask(taskId) }
    }

    override fun findTaskInstance(taskId: String): TaskInstance? {
        val tasks = taskInstances.values
        //return tasks.find { it.containTask(taskId) }
        val result = tasks.map {
            it.findSubByTaskId(taskId)
        }.last()

        return result
    }


    override fun getTaskInstances(specs: Set<TaskSpec>, statuses: Set<TaskStatus>): Single<List<TaskInstance>> {
        return Single.just(taskInstances.values
                .flatMap { task ->
                    specs.map { spec -> task.findSubBySpecId(spec.id) }
                }
                .filterNotNull()
                .filter {
                    //specs.contains(it.taskSpec) &&
                    statuses.isEmpty() || statuses.contains(it.status)
                })
    }

    /**
     * Save all the available task and sub-tasks instances in in a flat structure
     */
    override fun saveInstances(newInstances: List<TaskInstance>): Single<PersistResult> {

        newInstances.forEach { newTask ->
            if (taskInstances.containsKey(newTask.id)) {
                taskInstances[newTask.id] = newTask
            } else {
                var matched = false
                taskInstances.values.forEach { repoTask ->
                    if (repoTask.containTask(newTask.id)) {
                        taskInstances[repoTask.id] = repoTask.updateSub(newTask)
                        matched = true
                    }

                }
                if (!matched) {
                    taskInstances[newTask.id] = newTask
                }
            }
        }
        return Single.just(PersistResult(code = "ok"))
    }
}

