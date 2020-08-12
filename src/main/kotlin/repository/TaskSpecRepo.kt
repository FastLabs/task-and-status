package repository

import rx.Single
import task.TaskSpec
import task.TaskSpecMatch
import task.collectDependent

interface TaskSpecRepo {
    fun findTaskSpecForDependency(depName: String): Single<Set<TaskSpecMatch>>
    fun saveSpecs(taskSpecs: List<TaskSpec>): Single<PersistResult>

    //TODO: to work on single or observable
    fun getAllTaskSpecs(): List<TaskSpec>
}


class InMemoryTaskSpecRepo : TaskSpecRepo {


    private val specIndex = mutableMapOf<String, TaskSpec>()

    override fun getAllTaskSpecs(): List<TaskSpec> {
        return specIndex.values.toList()
    }

    override fun findTaskSpecForDependency(depName: String): Single<Set<TaskSpecMatch>> {
        return Single.just(specIndex.values
                .map { TaskSpecMatch(it, it.collectDependent(setOf(depName))) }
                .filter { it.matched.isNotEmpty() }
                .toSet())
    }

    override fun saveSpecs(taskSpecs: List<TaskSpec>): Single<PersistResult> {
        taskSpecs.forEach { specIndex[it.id] = it }
        return Single.just(PersistResult(code = "ok"))
    }

}