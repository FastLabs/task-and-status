package repository


import rx.Observable
import rx.Single
import task.TaskInstance
import task.TaskSpec
import task.TaskSpecMatch
import task.TaskStatus

class TaskRepository(private val tSpecRepo: TaskSpecRepo = InMemoryTaskSpecRepo(),
                     private val tInstanceRepo: TaskInstanceRepo = InMemoryTaskInstanceRepo()) : TaskSpecRepo, TaskInstanceRepo {

    override fun getAllTaskSpecs(): List<TaskSpec> {
        return tSpecRepo.getAllTaskSpecs()
    }

    override fun findPendingHierarchy(taskId: String): TaskInstance? {
        return tInstanceRepo.findPendingHierarchy(taskId)
    }

    override fun findTaskInstance(taskId: String): TaskInstance? {
        return tInstanceRepo.findTaskInstance(taskId)
    }


    override fun getTaskInstances(specs: Set<TaskSpec>, statuses: Set<TaskStatus>): Single<List<TaskInstance>> {
        return tInstanceRepo.getTaskInstances(specs)
    }

    override fun findTaskSpecForDependency(depName: String): Single<Set<TaskSpecMatch>> {
        return tSpecRepo.findTaskSpecForDependency(depName)
    }

    override fun saveSpecs(taskSpecs: List<TaskSpec>): Single<PersistResult> {
        return tSpecRepo.saveSpecs(taskSpecs)
    }

    override fun saveInstances(tInstances: List<TaskInstance>): Single<PersistResult> {
        return tInstanceRepo.saveInstances(tInstances)

    }


}