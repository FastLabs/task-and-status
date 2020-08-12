package orchestration

import repository.TaskRepository
import rx.Single
import task.HierarchyMatch
import task.TaskInstance
import task.fill
import util.flatToMap


/**
 * Will return a list task instances to be persisted.
 * if a task specification is not matched against event type, the promise will fail wih unroutable action
 * If no existing hierarchy instance is matched a new one will be created for particular task specification matched against event type
 * if a pending hierarchy is matched, try to create a new task for matched task specification
 */
fun TaskRepository.matchTaskHierarchy(event: OrchestrationEvent): Single<List<OrchestrateTaskAction>> {
    return this.findTaskSpecForDependency(event.eventType)
            .flatMap { specHierarchies ->
                if (specHierarchies.isEmpty()) {
                    throw Exception("Unable to find task spec for ${event.eventType}")
                } else {
                    val attributes = event.payload.flatToMap()
                    this.matchTaskInstanceHierarchies(specHierarchies, attributes)
                            .map { process(event.eventType, attributes, it) }
                }
            }
            .flatMap { hierarchies ->
                this.saveInstances(hierarchies)
                        .map { saveResult -> if (saveResult.code == "ok") hierarchies else emptyList() }
            }
            .map {
                if (it.isNotEmpty())
                    it.map { h -> h.orchestrate(event) }
                else {
                    throw Exception("Unable to persist hierarchies")
                }
            }

}


private fun process(eventType: String, attributes: Map<String, String>, matched: List<HierarchyMatch>): List<TaskInstance> {
    val matchDependencies = setOf(eventType)
    return matched.map { it.fill(attributes, matchDependencies).hierarchyRoot }.filterNotNull()
}