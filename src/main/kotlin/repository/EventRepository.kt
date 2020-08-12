package repository

import orchestration.OrchestrationEvent

class EventRepository {

    fun save(event: OrchestrationEvent) {
        println(" save $event")
    }
}
