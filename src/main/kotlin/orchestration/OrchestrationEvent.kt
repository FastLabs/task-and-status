package orchestration

import io.vertx.core.json.JsonObject


data class OrchestrationEvent(val eventId :String , val eventType: String, val payload: JsonObject)

