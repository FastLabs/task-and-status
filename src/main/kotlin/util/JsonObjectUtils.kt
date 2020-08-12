package util

import io.vertx.core.json.JsonObject
import task.TaskAttribute

fun JsonObject.flatToMap(): Map<String, String> { //TODO: immutability
    val result = mutableMapOf<String, String>()
    this.fieldNames().forEach {
        val value = this.getValue(it)
        when (value) {
            is String -> result[it] = value
            else -> result[it] = value.toString()
        }
    }
    return result
}

fun List<TaskAttribute>.toJson(): JsonObject {
    return this.fold(JsonObject()) { json, attr -> json.put(attr.definition.name, attr.value) }
}