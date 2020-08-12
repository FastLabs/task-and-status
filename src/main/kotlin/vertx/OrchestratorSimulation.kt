package vertx

import codec.OrchestrationEventCodec
import codec.appCodecList
import io.vertx.core.AbstractVerticle
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.json.JsonObject
import io.vertx.rxjava.core.Vertx
import orchestration.OrchestrationEvent


val dbBigPartyEvent = with(JsonObject(), {
    put("businessDate", "")
    put("timestamp", "")
    put("region", "UK") // UK, US, NL
    put("dataClass", "Party") //Account, Deals, Balances, Party
    put("businessDate", "2016-06-28")
    put("publisher", "dbBig")
    put("sourceSystem", "dbinternet")
})


class DbBigHandler : AbstractVerticle() {

    private fun toOrchestrationEvent(srcMessage: JsonObject): OrchestrationEvent {
        //TODO: validate the message
        val publisher = srcMessage.getString("publisher")
        val region = srcMessage.getString("region")
        val dataClass = srcMessage.getString("dataClass")
        return OrchestrationEvent(eventId = "$publisher-$region-$dataClass", eventType = "$publisher-$region-$dataClass", payload = srcMessage)

    }

    override fun start() {
        val eventBus = vertx.eventBus()

        val deliveryOptions = with(DeliveryOptions(), {
            setCodecName(OrchestrationEventCodec.name)
        })

        eventBus.consumer<JsonObject>("source.db-big", { dBigEvent ->
            println("process message: ${dBigEvent.body()}");
            eventBus.publish("orchestration.event.new", toOrchestrationEvent(dBigEvent.body()), deliveryOptions)
        })

        //sends a simulation event
        eventBus.send("source.db-big", dbBigPartyEvent) // just simple message
    }
}

// Will be converted in a main application not only simulation as it has some setup already
fun main(args: Array<String>) {
    val vertx = Vertx.vertx()
    val eventBus = vertx.eventBus()
    appCodecList.forEach { eventBus.registerCodec(it) }
    vertx.deployVerticle(DbBigHandler())

}