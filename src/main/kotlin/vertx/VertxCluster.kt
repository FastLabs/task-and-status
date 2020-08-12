package vertx

import codec.TaskInstanceCodec
import codec.appCodecList
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.VertxOptions
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.eventbus.EventBus
import io.vertx.rxjava.core.Vertx
import task.TaskInstance
import task.TaskSpec
import task.TaskStatus

//build a task dispatcher application
//Unknowns
//1. JMS Message
//2. HTPPS call
//3. configuration

//dispatching engine:
//at the moment is confusing because of re-play mechanism of the data distribution event


fun EventBus.publishTask(address: String, task: TaskInstance) {
    val sendOptions = with(DeliveryOptions(), {
        setCodecName(TaskInstanceCodec.name)
    })
    publish(address, task, sendOptions)
}

class MessageProducer : AbstractVerticle() {
    override fun start(startFuture: Future<Void>?) {
        val eventBus = vertx.eventBus()
        appCodecList.forEach {
            eventBus.registerCodec(it)
        }

        val task = TaskInstance(id = "1", status = TaskStatus.STARTED, taskSpec = TaskSpec(id = "T1"))
        vertx.setPeriodic(1000, {
            eventBus.publishTask("ping", task)
        })

        startFuture?.complete();
    }
}

class MessageConsumer : AbstractVerticle() {
    override fun start(startFuture: Future<Void>?) {
        val eventBus = vertx.eventBus()
        eventBus.registerCodec(TaskInstanceCodec)
        eventBus.consumer<TaskInstance>("ping", { message ->
            println(message.body())


        })
        startFuture?.complete();
    }
}

private fun clusteredEnv(verticles: Array<AbstractVerticle>) {
    val options = VertxOptions()

    //TODO: insert verticle config here
    Vertx.clusteredVertx(options, { clusterResult ->
        if (clusterResult.succeeded()) {
            val vertx = clusterResult.result()
            verticles.forEach { verticle -> vertx.deployVerticle(verticle) }
        }
    })
}

fun main(args: Array<String>) {
    clusteredEnv(arrayOf(MessageProducer()))

}