package dist


import io.vertx.rxjava.core.AbstractVerticle
import io.vertx.rxjava.core.RxHelper
import io.vertx.rxjava.core.Vertx
import rx.Completable

class First : AbstractVerticle() {
    override fun start() {
        val eventBus = vertx.eventBus();
        eventBus
                .consumer<String>("addr1") { msg ->
                    println(msg.body())
                }
        Completable.complete()
    }
}

fun main(vararg args: String) {
    println("Main App")

    val vertx = Vertx.vertx()
    val eventBus = vertx.eventBus()
    vertx.periodicStream(1000)
            .toObservable()
            .subscribe {
                eventBus.send("addr1", 1)
            }

    RxHelper.deployVerticle(vertx, First())
            .subscribe(::println)
}