package vertx


import io.vertx.ext.bridge.PermittedOptions
import io.vertx.ext.web.handler.sockjs.BridgeOptions
import io.vertx.ext.web.handler.sockjs.SockJSHandlerOptions
import io.vertx.rxjava.core.AbstractVerticle
import io.vertx.rxjava.core.Vertx
import io.vertx.rxjava.ext.web.Router
import io.vertx.rxjava.ext.web.handler.StaticHandler
import io.vertx.rxjava.ext.web.handler.sockjs.SockJSHandler
import rx.Completable


class WebVerticle : AbstractVerticle() {

    private fun getApiRouter(): Router {
        return Router.router(vertx)
    }

    private fun eventBridge(): Router {
        val opts = SockJSHandlerOptions().setHeartbeatInterval(2000)
        val handler = SockJSHandler.create(vertx, opts)
        val bridgeOpts = BridgeOptions()
                .addInboundPermitted(PermittedOptions().setAddressRegex("tick-address.*"))
                .addOutboundPermitted(PermittedOptions().setAddressRegex("tick-address.*"))
        handler.bridge(bridgeOpts)
        val router = Router.router(vertx)
        router.route("/").handler(handler)
        return router
    }

    override fun rxStart(): Completable {
        val router = Router.router(vertx)

        router.mountSubRouter("/events", eventBridge())
        router.route("/*")
                .handler(StaticHandler.create()
                        .setWebRoot("public")
                        .setCachingEnabled(false)
                        .setIndexPage("index.html"))


        return vertx.createHttpServer()
                .requestHandler(router)
                .webSocketHandler { ctx ->
                    println("received ")
                    ctx.writeTextMessage("ping")
                }
                .rxListen(8080)
                .toCompletable()


    }
}


fun main(vararg args: String) {
    val v = Vertx.vertx()

    v.rxDeployVerticle(WebVerticle())
            .subscribe()

    // val taskRepo = TaskRepository()
    //taskRepo.saveSpecs(listOf(TaskSpec(id = "t0")))
    //vertx.deployVerticle(TaskManagerApiVerticle(taskRepo))
}