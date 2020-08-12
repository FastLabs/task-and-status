package api

import com.google.gson.Gson
import io.vertx.rxjava.core.AbstractVerticle


import io.vertx.rxjava.ext.web.Router
import io.vertx.rxjava.ext.web.handler.BodyHandler
import repository.TaskRepository


class TaskManagerApiVerticle(private val taskRepository: TaskRepository) : AbstractVerticle() {

    val gson = Gson()

    override fun start() {
        val router = Router.router(vertx)
        router.mountSubRouter("/api", taskSpecsRouter())

        vertx.createHttpServer().requestHandler({ router.accept(it) }).listen(8081)
    }

    private fun taskSpecsRouter(): Router {

        val router = Router.router(vertx)
        router.route().handler(BodyHandler.create())
        router.route().consumes("application/json")
        router.route().produces("application/json")
        router.get("/specs").handler { context ->
            val specs = taskRepository.getAllTaskSpecs()
            context.response()
                    .setStatusCode(200)
                    .end(gson.toJson(specs))
        }
        return router
    }
}