package orchestration.verticle

import io.vertx.core.json.JsonObject
import io.vertx.ext.unit.TestContext
import orchestration.OrchestrationEvent
import orchestration.RouteTaskAction
import org.junit.Assert.assertNotNull
import org.junit.Test
import repository.TaskRepository
import task.*
import task.TaskStatus.SCHEDULED


class OrchestrationVerticleTest : AbstractOrchestrationVerticleTest() {

    @Test(timeout = 100000L)
    fun simpleTest(context: TestContext) {
        val vertx = vertx()
        val closeAsync = context.async()
        val unroutableAsync = context.async()
        val eventBus = vertx.eventBus()
        val taskRepo = TaskRepository()
        val taskSpec = TaskSpec(id = "simple-task",
                taskAction = RouteTaskAction("ETL_SERVICE"),
                preConditions = listOf(DependencyDefinition(name = "EV-1")))
        taskRepo.saveSpecs(listOf(taskSpec))

        eventBus.consumer<TaskInstance>("ETL_SERVICE") { message ->
            val tInst = message.body()
            context.assertEquals(taskSpec, tInst.taskSpec)
            context.assertEquals(SCHEDULED, tInst.status)

            eventBus.closeTask(tInst.id)
                    .subscribe({ msg ->
                        context.assertEquals("success - simple-task", msg)
                        val task = taskRepo.findTaskInstance("simple-task")
                        assertNotNull(task)
                        context.assertEquals(TaskStatus.COMPLETED, task?.status)
                        closeAsync.complete()
                    }, { err ->
                        context.fail(err)
                    })
        }
        eventBus.consumer<OrchestrationEvent>("orchestrate.unroutable") { message ->
            context.assertEquals("simple-task", message.body().eventType)
            unroutableAsync.complete()
        }

        vertx.rxDeployVerticle(OrchestrationEventVerticle(taskRepo = taskRepo))
                .subscribe {
                    val event = OrchestrationEvent(eventId = "ev1", eventType = "EV-1", payload = JsonObject())
                    eventBus.orchestrate(event)
                }
    }

    @Test
    fun test2(context: TestContext) {
        val taskRepo = TaskRepository()
        val taskSpec = TaskSpec(id = "simple-task",
                taskAction = RouteTaskAction("ETL-SERVICE"),
                preConditions = listOf(DependencyDefinition(name = "EV-1")))
        taskRepo.saveSpecs(listOf(taskSpec))

    }
}