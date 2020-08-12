package orchestration.verticle


import io.vertx.core.json.JsonObject
import io.vertx.core.logging.Logger
import io.vertx.core.logging.LoggerFactory
import io.vertx.ext.unit.TestContext
import io.vertx.rxjava.core.eventbus.EventBus
import orchestration.OrchestrationEvent
import orchestration.RouteTaskAction
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Test
import repository.TaskRepository
import task.DependencyDefinition
import task.TaskInstance
import task.TaskSpec
import task.TaskStatus.COMPLETED
import task.attrDef


fun EventBus.etlServiceStub(taskRepo: TaskRepository, log: Logger) {
    this.consumer<TaskInstance>("ETL-SERVICE") { message ->
        val task = message.body()
        log.info("Invoked ETL-SERVICE: $task")

        closeTask(task.id).subscribe(
                {
                    val taskInstance = taskRepo.findTaskInstance(task.id)
                    log.info("Request task closure: ${taskInstance?.id}")
                },
                {
                    log.info("failed to close the task $task with error $it")
                })
    }

}

class OrchestrationVerticleTest1 : AbstractOrchestrationVerticleTest() {
    var log = LoggerFactory.getLogger(OrchestrationVerticleTest1::class.java)
    val unroutableAddress = "orchestrate.unroutable"

    private fun defaultEvent(): OrchestrationEvent {
        val payload = JsonObject()
        payload.put("cobDate", "20160101")
        val event = OrchestrationEvent(eventId = "ev1", eventType = "EV-1", payload = payload)
        return event
    }

    /**
     * Simulates the scenario when there are 2 independent tasks where second task depends on the first task
     * a typical etl scenario. Independent means that the task are not grouped in a hierarchy
     * T1 - sourcing the file
     * T2 - processing the file
     */

    @Test(timeout = 100000L)
    fun test2(context: TestContext) {
        val fileSourceTask = TaskSpec(id = "f1-idg-sourcing",
                taskAction = RouteTaskAction("ETL-SERVICE"),
                attributes = mapOf(attrDef("cobDate", true)),
                preConditions = listOf(DependencyDefinition(name = "EV-1")))
        val fileProcessTask = TaskSpec(id = "f1-file-processing",
                taskAction = RouteTaskAction("ETL-SERVICE"),
                attributes = mapOf(attrDef("cobDate", true)),
                preConditions = listOf(DependencyDefinition(name = "f1-idg-sourcing")))

        //Run the integration
        //<editor-fold desc="Vertx initialization">
        val vertx = vertx()
        val eventBus = vertx.eventBus()
        val etlServiceAsync = context.async()
        //</editor-fold>
        val taskRepo = TaskRepository()
        taskRepo.saveSpecs(listOf(fileSourceTask, fileProcessTask))
        eventBus.etlServiceStub(taskRepo, log)
        eventBus.consumer<OrchestrationEvent>(unroutableAddress) { message ->
            val event = message.body()
            log.info("Discarded test unroutable event : $event ")
            etlServiceAsync.complete()
        }

        vertx.rxDeployVerticle(OrchestrationEventVerticle(taskRepo))
                .subscribe {
                    eventBus.orchestrate(defaultEvent())
                }
    }

    /**
     * Simulates the scenario when two inter-related tasks are grouped via parent task
     */
    @Test
    fun testHierarchy(context: TestContext) {
        val fileSourceTask = TaskSpec(id = "f1-idg-sourcing",
                taskAction = RouteTaskAction("ETL-SERVICE"),
                attributes = mapOf(attrDef("cobDate", true)),
                preConditions = listOf(DependencyDefinition(name = "EV-1")))
        val fileProcessTask = TaskSpec(id = "f1-file-processing",
                taskAction = RouteTaskAction("ETL-SERVICE"),
                attributes = mapOf(attrDef("cobDate", true)),
                preConditions = listOf(DependencyDefinition(name = "f1-idg-sourcing")))
        val parentTask = TaskSpec(id = "feed-parent",
                attributes = mapOf(attrDef("cobDate", true)),
                subTasks = setOf(fileSourceTask, fileProcessTask))
        //TODO: complete the assertions

        val taskRepo = TaskRepository()
        taskRepo.saveSpecs(listOf(parentTask))
        val vertx = vertx()
        val eventBus = vertx.eventBus()
        val completeAsync = context.async()
        eventBus.etlServiceStub(taskRepo, log)
        eventBus.consumer<OrchestrationEvent>(unroutableAddress) { message ->
            val event = message.body()
            log.info("Discarded test unroutable event : $event ")
            val inst = taskRepo.findTaskInstance("feed-parent-20160101")
            assertNotNull(inst)
            assertEquals(COMPLETED, inst?.status)
            assertEquals(2, inst?.subTasks?.size)

            completeAsync.complete()

        }
        vertx.rxDeployVerticle(OrchestrationEventVerticle(taskRepo))
                .subscribe {
                    eventBus.orchestrate(defaultEvent())
                }
    }

    /**
     * Simulates the scenario when there are 3 independent tasks with: second task depends on first task and the third task on second
     * a typical etl scenario. Independent means that the task are not grouped in a hierarchy
     * T1 - sourcing the file
     * T2 - processing the file
     * T3 - run data quality checks/ aggregation or other post-processing
     *
     *
     */
    @Test
    fun test3(context: TestContext) {

        val sourcingTask = TaskSpec(id = "source-file",
                taskAction = RouteTaskAction("ETL-SERVICE"),
                preConditions = listOf(DependencyDefinition("EV-1")),
                attributes = mapOf(attrDef("cobDate")))

        val processingTask = TaskSpec(id = "processing-file",
                taskAction = RouteTaskAction("ETL-SERVICE"),
                attributes = mapOf(attrDef("cobDate")),
                preConditions = listOf(DependencyDefinition("source-file")))

        val qualityTask = TaskSpec(id = "quality-check",
                taskAction = RouteTaskAction("ETL-SERVICE"),
                attributes = mapOf(attrDef("cobDate")),
                preConditions = listOf(DependencyDefinition("processing-file")))

        val taskRepo = TaskRepository()
        taskRepo.saveSpecs(listOf(sourcingTask, processingTask, qualityTask))
        val vertx = vertx()
        val eventBus = vertx.eventBus()
        val completeAsync = context.async()
        eventBus.etlServiceStub(taskRepo, log)
        eventBus.consumer<OrchestrationEvent>(unroutableAddress) { message ->
            completeAsync.complete()
        }

        vertx.rxDeployVerticle(OrchestrationEventVerticle(taskRepo))
                .subscribe {
                    eventBus.orchestrate(defaultEvent())
                }

    }
}