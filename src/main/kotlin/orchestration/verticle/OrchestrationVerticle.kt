package orchestration.verticle

import codec.OrchestrationEventCodec
import codec.TaskInstanceCodec

import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.logging.LoggerFactory


import io.vertx.rxjava.core.AbstractVerticle
import io.vertx.rxjava.core.eventbus.EventBus
import orchestration.*
import orchestration.rules.evaluateActions
import repository.TaskRepository
import rx.Single
import task.TaskInstance
import task.TaskStatus.COMPLETED
import task.TaskStatus.FAILED
import task.findSubByTaskId


private const val orchestrateEventAddress = "orchestrate.event"
private const val unroutableEventAddress = "orchestrate.unroutable"
private const val closeTaskAddress = "orchestrate.task.close"
//private val computeTaskStatus = "orchestrate.task.refresh"

fun EventBus.orchestrate(event: OrchestrationEvent) {
    val opts = DeliveryOptions()
    opts.codecName = OrchestrationEventCodec.name
    this.send(orchestrateEventAddress, event, opts)
}


fun EventBus.closeTask(taskId: String): Single<String> {
    return this.rxRequest<String>(closeTaskAddress, taskId)
            .map { it.body() }
}

private fun EventBus.routeTask(route: String, tInstance: TaskInstance) {
    val msgOpts = DeliveryOptions()
    msgOpts.codecName = TaskInstanceCodec.name
    this.send(route, tInstance, msgOpts)
}

private fun EventBus.unroutable(orchEvent: OrchestrationEvent) {
    val opts = DeliveryOptions()
    opts.codecName = OrchestrationEventCodec.name
    this.send(unroutableEventAddress, orchEvent, opts)
}


class OrchestrationEventVerticle(private val taskRepo: TaskRepository = TaskRepository()) : AbstractVerticle() {

    var log = LoggerFactory.getLogger(OrchestrationEventVerticle::class.java)

    private fun routeAction(routeTaskAction: RouteTaskAction, tInstance: TaskInstance) {
        val eventBus = vertx.eventBus()!!

        taskRepo.saveInstance(tInstance).subscribe {
            val taskId = tInstance.id
            log.info("Persisted the task $taskId with status ${tInstance.status}")
            log.info("Route the task to: ${routeTaskAction.route}")
            eventBus.routeTask(routeTaskAction.route, tInstance)
        }
    }

    private fun persistAction(persistTaskAction: PersistTaskAction, tInstance: TaskInstance) {
       // val eventBus = vertx.eventBus()

        log.info("persist action with the reason: ${persistTaskAction.reason}")
        taskRepo.saveInstance(tInstance)
                .subscribe {
                    when (tInstance.status) { //TODO: review the actions
                        // COMPLETED -> eventBus.orchestrate(tInstance.toOrchestrationEvent())
                        FAILED -> println("FAILED")
                        else -> println("Not able to orchestrate ${tInstance.id}")
                    }
                }
    }

    private fun unroutableAction(unroutableAction: UnroutableAction) {
        val eventBus = vertx.eventBus()!!
        log.warn("Unroutable event with reason ${unroutableAction.reason}")
        eventBus.unroutable(unroutableAction.event)
    }

    //TODO: review this function, is recursiv and some actions are async, I think best option is to batch all operations
    //in a single async option
    private fun applyTaskAction(oa: OrchestrateTaskAction) {
        when (oa.action) {
            is ProcessHierarchyAction -> oa.task.evaluateActions(oa.action.event).forEach { applyTaskAction(it) }
            is RouteTaskAction -> routeAction(oa.action, oa.task)
            is NoTaskAction -> log.info("no task action for task ${oa.task.id} because ${oa.action.reason}")
            is PersistTaskAction -> persistAction(oa.action, oa.task)
            is UnroutableAction -> unroutableAction(oa.action)
            else -> log.info("Not Applicable action for task ${oa.task.id}")
        }
    }

    /**
     * A new received event is matched o a task hierarchy and then all the resulted actions are executed
     */
    private fun orchestrateEvent(action: OrchestrateEventAction) {
        val (event) = action
        taskRepo.matchTaskHierarchy(event)
                .map { actions -> actions.forEach { applyTaskAction(it) } }
                .subscribe(
                        {
                            log.info("Apply orchestration rules for ${event.eventType} event type")
                        },
                        { err ->
                            log.warn("Unable to process with reason $err")
                            event.unroutable("Error when accessing task spec repository for ${event.eventType}")
                                    .applyEventAction()
                        })
    }

    private fun EventAction.applyEventAction() {
        when (this) {
            is OrchestrateEventAction -> orchestrateEvent(this)
            is UnroutableAction -> unroutableAction(this)
            else -> println("xxx")
        }
    }

    /**
     * If we want to process a particular hierarchy then this should be invoked
     * usually when we assume that there is a task closed or we want to trigger a task hierarchy process
     * if there is no pending hierarchy we will try to find completed and trigger the orchestration action
     * in order fo fire the dependent tasks
     * TODO: check the async flow
     */
    private fun processPending(taskId: String) {
        val pendingHierarchy = taskRepo.findPendingHierarchy(taskId)
        if (pendingHierarchy != null) {
            log.info("Found ${pendingHierarchy.id} as pending")
            val sub = pendingHierarchy.findSubByTaskId(taskId)

            pendingHierarchy.evaluateActions()
                    .forEach { applyTaskAction(it) }

            sub?.completeEvent()?.applyEventAction()
        } else {
            val completeCandidate = taskRepo.findTaskInstance(taskId)
            completeCandidate?.completeEvent()?.applyEventAction()
        }

    }

    override fun start() {
        val eventBus = vertx.eventBus()!!
        //listen for the orchestration events
        eventBus.consumer<OrchestrationEvent>(orchestrateEventAddress) { message ->
            message.body()?.let { event ->
                event.matchTaskAction().applyEventAction()
            }
        }
        //listen to close events requests
        eventBus.consumer<String>(closeTaskAddress) { message ->
            message.body()?.let {
                log.info("An attempt to close the task with the id $it")
                val tInst = taskRepo.findTaskInstance(it)
                tInst?.let {
                    taskRepo.saveInstance(tInst.copy(status = COMPLETED))
                            .subscribe({
                                log.info("Process pending hierarchies for ${tInst.id}")
                                processPending(tInst.id)
                                message.reply("success - ${tInst.id}")
                            }, { err ->
                                log.error("Unable to save the closed task", err)
                            })

                }
            }
        }
    }
}