package orchestration.verticle

import codec.appCodecList
import io.vertx.ext.unit.junit.RunTestOnContext
import io.vertx.ext.unit.junit.VertxUnitRunner


import io.vertx.rxjava.core.Vertx
import org.junit.Ignore
import org.junit.Rule
import org.junit.runner.RunWith

@RunWith(VertxUnitRunner::class)
@Ignore
open class AbstractOrchestrationVerticleTest {
    @get:Rule
    val rule = RunTestOnContext()

    fun vertx(): Vertx {
        val vertx = Vertx(rule.vertx()!!)
        val eventBus = vertx.eventBus()
        appCodecList.forEach { eventBus.registerCodec(it) }
        return vertx
    }
}