package repository

import org.junit.Assert.assertEquals
import org.junit.Test
import task.DependencyDefinition
import task.TaskSpec


class TaskSpecRepoTest {

    @Test fun testInMemory() {
        val taskSpecRep = InMemoryTaskSpecRepo()
        val spec0 = TaskSpec(id = "spec0", preConditions = listOf(DependencyDefinition("EVENT-1")))
        taskSpecRep.saveSpecs(listOf(spec0))
        val matched = taskSpecRep.findTaskSpecForDependency("EVENT-1").toBlocking().value()
            assertEquals(1, matched.size)
        }


    }

