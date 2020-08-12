package task

import org.junit.Assert.assertEquals
import org.junit.Test


class TaskSpecBuilderTest {
    @Test
    fun simpleCreation() {
        val t = taskDef { id = "task-1" }
        assertEquals("task-1", t.id)
    }
}
