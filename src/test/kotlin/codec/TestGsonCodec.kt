package codec


import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import task.TaskStatus.STARTED

import task.TaskInstance
import org.junit.Test
import task.TaskSpec

data class MyTest(val name: String? = "")


//TODO: review this class if is required
class TestGsonCodec {

    @Test fun testSimple() {
        val gson = Gson()

        println(gson.toJson(MyTest(name = "Oleg")))
        val yy = TypeToken.get(MyTest::class.java)
        val src = "{\"name\" : \"Oleg1\"}"
        val x = gson.fromJson<MyTest>(src, yy.type)


        val task = TaskInstance(id = "1", status = STARTED, taskSpec = TaskSpec(id = "T1"))
        val jsonTask = gson.toJson(task)
        println("task---")
        println(gson.fromJson<TaskInstance>(jsonTask, yy.type))

        println(x.name)

    }
}