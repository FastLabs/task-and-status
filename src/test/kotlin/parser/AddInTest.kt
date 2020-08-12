package parser

import org.junit.Test


class AddInTest {


    fun addIn(owner: Any, path: List<String>, value: Any): Triple<String, Any, Boolean> {
        val current = path.first()

        if (path.size == 1) {
            val nR = when (owner) { is Map<*, *> -> owner.containsKey(current)
                else -> false
            }
            return Triple(current, value, nR)
        }

        val currentContainer = when (owner) {
            is MutableMap<*, *> -> owner[current] ?: mutableMapOf<String, Any>()
            is List<*> -> owner.last() ?: mutableMapOf<String, Any>()
            else -> mutableMapOf<String, Any>()
        }
        val candidate = addIn(currentContainer, path.takeLast(path.size - 1), value)
        if(currentContainer is MutableMap<*,*>) {
            val mm = currentContainer as MutableMap<String, Any>
            if (currentContainer.containsKey(candidate.first) && candidate.third) {
                val x = currentContainer[candidate.first]
                if(x is MutableList<*>) {
                    mm[candidate.first] = x + candidate.second
                } else {
                    currentContainer[candidate.first] = mutableListOf(x, candidate.second)
                }

            } else {
                currentContainer[candidate.first] = candidate.second
            }
        }


        return Triple(current, currentContainer, false)

    }

    fun MutableMap<String, Any>.addIn(path: List<String>, value: Any) {

        val x = addIn(this, path, value)
        this[x.first] = x.second
    }

    @Test fun testAddIn() {

        val m = mutableMapOf<String, Any>()
        m.addIn(listOf("admin", "user", "name"), "Oleg")
        m.addIn(listOf("admin", "user", "surname"), "Bulavitchi")
        println(m)
    }

    @Test fun testAddIn1() {
        val m = mutableMapOf<String, Any>()
        m.addIn(listOf("admin", "user", "name"), "Oleg")
        m.addIn(listOf("admin", "user", "name"), "Luca")
        m.addIn(listOf("admin", "user", "name"), "Ioana")
        println(m)
    }

    @Test fun testAddIn2() {
        val m = mutableMapOf<String, Any>()
        m.addIn(listOf("admin", "user", "name"), "Oleg")
        m.addIn(listOf("admin", "user", "name"), "Luca")
        m.addIn(listOf("admin", "user", "addresses", "address", "street"), "Garden Avenue")
        m.addIn(listOf("admin", "user", "addresses", "address", "town"), "xx")
        m.addIn(listOf("admin", "user", "addresses", "address", "street"), "Oak Way")
        m.addIn(listOf("admin", "user", "addresses", "address", "town"), "yy")
        println(m)
    }

}