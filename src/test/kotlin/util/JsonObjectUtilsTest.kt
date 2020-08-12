package util

import io.vertx.core.json.JsonObject
import org.junit.Assert.*
import org.junit.Test


class JsonUtilsTest {

    /**
     * Test simple values: strings, integers...
     * the keys and the values will be always string
     */
    @Test
    fun testFlatJsonSimpleVals() {
        val jsonObj = JsonObject()
        with(jsonObj) {
            put("f1", "v1")
            put("f2", 10)
        }

        val m = jsonObj.flatToMap()
        assertTrue(m.containsKey("f1"))
        assertEquals("v1", m["f1"])
        assertTrue(m.containsKey("f2"))
        assertEquals("10", m["f2"])
    }

    /**
     * A flat map is considered a map where there is no nested values. Even a nested will be flattered
     * and the property names will represent the path:
     * example:
     *
     * {
     *  "amount": 10,
     *  "currency": {"id": 840, "code": "usd"}
     * }
     *
     * will be transformed to
     * {
     *  "amount": "10",
     *  "currency.id" : "840",
     *  "currency.code: "usd"
     * }
     *
     */
    @Test
    fun testFlatJsonHierarchy() {
        //fail("Implement the test")
        val x = with(JsonObject()) {
            put("amount", 10)
            put("currency", with(JsonObject()) {
                put("id", 840)
                put("code", "usd")
            })
        }
        println(x.flatToMap())
    }
}