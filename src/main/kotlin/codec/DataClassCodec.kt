package codec


import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import io.netty.util.CharsetUtil
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.MessageCodec

open class DataClassCodec <R>( val typeToken :TypeToken<R>, val name : String = typeToken.type.typeName) : MessageCodec<R, R> {
    companion object{
        val gson = Gson()
    }

    override fun transform(v: R?): R? {
        return v
    }

    override fun systemCodecID(): Byte {
        return -1
    }

    override fun name(): String {
        return name
    }

    override fun decodeFromWire(pos: Int, buffer: Buffer): R {
        val length = buffer.getInt(pos)
        val newPos = pos + 4
        val bytes = buffer.getBytes(newPos, newPos + length)
        return gson.fromJson(String(bytes, CharsetUtil.UTF_8), typeToken.type)
    }

    override fun encodeToWire(buffer: Buffer, s: R) {
        val strBytes = gson.toJson(s).toByteArray(CharsetUtil.UTF_8)
        buffer.appendInt(strBytes.size)
        buffer.appendBytes(strBytes)
    }
}

