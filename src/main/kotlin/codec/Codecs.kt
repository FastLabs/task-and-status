package codec

import com.google.gson.reflect.TypeToken
import orchestration.OrchestrationEvent
import task.TaskInstance
import task.TaskSpec


object TaskInstanceCodec : DataClassCodec<TaskInstance>(TypeToken.get(TaskInstance::class.java))
object TaskSpecCodec : DataClassCodec<TaskSpec>(TypeToken.get(TaskSpec::class.java))
object OrchestrationEventCodec : DataClassCodec<OrchestrationEvent>(TypeToken.get(OrchestrationEvent::class.java))
private object taskSpecListType : TypeToken<List<TaskSpec>>()
private object taskInstLstType : TypeToken<List<TaskInstance>>()
object TaskSpecListCodec : DataClassCodec<List<TaskSpec>>(taskSpecListType)
object TaskInstListCodec : DataClassCodec<List<TaskInstance>>(taskInstLstType)

val appCodecList = listOf( OrchestrationEventCodec, TaskInstanceCodec)