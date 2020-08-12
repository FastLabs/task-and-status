package data

import task.taskDef
import orchestration.RouteTaskAction
import task.TaskAttributeDefinition
import task.attr


private val newT0 = taskDef {
    id = "T0"
    description = "DB-BIG-UK-PARENT"
    attributes(attr {
        id = "T1"
        action(RouteTaskAction("ETL_SERVICE"))
        attributes(attr {
            name = "region"
            mandatory = true
        }, attr { name = "country" })
    }
    )
    subTasks(
            {
                id = "T1"
                action(RouteTaskAction("ETL_SERVICE"))
                attributes(attr {
                    name = "region"
                    mandatory = true
                }, attr { name = "country" })
            },
            {
                id = "T2"
                action(RouteTaskAction("ETL_SERVICE"))
            })
    attributes(TaskAttributeDefinition("cobDate", true))

}