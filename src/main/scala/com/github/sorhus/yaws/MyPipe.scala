package com.github.sorhus.yaws

import com.github.sorhus.yaws.model.target.URITarget
import com.github.sorhus.yaws.model.task.{ExternalTask, Task}
import com.github.sorhus.yaws.model.YawsPipe
import com.twitter.scalding.Args

class MyPipe(name: String, args: Args) extends YawsPipe(name, args) {

  def getTopLevelTasks: List[Task] = {
    val top: CreateFileTask = {
      val t1: CreateFileTask = new CreateFileTask("file:///home/anton/yaws/t1")
      val t2: ExternalTask = ExternalTask("t2", URITarget("file:///home/anton/yaws/t2"), t1)

      new CreateFileTask("file:///home/anton/yaws/top", t1 :: t2 :: Nil)
    }
    top :: Nil
  }

}
