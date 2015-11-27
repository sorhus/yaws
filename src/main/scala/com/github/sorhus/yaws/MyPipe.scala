package com.github.sorhus.yaws

import com.github.sorhus.yaws.model.target.URITarget
import com.github.sorhus.yaws.model.task.{YarnTask, ExternalTask, Task}
import com.github.sorhus.yaws.model.YawsPipe
import com.twitter.scalding.Args

class MyPipe(name: String, args: Args) extends YawsPipe(name, args) with App {

  run()

  override def topLevelTasks: List[Task] = {
    val top: Task = {
      val t1: CreateFileTask = new CreateFileTask("file:///home/anton/yaws/t1")
//      val t2: ExternalTask = ExternalTask("t2", URITarget("file:///home/anton/yaws/t2"), Nil)
//      val t3 = new YarnTask("t3", URITarget("file:///home/anton/yaws/t3") :: Nil, t2 :: Nil, "touch /home/anton/yaws/t3")
//      new CreateFileTask("file:///home/anton/yaws/top", t1 :: t2 :: t3 :: Nil)
      t1
    }
    top :: Nil
  }

}
