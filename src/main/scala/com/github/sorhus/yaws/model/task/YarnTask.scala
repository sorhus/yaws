package com.github.sorhus.yaws.model.task

import com.github.sorhus.yaws.model.target.Target

class YarnTask(val name: String,
               val targets: List[Target],
               val dependencies: List[Task],
               val command: String) extends Task {

  var running: Boolean = false
  var done: Boolean = false

  override def run: Boolean = throw new RuntimeException

  override def isDone = done
  override def isCandidate = !running && super.isCandidate

}
