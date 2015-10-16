package com.github.sorhus.yaws.model.task

import com.github.sorhus.yaws.model.target.Target

class YarnTask(val task: Task) extends Task {

  def command: String = ""

  var running: Boolean = false
  var done: Boolean = false

  override def run: Boolean = throw new RuntimeException

  override def isDone = done
  override def isCandidate = !running && super.isCandidate

  override val name: String = task.name
  override val dependencies: List[Task] = task.dependencies
  override val targets: List[Target] = task.targets
}
