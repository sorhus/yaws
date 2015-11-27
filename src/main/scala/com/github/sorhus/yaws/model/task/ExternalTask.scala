package com.github.sorhus.yaws.model.task

import com.github.sorhus.yaws.model.target.Target
import java.time.{Instant, Duration}

class ExternalTask(val name: String, val targets: List[Target], val dependencies: List[Task], interval: Duration)
  extends Task {

  var lastCheck: Instant = Instant.EPOCH

  def run: Boolean = {
    println(s"$name running")
    interval.getSeconds
    if(Duration.between(lastCheck, Instant.now()).getSeconds > interval.getSeconds) {
      println(s"$name cheking targets")
      lastCheck = Instant.now()
      targets.forall(_.isFulfilled)
    } else {
      println(s"$name not checking targets")
      false
    }
  }

}

object ExternalTask {
  // TODO more convenience methods needed
  def apply(name: String, target: Target) = new ExternalTask(name, target :: Nil, Nil, Duration.ofSeconds(60))
  def apply(name: String, target: Target, dependency: Task) = new ExternalTask(name, target :: Nil, dependency :: Nil, Duration.ofSeconds(0))
  def apply(name: String, target: Target, dependencies: List[Task]) = new ExternalTask(name, target :: Nil, dependencies, Duration.ofSeconds(0))
  def apply(name: String, targets: List[Target], dependencies: List[Task]) = new ExternalTask(name, targets, dependencies, Duration.ofSeconds(0))
}