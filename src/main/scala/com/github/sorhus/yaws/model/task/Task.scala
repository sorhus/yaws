package com.github.sorhus.yaws.model.task

import com.github.sorhus.yaws.model.target.Target

abstract class Task {

  val name: String
  val targets: List[Target]
  val dependencies: List[Task]

  def run: Boolean

  def isCandidate: Boolean = {
    val isCandidate = dependencies.forall(_.isDone)
    println(s"$name isCandidate? $isCandidate")
    isCandidate
  }

  def isDone: Boolean = {
    val isDone = targets.forall(_.isFulfilled)
    println(s"$name isDone? $isDone")
    isDone
  }

  override def toString: String = name

}
