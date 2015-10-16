package com.github.sorhus.yaws

import java.io.File
import java.net.URI

import com.github.sorhus.yaws.model.target.{URITarget, Target}
import com.github.sorhus.yaws.model.task.Task

class CreateFileTask(path: String, val dependencies: List[Task] = Nil) extends Task {

  val name = path.split("/").last

  def run(): Boolean = {
    println(s"$path running");
    new File(new URI(path)).createNewFile();
    true
  }

  val targets: List[Target] = URITarget(path) :: Nil
}

