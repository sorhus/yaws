package com.github.sorhus.yaws.plumbing

import java.lang.reflect.Constructor

import com.github.sorhus.yaws.model.YawsPipe
import com.twitter.scalding.Args

object PipeRunner extends App {

  println(s"PipeRunner user: ${System.getProperty("user.name")}")

  val clazz: String = args(0)
//  val clazz: String = "com.github.sorhus.yaws.MyPipe"
  //  val pipeName: String = args(1)
  val pipeName: String = "pippe"
  //  val a: Array[String] = java.util.Arrays.copyOfRange(args, 2, args.length)
  val pipeArgs: Args = Args(java.util.Arrays.copyOfRange(args, 0, args.length))

  Class.forName(clazz).getConstructors.foreach(println)
  val pipeConstructor: Constructor[_] = Class.forName(clazz).getConstructor(classOf[String], classOf[Args])
  val pipe: YawsPipe = pipeConstructor.newInstance(pipeName, pipeArgs).asInstanceOf[YawsPipe]

  println(pipe)

  pipe.run

}
