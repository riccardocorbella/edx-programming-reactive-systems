package ch.epfl.lamp

import sbt._

/**
  * Settings shared by the student build and the teacher build
  */
object MOOCPlugin extends AutoPlugin {

  object autoImport {
    val course = SettingKey[String]("course")

    val assignment = SettingKey[String]("assignment")

    val courseId = SettingKey[String]("courseId")

    val commonSourcePackages = SettingKey[Seq[String]]("commonSourcePackages")

    lazy val scalaTestDependency = "org.scalatest" %% "scalatest" % "3.0.5"
  }

  override def trigger = allRequirements

}
