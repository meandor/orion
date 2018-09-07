package com.github.meandor

import com.typesafe.config.{Config, ConfigFactory}

object Configs {
  val configFile: Config = ConfigFactory.load("app.conf")

  def value(property: String): Option[String] = {
    try {
      Option(configFile.getString(property))
    } catch {
      case e: Exception => None
    }
  }
}
