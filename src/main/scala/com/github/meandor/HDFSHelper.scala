package com.github.meandor

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object HDFSHelper {
  val basePath: String = Configs.value("hdfs.basePath").get

  def latestGeneration(fileSystem: FileSystem, path: String): String = {
    val fullPath = new Path(basePath + path)
    val generations = fileSystem.listStatus(fullPath)
      .filter(status => status.isDirectory)
      .map(_.getPath.toString.split("/").last)
      .filter(s => s.matches("^\\d+$"))

    if (generations.isEmpty) {
      return "0000"
    }

    generations.max
  }

  def defaultHDFS(): FileSystem = {
    val hdfsConf = new Configuration()
    hdfsConf.set("fs.defaultFS", basePath)
    FileSystem.get(hdfsConf)
  }

  def nextGeneration(fileSystem: FileSystem, path: String): String = {
    val currentGeneration = latestGeneration(fileSystem, path)
    "%04d".format(currentGeneration.toInt + 1)
  }

  def cleanUpGenerations(fileSystem: FileSystem, path: String, noToKeep: Int): Seq[String] = {
    val fullPath = new Path(basePath + path)
    val generations = fileSystem.listStatus(fullPath)
      .filter(status => status.isDirectory)
      .map(_.getPath.toString)
      .filter(s => s.matches("^.*/\\d{4}$"))
      .sorted

    val toBeDeleted = generations.take(generations.length - noToKeep)
    toBeDeleted.foreach(pathString => {
      val path = new Path(pathString)
      fileSystem.delete(path, true)
    })
    toBeDeleted
  }
}
