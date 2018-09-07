package com.github.meandor

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocalFileSystem}
import org.scalatest.{FeatureSpec, Matchers}

class HDFSHelperTest extends FeatureSpec with Matchers {

  val localHDFS: LocalFileSystem = FileSystem.getLocal(new Configuration())

  feature("get latest generation in path") {
    scenario("one generation already existing") {
      HDFSHelper.latestGeneration(localHDFS, "/foo") shouldBe "0001"
    }

    scenario("nothing existing yet") {
      HDFSHelper.latestGeneration(localHDFS, "/nofoo") shouldBe "0000"
    }
  }

  feature("get next generation in path") {
    scenario("one generation already existing") {
      HDFSHelper.nextGeneration(localHDFS, "/foo") shouldBe "0002"
    }

    scenario("nothing existing yet") {
      HDFSHelper.nextGeneration(localHDFS, "/nofoo") shouldBe "0001"
    }
  }
}
