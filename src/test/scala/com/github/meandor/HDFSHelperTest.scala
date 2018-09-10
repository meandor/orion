package com.github.meandor

import java.io.File
import java.nio.file.{Files, Paths}

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

  feature("clean up old generations") {
    scenario("keep only 3 generations and no generation existing") {
      HDFSHelper.cleanUpGenerations(localHDFS, "/noCleanUpGen", 3) shouldBe Seq()
    }

    scenario("keep only 3 generations and n generation existing") {
      HDFSHelper.cleanUpGenerations(localHDFS, "/noCleanUpGenN", 3) shouldBe Seq()
    }

    scenario("keep only 2 generations and 3 generation existing") {
      val basePath = "src/test/resources/cleanUpGen"
      try {
        Files.createDirectories(Paths.get(s"$basePath/0002"))
        Files.createFile(Paths.get(s"$basePath/0002/_SUCCESS"))
      } catch {
        case _: Exception =>
      }

      val deletedFolders = HDFSHelper.cleanUpGenerations(localHDFS, "/cleanUpGen", 2)
      deletedFolders.length shouldBe 1
      deletedFolders.head should endWith(s"$basePath/0002")

      val tmpBasePath = new File(basePath)
      tmpBasePath.list().toSeq should contain theSameElementsAs Seq("0003", "0004", "_SUCCESS")
    }
  }
}
