package com.github.meandor

import org.scalatest.{FeatureSpec, Matchers}

class ConfigsTest extends FeatureSpec with Matchers {
  feature("Load config properties") {
    scenario("existing property from config file") {
      Configs.value("baz").get shouldBe "bob"

      Configs.value("foo.bar").get shouldBe "baz"
    }

    scenario("non existing property") {
      Configs.value("foobar").isEmpty shouldBe true
    }
  }
}
