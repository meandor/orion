import org.scalatest.{FeatureSpec, Matchers}

class LibrarySuite extends FeatureSpec with Matchers {
  feature("someLibraryMethod is always true") {
    scenario("test") {
      val library = new Library()

      library.someLibraryMethod shouldBe true
    }
  }
}
