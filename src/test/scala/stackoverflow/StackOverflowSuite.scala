package stackoverflow

import org.apache.spark.rdd.RDD
import org.junit.runner.RunWith
import org.scalatest.Matchers._
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import stackoverflow.StackOverflow._


@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FlatSpec with BeforeAndAfterAll {


  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120
  }

  val postingsList = Seq(
    Posting(1, 22, None, None, 2, None), //question
    Posting(2, 23, Some(100), Some(22), 2, None), // answer
    Posting(2, 24, Some(101), Some(22), 3, None), // answer
    Posting(2, 25, Some(102), Some(22), 4, None), // answer
    Posting(1, 55, None, None, 2, None), //question
    Posting(2, 26, Some(103), Some(55), 2, None), // answer
    Posting(2, 27, Some(104), Some(55), 3, None), // answer
    Posting(1, 56, None, None, 2, None) //question - no answer
  )



  "StackOverflow"  should "testObject can be instantiated" in {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  "groupPostings" should  "return an empty list with an RDD with a single question but no answer" in {

    val postings = sc.parallelize(Seq(Posting(1, 22, None, None, 2, None)))

    val g = grouped(postings)

    assert(g === List.empty)
  }

  it should "show the correct results " in {
    val rdd = sc.parallelize(List(0 → 9172, 0 → 10, 1 → 10, 2 → 4))


  }

  it should "'groupPostings' should return a question and answer for an rdd with a single question and answer" in {
    val question = Posting(1, 22, None, None, 2, None)
    val answer = Posting(2, 23, Some(100), Some(22), 2, None)
    val postings =
      sc
        .parallelize(
          Seq(
            question, //question
            answer // answer
          )
        )

    val g = grouped(postings)

    assert(g == List((22, List(question -> answer))))
  }

  it should "'groupPostings' should return a list of questions and answers" in {

    val postings =
      sc
        .parallelize(postingsList)

    val g = grouped(postings)

    val expectedResult = List(
      22 → Iterable(
        Posting(1, 22, None, None, 2, None) → Posting(2, 23, Some(100), Some(22), 2, None ),
        Posting(1, 22, None, None, 2, None) → Posting(2, 24, Some(101), Some(22), 3, None ),
        Posting(1, 22, None, None, 2, None) → Posting(2, 25, Some(102), Some(22), 4, None )
      ),
      55 → Iterable(
        Posting(1, 55, None, None, 2, None) → Posting(2, 26, Some(103), Some(55), 2, None ),
        Posting(1, 55, None, None, 2, None) → Posting(2, 27, Some(104), Some(55), 3, None )
      )
    )

    g should contain  theSameElementsAs expectedResult

  }

  it should "return an empty list for one question with no answer" in {
    val posting = Posting(1, 22, None, None, 2, None)
    val postings = sc.parallelize(
      Seq(
        posting
      )
    )

    val g = groupedPostings(postings)

    val highScore = scoredPostings(g)

    highScore.collect shouldBe Seq.empty
  }

  it should "find the highest score for each question" in {
    val postings = sc.parallelize(postingsList)

    val expectedResult = Seq(Posting(1, 22, None, None, 2, None) → 4, Posting(1, 55, None, None, 2, None) → 3 )

    scoredPostings(groupedPostings(postings)).collect should contain theSameElementsAs  expectedResult
  }

  def grouped(rdd:RDD[Posting]): Seq[(QID, Iterable[(Question, Answer)])] = groupedPostings(rdd).collect.toList

  override protected def afterAll(): Unit = {
    sc.stop()
  }
}
