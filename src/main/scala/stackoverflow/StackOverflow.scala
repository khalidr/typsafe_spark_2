package stackoverflow

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import annotation.tailrec
import scala.reflect.ClassTag

/** A raw stackoverflow posting, either a question or an answer */
case class Posting(postingType: Int, id: Int, acceptedAnswer: Option[Int], parentId: Option[QID], score: Int, tags: Option[String]) extends Serializable


/** The main class */
object StackOverflow extends StackOverflow {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {

    val lines   = sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")
    val raw     = rawPostings(lines)

    println("************** grouping *****************")
    val grouped = groupedPostings(raw)


    println("************** scoring *****************")
    val scored  = scoredPostings(grouped).sample(true, 0.1, 0)

/*
    val t:List[(Question, HighScore)] = scored.collect().toList

    assert (t.contains((Posting(1, 6,   None, None, 140, Some("CSS")),  67)))
    assert (t.contains((Posting(1, 42,  None, None, 155, Some("PHP")),  89)))
    assert (t.contains((Posting(1, 72,  None, None, 16,  Some("Ruby")), 3)))
    assert (t.contains((Posting(1, 126, None, None, 33,  Some("Java")), 30)))
    assert (t.contains((Posting(1, 174, None, None, 38,  Some("C#")),   20)))
*/

    println("************** vectors *****************")
    val vectors = vectorPostings(scored)
/*

    val tuples = vectors.collect()
    assert(tuples.toList.contains((350000,67)))
    assert(tuples.toList.contains((100000, 89)))
    assert(tuples.toList.contains((300000, 3)))
    assert(tuples.toList.contains((50000,  30)))
    assert(tuples.toList.contains((200000, 20)))
    assert(vectors.count() == 2121822, "Incorrect number of vectors: " + vectors.count())
*/

    println("************** kmeans *****************")
   val means = kmeans(sampleVectors(vectors), vectors, debug = true)

    println("************** results *****************")
    val results = clusterResults(means, vectors)
    printResults(results)
  }
}


/** The parsing and kmeans methods */
class StackOverflow extends Serializable {

  /** Languages */
  val langs =
    List(
      "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
      "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
  def langSpread = 50000
  assert(langSpread > 0, "If langSpread is zero we can't recover the language from the input data!")

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 45

  /** K-means parameter: Convergence criteria */
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120


  //
  //
  // Parsing utilities:
  //
  //

  /** Load postings from the given file */
  def rawPostings(lines: RDD[String]): RDD[Posting] ={

    lines.map(line => {
      val arr = line.split(",")
      Posting(postingType =    arr(0).toInt,
        id =             arr(1).toInt,
        acceptedAnswer = if (arr(2) == "") None else Some(arr(2).toInt),
        parentId =       if (arr(3) == "") None else Some(arr(3).toInt),
        score =          arr(4).toInt,
        tags =           if (arr.length >= 6) Some(arr(5).intern()) else None)
    })
  }


  /** Group the questions and answers together */
  def groupedPostings(postings: RDD[Posting]): RDD[(QID, Iterable[(Question, Answer)])] = {

    //questions
    val questions:RDD[(QID, Question)] = postings
      .filter(_.postingType == 1)
        .map(posting ⇒ posting.id → posting)

    val answers = postings.filter{_.postingType == 2}.map { posting ⇒ posting.parentId.get → posting }

    val joined = questions.join(answers).map{case(qid, qAndA) ⇒
      qid → Iterable(qAndA)
    }

    //joined.groupByKey()
    joined.reduceByKey{_ ++ _}

  }


  /** Compute the maximum score for each posting */
  def scoredPostings(grouped: RDD[(QID, Iterable[(Question, Answer)])]): RDD[(Question, HighScore)] = {

    def answerHighScore(as: Array[Answer]): HighScore = {
      var highScore = 0
          var i = 0
          while (i < as.length) {
            val score = as(i).score
                if (score > highScore)
                  highScore = score
                  i += 1
          }
      highScore
    }

    grouped.map { case (x, questionsAndAnswers ) ⇒
      questionsAndAnswers.head._1 → answerHighScore(questionsAndAnswers.map(_._2).toArray)
    }

  }


  /** Compute the vectors for the kmeans */
  def vectorPostings(scored: RDD[(Question, HighScore)]): RDD[(LangIndex, HighScore)] = {
    /** Return optional index of first language that occurs in `tags`. */
    def firstLangInTag(tag: Option[String], ls: List[String]): Option[Int] = {
      if (tag.isEmpty) None
      else if (ls.isEmpty) None
      else if (tag.get == ls.head) Some(0) // index: 0
      else {
        val tmp = firstLangInTag(tag, ls.tail)
        tmp match {
          case None => None
          case Some(i) => Some(i + 1) // index i in ls.tail => index i+1
        }
      }
    }

    /*val p = scored.zipWithIndex().flatMap{case ((question, highScore), idx) ⇒
      firstLangInTag(question.tags, langs).map { x ⇒ (idx, (x * langSpread, highScore))}
    }.sortByKey().values*/

    val p = scored.flatMap{case (question, highScore) ⇒
      firstLangInTag(question.tags, langs).map { x ⇒ (x * langSpread, highScore)}
    }
    p.cache()

    p

  }


  /** Sample the vectors */
  def sampleVectors(vectors: RDD[(LangIndex, HighScore)]): Array[(Int, Int)] = {

    assert(kmeansKernels % langs.length == 0, "kmeansKernels should be a multiple of the number of languages studied.")
    val perLang = kmeansKernels / langs.length

    // http://en.wikipedia.org/wiki/Reservoir_sampling
    def reservoirSampling(lang: Int, iter: Iterator[Int], size: Int): Array[Int] = {
      val res = new Array[Int](size)
      val rnd = new util.Random(lang)

      for (i <- 0 until size) {
        assert(iter.hasNext, s"iterator must have at least $size elements")
        res(i) = iter.next
      }

      var i = size.toLong
      while (iter.hasNext) {
        val elt = iter.next
        val j = math.abs(rnd.nextLong) % i
        if (j < size)
          res(j.toInt) = elt
        i += 1
      }

      res
    }

    val res =
      if (langSpread < 500)
        // sample the space regardless of the language
        vectors.takeSample(false, kmeansKernels, 42)
      else
        // sample the space uniformly from each language partition
        vectors.groupByKey.flatMap({
          case (lang, vectors) => reservoirSampling(lang, vectors.toIterator, perLang).map((lang, _))
        }).collect()

    assert(res.length == kmeansKernels, res.length)
    res
  }


  //
  //
  //  Kmeans method:
  //
  //

  def classify(means:Array[(Int, Int)], vectors:RDD[(Int, Int)])/*: Map[Int, Iterable[(LangIndex, HighScore)]]*/ = {
    /*val vectorsWithClosestMeanIndex = vectors.map{ point ⇒
      findClosest(point, means) → point
    }

    vectors.groupBy{ findClosest(_, means)}

    val vectorsGroupedByMeans = vectorsWithClosestMeanIndex
      .aggregateByKey(List.empty[(Int, Int)]) ({ _ :+ _ }, { _ ++ _ })
        .collect()
        .toMap

    means.zipWithIndex.map { case (mean, idx) ⇒ idx → vectorsGroupedByMeans.getOrElse(idx, List())
    }*/

    /*vectors
        .map(i ⇒ (findClosest(i, means), i))
        .aggregateByKey(List.empty[(Int, Int)])({ _ :+ _ },{ _ ++ _ })
        .collect()
        .toMap*/


    /*vectors
      .map(i ⇒ (findClosest(i, means), i))
      .aggregateByKey(List.empty[(Int, Int)])({ _ :+ _ },{ _ ++ _ })
      .collect()
      .toMap*/

/*        vectors
          .mapPartitions({itr ⇒ itr.map(i ⇒ (findClosest(i, means), i) )}, true)
            .aggregateByKey(List.empty[(Int, Int)])({ _ :+ _ },{ _ ++ _ })
            .collect()
            .toMap*/

    vectors
      .map(i ⇒ (findClosest(i, means), Iterable(i)))
      .reduceByKey(_ ++ _)
      //.aggregateByKey(List.empty[(Int, Int)])({ _ :+ _ },{ _ ++ _ })

  }


  /** Main kmeans computation */
  @tailrec final def kmeans(means: Array[(Int, Int)], vectors: RDD[(Int, Int)], iter: Int = 1, debug: Boolean = false): Array[(Int, Int)] = {

    val newMeans = means.clone()

    //classify
    val classified = classify(means, vectors)


    //avg
    val avg = classified.mapValues{ averageVectors }

    //update old means
    avg.collect().foreach{case (idx, newMean) ⇒ newMeans.update(idx, newMean)}

/*
    val newMeans = associated.aggregateByKey(((0,0), 0))(
      {case (((accumulatedIdx, accumulatedHighScore), vectorCount), (idx, score)) ⇒
          val newIdx = accumulatedIdx + idx
          val newScore = accumulatedHighScore + score

        ((newIdx, newScore), vectorCount + 1)

      }, {case (((idx1, score1), count1),((idx2, score2), count2)) ⇒ ((idx1 + idx2, score1 + score2), count1 + count2)})
      .mapValues { case ((totalIdx, totalScore), count) ⇒
        (totalIdx / count) → (totalScore / count)
      }
      .values
      .collect()*/

    /*val x =
      vectors
        .aggregateByKey(List.empty[(Int, Int)]) ({ _ :+ _}, {_ ++ _})
        //.map{case (mean, vectors) ⇒ averageVectors(vectors)}
        .collect()
*/
    //val newMeans = associated.reduceByKey{ case ((idx1, score1), (idx2, score2) ) ⇒ (idx1 + idx2) → score1 + score2 / 2 } //means.clone() // you need to compute newMeans

    // TODO: Fill in the newMeans array
    val distance = euclideanDistance(means, newMeans)

    if (debug) {
      println(s"""Iteration: $iter
                 |  * current distance: $distance
                 |  * desired distance: $kmeansEta
                 |  * means:""".stripMargin)
      for (idx <- 0 until kmeansKernels)
      println(f"   ${means(idx).toString}%20s ==> ${newMeans(idx).toString}%20s  " +
              f"  distance: ${euclideanDistance(means(idx), newMeans(idx))}%8.0f")
    }

    if (converged(distance))
      newMeans
    else if (iter < kmeansMaxIterations)
      kmeans(newMeans, vectors, iter + 1, debug)
    else {
      if (debug) {
        println("Reached max iterations!")
      }
      newMeans
    }
  }




  //
  //
  //  Kmeans utilities:
  //
  //

  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double) =
    distance < kmeansEta


  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2
  }

  /** Return the euclidean distance between two points */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
    assert(a1.length == a2.length)
    var sum = 0d
    var idx = 0
    while(idx < a1.length) {
      sum += euclideanDistance(a1(idx), a2(idx))
      idx += 1
    }
    sum
  }

  /** Return the closest point */
  def  findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centers.length) {
      val tempDist = euclideanDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }


  /** Average the vectors */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
    val iter = ps.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while (iter.hasNext) {
      val item = iter.next
      comp1 += item._1
      comp2 += item._2
      count += 1
    }
    ((comp1 / count).toInt, (comp2 / count).toInt)
  }




  //
  //
  //  Displaying results:
  //
  //
  def clusterResults(means: Array[(Int, Int)], vectors: RDD[(LangIndex, HighScore)]): Array[(String, Double, Int, Int)] = {
    val closest = vectors.map(p => (findClosest(p, means), p))
    val closestGrouped = closest.groupByKey()

    val median = closestGrouped.mapValues { vs =>
      val mostCommonLangIdx = vs.maxBy(_._2)
      val langLabel: String   = langs(mostCommonLangIdx._1/langSpread)
      val value:Double = mostCommonLangIdx._2.toDouble / langSpread.toDouble
      // most common language in the clusterexit
      val langPercent: Double = (value.toDouble / vs.size.toDouble ) * 100 // percent of the questions in the most common language
      val clusterSize: Int    = vs.size
      val medianScore: Int    = {
        val unsorted = vs.map(_._2).toList
        val sorted = unsorted.sortWith(_ < _)

        if(sorted.size % 2 > 0) sorted(sorted.size / 2)
        else {
          val (a, b) = sorted.splitAt(sorted.size / 2)
          (a.last + b.head) / 2
        }
      }

      (langLabel, langPercent, clusterSize, medianScore)
    }

    median.collect().map(_._2).sortBy(_._4)
  }

  def printResults(results: Array[(String, Double, Int, Int)]): Unit = {
    println("Resulting clusters:")
    println("  Score  Dominant language (%percent)  Questions")
    println("================================================")
    for ((lang, percent, size, score) <- results)
      println(f"${score}%7d  ${lang}%-17s (${percent}%-5.1f%%)      ${size}%7d")
  }
}
