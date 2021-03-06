package com.accenture.bootcamp

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

import scala.language.postfixOps
import Utils._
import org.apache.spark.sql.functions.col

import org.apache.spark.sql.SparkSession






object App extends SparkSupport {


  def unoptimalCode1(text: RDD[String]): (Array[String], Array[String]) = {

    // ignore empty lines
    val nonEmptyLines = text.filter(_.nonEmpty)

    // get first tab separated token from each line (code)
    val codes = nonEmptyLines.map(_.split("\t").head)

    // codes grouped by 2 first characters
    val groupedCodes = codes.groupBy(_.substring(0, 2))

    // compute sizes of all groups
    val groupSizes = groupedCodes.map { case (group, members) => (group, members.size) }

    // sort groups by sizes descending
    val sortedGroups = groupSizes.sortBy(_._2, ascending = false)

    // get 3 groups with most members
    val top3Groups = sortedGroups.map(_._1).take(3)

    // sort groups by sizes ascending
    val sortedGroupsAsc = groupSizes.sortBy(_._2)

    // get 3 groups with least members
    val bottom3Groups = sortedGroupsAsc.map(_._1).take(3)

    (top3Groups, bottom3Groups)
  }

  // TODO: Improve unoptimalCode1. Implement and test optimalCode1
  // Requirement: Write more time efficient code
  // Hint: you probably want to use reduceByKey.
  // Hint2: what about persistence?
  // Hint3: Any other way how to get results?
  // Hint4: use sc.parallelize() when providing data for tests (for unit testing)
  def optimalCode1(text: RDD[String]): (Array[String], Array[String]) = {

    // ignore empty lines
    val nonEmptyLines = text.filter(_.nonEmpty).cache()

    // get first tab separated token from each line (code)
    val codes = nonEmptyLines.map(_.split("\t").head)

    // codes grouped by 2 first characters
    val groupedCodes = codes.groupBy(_.substring(0, 2)).cache()

    // compute sizes of all groups
    val groupSizes = groupedCodes.map(x => (x._1, 1)).reduceByKey(_+_)

    // sort groups by sizes descending
    val sortedGroups = groupSizes.sortBy(_._2, ascending = false)

    // get 3 groups with most members
    val top3Groups = sortedGroups.map(_._1).take(3)

    // sort groups by sizes ascending
    val sortedGroupsAsc = groupSizes.sortBy(_._2)

    // get 3 groups with least members
    val bottom3Groups = sortedGroupsAsc.map(_._1).take(3)

    (top3Groups, bottom3Groups)
  }

  // TODO: Rewrite optimalCode1 using aggregateByKey() instead of reduceByKey. Implement and test
  def optimalCode11(text: RDD[String]): (Array[String], Array[String]) = {

    // ignore empty lines
    val nonEmptyLines = text.filter(_.nonEmpty).cache()

    // get first tab separated token from each line (code)
    val codes = nonEmptyLines.map(_.split("\t").head)

    // codes grouped by 2 first characters
    val groupedCodes = codes.groupBy(_.substring(0, 2)).cache()

    val groupSizes = groupedCodes.map(x => (x._1, 1)).aggregateByKey(0)(_+_,_+_)

    // sort groups by sizes descending
    val sortedGroups = groupSizes.sortBy(_._2, ascending = false)

    // get 3 groups with most members
    val top3Groups = sortedGroups.map(_._1).take(3)

    // sort groups by sizes ascending
    val sortedGroupsAsc = groupSizes.sortBy(_._2)

    // get 3 groups with least members
    val bottom3Groups = sortedGroupsAsc.map(_._1).take(3)

    (top3Groups, bottom3Groups)
  }

  def unoptimalCode2(crimesDb: RDD[String], commitedCrimes: RDD[String]): Unit = {

    case class Crime(code: String, code2: String, category: String, subcategory: String, level: String)
    case class CommitedCrime(cdatetime: String, address: String, district: String, beat: String, grid: String, crimedescr: String, ucr_ncic_code: String, latitude: String, longitude: String)

    // ignore empty lines
    val nonEmptyLines = crimesDb.filter(_.nonEmpty)

    // create RDD[Crime]
    val crimes = nonEmptyLines.map(line => {
      val cols = line.split("\t")
      Crime(cols(0), cols(1), cols(2), cols(3), cols(4))
    })

    var idx = 0

    // This function does processing and saving of data
    def addCommitedCrimes(commited: RDD[String]) = {

      // Map commited crimes with it's codes
      val codesCommited = commited.map(line => {
        val cols = line.split(",")
        // column 6 contains code
        (cols(6), CommitedCrime(cols(0), cols(1), cols(2), cols(3), cols(4), cols(5), cols(6), cols(7), cols(8)))
      })

      // combine each CommitedCrime with corresponding Crime by it's code
      val joinedCrimes = crimes.map(crime => (crime.code, crime)).join(codesCommited)

      // Store files in FS.
      joinedCrimes.map { case (_, (crime, commitedCrime)) => (commitedCrime.district, crime.category) }
        .reduceByKey(_ + "," + _)
        .saveAsTextFile("output/" + System.nanoTime() + "_output" + idx)
      idx += 1
    }

    // Code below simulates situation when new data comes in portions.
    // Think of it like each day you receive new data and need to process it and save the result
    val commitedCrimesParts = commitedCrimes.randomSplit(Array(.2, .2, .2, .2, .2))
    // 1st day data
    addCommitedCrimes(commitedCrimesParts(0))
    // 2nd day data
    addCommitedCrimes(commitedCrimesParts(1))
    // 3rd day data
    addCommitedCrimes(commitedCrimesParts(2))
    // 4th day data
    addCommitedCrimes(commitedCrimesParts(3))
    // 5th day data
    addCommitedCrimes(commitedCrimesParts(4))
  }

  // TODO: Improve unoptimalCode2. Implement and test optimalCode2
  // Requirement: Write more time efficient code
  // Hint: Use range partitioner
  // Hint1: Are there any other improvements?
  // Hint2: Do you need to persist something?
  def optimalCode2(crimesDb: RDD[String], commitedCrimes: RDD[String]): Unit = {

    // TODO: pre process crimesDB here

    // I will test if datafames will do better than rdd performing joins and spark.sql queries

    case class Crime(code: String, code2: String, category: String, subcategory: String, level: String)
    case class CommitedCrime(cdatetime: String, address: String, district: String, beat: String, grid: String, crimedescr: String, ucr_ncic_code: String, latitude: String, longitude: String)

    // ignore empty lines
    val nonEmptyLines = crimesDb.filter(_.nonEmpty)

    // create RDD[Crime]
    val crimes = nonEmptyLines.map(line => {
      val cols = line.split("\t")
      Crime(cols(0), cols(1), cols(2), cols(3), cols(4))
    })

    // creating crimes classification dataframe
/*    val crimeswCols = crimes.map(crime => (crime.code, crime.code2, crime.category, crime.subcategory, crime.level))


    val dfCrimes = spark.createDataFrame(crimeswCols).toDF("id", "a1", "a2", "a3", "a4").persist()*/

    val crimeswCols = crimes.map(crime => (crime.code, crime.category))


    val dfCrimes = spark.createDataFrame(crimeswCols).toDF("id", "a2").persist()



    var idx = 0

    // This function does processing and saving of data
    def addCommitedCrimes(commited: RDD[String]) = {

      // TODO: join commitedCrimes with Crimes DB by code
      // TODO: for each district create list of categories of commited crimes
      // TODO: resulting RDD assign to result value


      // Map commited crimes with it's codes
      val codesCommited = commited.map(line => {
        val cols = line.split(",")
        // column 6 contains code
        (CommitedCrime(cols(0), cols(1), cols(2), cols(3), cols(4), cols(5), cols(6), cols(7), cols(8)))
      })

      /*val committedwCols = codesCommited.map(commitedCrime =>
        (commitedCrime.ucr_ncic_code, commitedCrime.cdatetime, commitedCrime.address,
          commitedCrime.district, commitedCrime.beat, commitedCrime.grid, commitedCrime.crimedescr,
          commitedCrime.ucr_ncic_code, commitedCrime.latitude, commitedCrime.longitude))

      // creating commited crimes dataframe
      val dfCommitted2 = spark.createDataFrame(committedwCols).toDF("id", "b1", "b2", "b3", "b4", "b5", "b6", "b7", "b8", "b9")
      */

      val committedwCols = codesCommited.map(commitedCrime =>
        (commitedCrime.ucr_ncic_code, commitedCrime.district))

      // creating commited crimes dataframe
      val dfCommitted2 = spark.createDataFrame(committedwCols).toDF("id", "b3")

      /*val dfCommitted = dfCommitted2.repartition(5).persist() // there are 6 districts with evenly split records but results were slower than with 2*/
      val dfCommitted = dfCommitted2.repartitionByRange(6, col("b3"))
      // println(dfCommitted.columns)

      val joinedCrimes = dfCommitted.join(dfCrimes, "id")
      //println(joinedCrimes.show())

      joinedCrimes.createOrReplaceTempView("crimesTemp")


      val resultdf = spark.sql("""
        SELECT b3 AS district, a2 AS category, COUNT(id)
        FROM crimesTemp
        GROUP BY district, category
        """
      )
      val result = resultdf.rdd

       /**/
      // Store files in FS.
      result.saveAsTextFile("output/" + System.nanoTime() + "_output" + idx)
      idx += 1

      //print(resultdf.show())

    }


    // DO NOT CHANGE CODE BELOW!
    // Code below simulates situation when new data comes in portions.
    // Think of it like each day you receive new data and need to process it and save the result
    val commitedCrimesParts = commitedCrimes.randomSplit(Array(.2, .2, .2, .2, .2))
    // 1st day data
    addCommitedCrimes(commitedCrimesParts(0))
    // 2nd day data
    addCommitedCrimes(commitedCrimesParts(1))
    // 3rd day data
    addCommitedCrimes(commitedCrimesParts(2))
    // 4th day data
    addCommitedCrimes(commitedCrimesParts(3))
    // 5th day data
    addCommitedCrimes(commitedCrimesParts(4))
  }


  def main(args: Array[String]): Unit = {


    // read text into RDD
    val crimeCategories:RDD[String] = sc.textFile(filePath("ucr_ncic_codes.tsv"))
    println("Task #2")

    val commitedCrimes = sc.textFile(filePath("SacramentocrimeJanuary2006.csv"))

   // val t0 = System.nanoTime()

    val (t1, any) = time {
      unoptimalCode2(crimeCategories, commitedCrimes)
    }

    //val d1 = System.nanoTime() - t0

    //println(s"""( ${ top.mkString(",") } ... ${ bottom.reverse.mkString(",") })""")



    //val t1 = System.nanoTime()

    val (t2, r2) = time {
      optimalCode2(crimeCategories, commitedCrimes)
    }

    //val d2 = System.nanoTime() - t1

    println()
    println("Performance difference between initial and optimised code is ")
    println((t1 - t2)/1e+9 + " seconds")






    // TODO: check perfromance optimalCode1, optimalCode11
    // TODO: check performance unoptimalCode2, optimalCode2
    /*

    optimalCode1:  ~ 650000000 ns improvement after applying reduceByKey
                   ~ 750000000 ns improvement after applying cache()
                   no significant changes after changing reduceByKey to aggregateByKey


    optimalCode2:  joining Spark DataFrames and running sql query had significantly slower
                   performance than RDD operations. Time difference ~ -23 seconds.
                   When I applied repartitionByRange difference decreased to -9 seconds.
                   Adding persist() to dataframes did not improve the results.

     */

  }
}

