package org.bhnte.parquet968

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.proto.ProtoWriteSupport
import org.apache.parquet.proto.utils.WriteUsingMR
import org.apache.spark.sql.{SparkSession, SparkSql}
import org.bhnte.test.{TestProto2, TestProto3}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class SparkTest extends FunSuite with Matchers with BeforeAndAfterAll {

  protected val javaTmpDir: String = System.getProperty("java.io.tmpdir")

  private var spark = SparkSession
    .builder()
    .master("local")
    .getOrCreate()

  override protected def afterAll(): Unit = {
    spark.close()
    super.afterAll()
  }
/*
  test("1.9.0: proto3 writes using old collections style") {

    val msg = TestProto3.Test3.newBuilder()
      .setIntSet(1)
      .addNonEmptyRepeated(1).addNonEmptyRepeated(1)
      .putNonEmptyMap(1, 1).putNonEmptyMap(2, 2)
      .build()
    val path = new WriteUsingMR().write(msg)

    val expectedRows =
      s"""+---------+------+-------------+----------------+--------+----------------+
         ||intNotSet|intSet|emptyRepeated|nonEmptyRepeated|emptyMap|     nonEmptyMap|
         |+---------+------+-------------+----------------+--------+----------------+
         ||     null|     1|           []|          [1, 1]|      []|[[1, 1], [2, 2]]|
         |+---------+------+-------------+----------------+--------+----------------+
         |""".stripMargin

    val rows = SparkSql.showString(spark.read.parquet(path.toString))
    println(rows)
    rows shouldBe expectedRows
  }
*/
  test("1.9.1: proto3 with new schema style") {

    val msg = TestProto3.Test3.newBuilder()
      .setIntSet(1)
      .addNonEmptyRepeated(1).addNonEmptyRepeated(1)
      .putNonEmptyMap(1, 1).putNonEmptyMap(2, 2)
      .build()

    val conf = new Configuration()
    ProtoWriteSupport.setWriteSpecsCompliant(conf, true)

    val path = new WriteUsingMR(conf).write(msg)

    val expectedRows =
      s"""+---------+------+-------------+----------------+--------+----------------+
         ||intNotSet|intSet|emptyRepeated|nonEmptyRepeated|emptyMap|     nonEmptyMap|
         |+---------+------+-------------+----------------+--------+----------------+
         ||     null|     1|         null|          [1, 1]|    null|[1 -> 1, 2 -> 2]|
         |+---------+------+-------------+----------------+--------+----------------+
         |""".stripMargin

    val rows = SparkSql.showString(spark.read.parquet(path.toString))
    println(rows)
    rows shouldBe expectedRows
  }

  test("1.9.1: proto2 with old schema style") {

    val msg = TestProto3.Test3.newBuilder()
      .setIntSet(1)
      .addNonEmptyRepeated(1).addNonEmptyRepeated(1)
      .putNonEmptyMap(1, 1).putNonEmptyMap(2, 2)
      .build()

    val path = new WriteUsingMR().write(msg)

    val expectedRows =
      s"""+---------+------+-------------+----------------+--------+----------------+
         ||intNotSet|intSet|emptyRepeated|nonEmptyRepeated|emptyMap|     nonEmptyMap|
         |+---------+------+-------------+----------------+--------+----------------+
         ||     null|     1|           []|          [1, 1]|      []|[[1, 1], [2, 2]]|
         |+---------+------+-------------+----------------+--------+----------------+
         |""".stripMargin

    val rows = SparkSql.showString(spark.read.parquet(path.toString))
    println(rows)
    rows shouldBe expectedRows
  }

  test("1.9.1: proto2 with new schema style") {

    val msg = TestProto2.Test2.newBuilder()
      .setIntSet(1)
      .addNonEmptyRepeated(1).addNonEmptyRepeated(1)
      .putNonEmptyMap(1, 1).putNonEmptyMap(2, 2)
      .build()

    val conf = new Configuration()
    ProtoWriteSupport.setWriteSpecsCompliant(conf, true)

    val path = new WriteUsingMR(conf).write(msg)

    val expectedRows =
      s"""+---------+------+-------------+----------------+--------+----------------+
         ||intNotSet|intSet|emptyRepeated|nonEmptyRepeated|emptyMap|     nonEmptyMap|
         |+---------+------+-------------+----------------+--------+----------------+
         ||     null|     1|         null|          [1, 1]|    null|[1 -> 1, 2 -> 2]|
         |+---------+------+-------------+----------------+--------+----------------+
         |""".stripMargin

    val rows = SparkSql.showString(spark.read.parquet(path.toString))
    println(rows)
    rows shouldBe expectedRows
  }

  test("1.9.1: proto3 with old schema style") {

    val msg = TestProto2.Test2.newBuilder()
      .setIntSet(1)
      .addNonEmptyRepeated(1).addNonEmptyRepeated(1)
      .putNonEmptyMap(1, 1).putNonEmptyMap(2, 2)
      .build()

    val path = new WriteUsingMR().write(msg)

    val expectedRows =
      s"""+---------+------+-------------+----------------+--------+----------------+
         ||intNotSet|intSet|emptyRepeated|nonEmptyRepeated|emptyMap|     nonEmptyMap|
         |+---------+------+-------------+----------------+--------+----------------+
         ||     null|     1|           []|          [1, 1]|      []|[[1, 1], [2, 2]]|
         |+---------+------+-------------+----------------+--------+----------------+
         |""".stripMargin

    val rows = SparkSql.showString(spark.read.parquet(path.toString))
    println(rows)
    rows shouldBe expectedRows
  }

}
