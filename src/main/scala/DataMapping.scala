import java.io.File
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

import scala.collection.immutable.HashMap

object DataMapping {
  // Triple Table schema
  case class Triple(sub: String, pred: String, obj: String)
  // 环境

  //sparkSession封装了SparkContext和SQLContext,并且在builder的getOrCreate方法中判断是否符合要求的SparkSession存在，有责是用，没有则进行创建
  val spark: SparkSession = Settings.sparkSession
  val _sc: SparkContext = spark.sparkContext
  import spark.implicits._


  def read(inputFile: String): DataFrame = {
    val tris = _sc.textFile(inputFile)
      .map(str => str.split("\\s+"))
      .map(p => Triple(p(0), p(1), p(2)))
      .toDF()
    tris
  }
  def getPreds(tri: DataFrame): Seq[String] = {
    val seq = tri.select("pred").distinct()
      .collect().map(row => row.toString()).toSeq
    seq
  }
  def getSO(tri: DataFrame): Seq[String] = {

    val seq = tri.select("sub").distinct().collect().map(row => row.toString()).toSet
      .++(tri.select("obj").distinct().collect().map(row => row.toString()).toSet)
      .toSeq
    seq
  }
  def dataMapping(inputFile: String, outputDIR: String): Unit = {
    var fileName = inputFile.substring(inputFile.lastIndexOf("/") + 1)
    val frame = read(inputFile)
    fileName = fileName.substring(0, fileName.lastIndexOf(".") )
    println("[inputFile] " + inputFile)
    println("[outputDIR] " + outputDIR)
    println("[process] " + fileName)
    frame.show()
    // 新想法【不会产生大量的对象】
    // 生成pred index
    // so index
    // 然后分别与原 frame 做 join操作。
    // ===== pred
    import org.apache.spark.sql.functions._
    val preds = frame.select("pred").distinct()
      .map(_.toString().replace("[", "").replace("]", ""))
      .rdd.zipWithIndex().toDF()
      .withColumnRenamed("_1", "pred")
      .withColumnRenamed("_2", "predID")
    preds.show()
    preds.write.parquet(outputDIR + File.separator + fileName + File.separator + fileName + ".p")
    println("[PRED SAVE DONE]")
    // ===== so
    val sos = frame.select("sub").distinct() //207424
      .union(frame.select("obj").distinct()).distinct() // 314853
      .toDF()
      .withColumnRenamed("sub", "sos")

    import org.apache.spark.sql.catalyst.encoders.RowEncoder
    val encoder = Encoders.tuple(
      Encoders.STRING,
      RowEncoder(
        sos.schema)
    )
    val frame1 = sos.map { row =>
      (row.toString().replace("[", "")
        .replace("]", ""), row)
    }(encoder)
      .withColumnRenamed("_1", "sos").toDF()
    frame1.show()

    val so = frame1.select("sos")
      .map(row => row.toString().replace("[", "").replace("]", ""))
      .rdd.zipWithIndex()
      .toDF()
      .withColumnRenamed("_1", "sos")
      .withColumnRenamed("_2", "sosID")
    so.show()
    so.write.parquet(outputDIR + File.separator + fileName + File.separator + fileName + ".so")
    println("[SO SAVE DONE]")
    // join
    // 原 frame
    // 谓词 preds
    // 主宾 so
    val frame2 = frame.join(preds, frame("pred") === preds("pred"))
      .join(so, frame("obj") === so("sos"))
      .withColumnRenamed("sosID", "objID")
      .select("sub", "predID", "objID")
      .join(so, frame("sub") === so("sos"))
      .withColumnRenamed("sosID", "subID")
      .select("subID", "predID", "objID")
      .withColumnRenamed("subID", "sub")
      .withColumnRenamed("predID", "pred")
      .withColumnRenamed("objID", "obj")
      .toDF()
    frame2.show()
    frame2.write.parquet(outputDIR + File.separator + fileName + File.separator + fileName + ".tri")

//    frame2.show()
//      .join(so, frame("sub") === so("sos"))


//    val so = _sc.parallelize(sos)
//      .map(row => row.toString().replace("[","")
//        .replace("]","")).toDF()
//    so.rdd.zipWithIndex().toDF().show()

//    // PRED =========================
//    val predArr = frame.select("pred").distinct()
//      .map(row => row.toString())
//      .rdd
//      .zipWithIndex().collect()
//
//    var preds = new HashMap[String, Long]
//    predArr.foreach(tup => {
//      println("=> " + tup._1 + " " + tup._2)
//      preds += (tup._1.replace("[", "").replace("]", "").trim
//        -> tup._2)
//    })
//
//    // TODO 存储 preds
//    _sc.parallelize(preds.toSeq).toDF().write.parquet(outputDIR + File.separator + fileName + File.separator + fileName + ".p")
//    println("[PRED SAVE DONE] ")
//    // SO ============================
//    val soArr = frame.select("sub").distinct().map(row => row.toString()).rdd
//      .union(frame.select("obj").distinct().map(row => row.toString()).rdd).zipWithIndex().collect()
//    var sos = new HashMap[String, Long]
//    soArr.foreach(tup => {
//      sos += (tup._1.replace("[", "").replace("]", "").trim
//        -> tup._2)
//    })
//
//    // TODO 存储 so
//    _sc.parallelize(sos.toSeq).toDF().write.parquet(outputDIR + File.separator + fileName + File.separator + fileName + ".so")
//    println("[SO SAVE DONE] ")
//    // TRIPLES =====================================
//
//    // TODO 存储 tri
//    frame.map(row => {
//      println("-> " + row.get(0).toString + " " + row.get(1).toString + " " + row.get(2).toString)
//      sos.get(row.get(0).toString).get + " " + preds.get(row.get(1).toString).get + " " + sos.get(row.get(2).toString).get
//    }).toDF().write.parquet(outputDIR + File.separator + fileName + File.separator + fileName + ".tri")
    println("[SPO SAVE DONE] ")
    println("[triples] " + frame.count())
    println("[MAPPING triples] " + frame2.count())
    println("[preds] " + preds.count())
    println("[so] " + sos.count())
    println("[数据mapping] DONE ")
  }
}
