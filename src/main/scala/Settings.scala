import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Settings {
  val sparkSession = loadSparkSession(loadSparkConf())
  val sparkContext = loadSparkContext(sparkSession)
  var inputFile = ""
  var outputDIR = ""
  def loadUserSettings(inFile:String,
                       outDIR:String ) = {
    this.inputFile = inFile
    this.outputDIR = outDIR
  }

  /**
   * Create SparkContext.
   * The overview over settings:
   * http://spark.apache.org/docs/latest/programming-guide.html
   */
  def loadSparkConf(): SparkConf = {

    val conf = new SparkConf()
      .setAppName("NCI_MAPPING")
      .setMaster("local[*]")
      .set("spark.executor.memory", "8g")
        .set("spark.driver.memory", "8g")
        .set("spark.executor.heartbeatInterval", "20s")

//      .set("spark.sql.inMemoryColumnarStorage.compressed", "true")
//      //.set("spark.sql.autoBroadcastJoinThreshold", "-1")
//      .set("spark.sql.parquet.filterPushdown", "true")  //优化手段，先filter再join
      .set("spark.sql.inMemoryColumnarStorage.batchSize", "20000")
//      .set("spark.storage.blockManagerSlaveTimeoutMs", "3000000")
//      //.set("spark.sql.shuffle.partitions", "200")
//      .set("spark.storage.memoryFraction", "0.5")
      //                                  .setMaster("spark://dbnode7:7077")

//      .set("spark.sql.warehouse.dir","file:///")
    conf
  }

  def loadSparkSession(conf: SparkConf): SparkSession  = {
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    spark
  }
  def loadSparkContext(sparkSession: SparkSession): SparkContext = {
    sparkSession.sparkContext
  }
}
