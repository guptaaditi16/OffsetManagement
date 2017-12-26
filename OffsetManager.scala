import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.zookeeper.{CreateMode, KeeperException, Watcher, WatchedEvent, ZooKeeper}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.retry.ExponentialBackoffRetry
import java.lang.Boolean

/* OffsetManager Object denotes a Fault-Tolerant Data Processing System built with the help of zookeeper data node. Initially zookeeper node's value is set to 0.Records are read and processed in the chunks of size 10.
 * Every time all 10 records in a chunk get processed, we store the current record's offset into the zookeeper node. This helps the application to resume the processing 
 * from the same chunk number, where the master node of spark application got failed/crashed. 
 */
object OffsetManager {
  
  // Setting configuration for spark 
  val SPARK_CONFIGURATION = new SparkConf()
     .setAppName("OffsetManager")
     .setMaster("local[*]")  
  val SPARK_CONTEXT = new SparkContext(SPARK_CONFIGURATION)
  
  // Zookeeper data node path
  val ZNODE_PATH = "/aditiZNode"
  val INPUT_FILE_PATH = "input.txt"
  val ZOOKEEPER_CONNECTION_STRING = "localhost:2181"
  
  /* Chunk size denotes the number of records to be processed every time before updating the offset value into the zookeeper data node */
  val CHUNK_SIZE = 10
  
  /*
   * Function to create a connection to zookeeper and return a client instance 
   */
  def getZookeeperClient() : CuratorFramework = {
    val retryPolicy = new ExponentialBackoffRetry(1000, 3)
    return CuratorFrameworkFactory.newClient(ZOOKEEPER_CONNECTION_STRING, retryPolicy)
  }
  
  /*
   * Function to check whether the given record's index is within the current chunk's range or not.
   * @return true if index is within the range, false otherwise
   */
  def isIndexWithinChunkRange(recordIndex : Int, currentRecordNo: Int) : Boolean = {
    (recordIndex >= currentRecordNo && recordIndex < (currentRecordNo + CHUNK_SIZE))
  }
  
   def main(args: Array[String]) = {
    val inputRDD = SPARK_CONTEXT.textFile(INPUT_FILE_PATH) 
    val indexedInputRDD = inputRDD.zipWithIndex()
    
    /* Get the instance of a zookeeper client and start the connection */ 
    val zookeeperClient = getZookeeperClient()
    zookeeperClient.start()
    
    /* Fetching the latest value of offset from Zookeeper Node, initially set 0 */
    var currentRecordNo = new String(zookeeperClient.getData.forPath(ZNODE_PATH)).toInt
    while (currentRecordNo < inputRDD.count.toInt) {
      
      //1 second delay added to ease the manual killing of process
      Thread.sleep(1000)
      
      //Selecting a Chunk of records equal to CHUNK_SIZE
      val filteredRDD = indexedInputRDD.filter(record => isIndexWithinChunkRange(record._2.toInt, currentRecordNo))
      // val subinputRDD = inputRDD.take(currentRecordNo + CHUNK_SIZE).drop(currentRecordNo)
     
      //Processing each record from the Chunk, in our case - changing the case to uppercase.
      val outRDD = filteredRDD.map {record => 
         record._1.toUpperCase()
      }
      
      //Printing the processed result
      outRDD.foreach { println }
      //Update record number to the latest value of offset, in this application as well as in zookeeper
      currentRecordNo = currentRecordNo + CHUNK_SIZE
      zookeeperClient.setData().forPath(ZNODE_PATH,currentRecordNo.toString().getBytes)
    }
    zookeeperClient.close()
    SPARK_CONTEXT.stop
   }
}