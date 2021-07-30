package org.apache.spark.rawkvbulkload

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partitioner, SparkConf}
import org.slf4j.LoggerFactory
import org.tikv.common.codec.KeyUtils
import org.tikv.common.importer.{ImporterClient, SwitchTiKVModeClient}
import org.tikv.common.key.Key
import org.tikv.common.region.TiRegion
import org.tikv.common.util.{FastByteComparisons, Pair}
import org.tikv.common.{TiConfiguration, TiSession}
import org.tikv.shade.com.google.protobuf.ByteString

import java.util
import java.util.UUID
import java.util.concurrent.{Executors, ScheduledExecutorService}
import scala.collection.JavaConverters._
import scala.collection.mutable

object RawKVBulkLoad {
  var input: String = null
  var pdaddr: String = null

  def main(args: Array[String]): Unit = {


    args.sliding(2, 2).toList.collect {
      case Array("--input", argInput: String) => input = argInput
      case Array("--pdaddr", argPdAddr: String) => pdaddr = argPdAddr
    }
    assert(input != null, "should set --input")
    assert(pdaddr != null, "should set --pdaddr")

    val sparkConf = new SparkConf()
      .setIfMissing("spark.master", "local[*]")
      .setIfMissing("spark.app.name", getClass.getName)

    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    // TODO get parquet in correct schema
    val rdd = spark.read.parquet(input).rdd.map(row => {
      (row.getString(0).toArray.map(_.toByte), row.getString(1).toArray.map(_.toByte))
    })
    //    init ticklient

    val conf = TiConfiguration.createDefault(pdaddr)
    new RawKVBulkLoad().bulkLoad(conf, rdd)


  }
}

class RawKVBulkLoad extends Serializable {
  private final val logger = LoggerFactory.getLogger(getClass.getName)

  @transient private val scheduledExecutorService: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true).build);

  @transient private var tiSession: TiSession = _
  private var tiConf: TiConfiguration = _

  private var partitioner: TiReginSplitPartitionerV2 = _

  // region split
  val optionsSplitRegionBackoffMS = 120000
  val optionsScatterRegionBackoffMS = 30000
  val optionsScatterWaitMS = 300000

  // sample
  private val optionsRegionSplitNum = 0
  private val optionsMinRegionSplitNum = 1
  private val optionsRegionSplitKeys = 960000
  private val optionsMaxRegionSplitNum = 64
  private val optionsSampleSplitFrac = 1000
  private val optionsRegionSplitUsingSize = true
  private val optionsBytesPerRegion = 100663296

  private def bulkLoad(conf: TiConfiguration, rdd: RDD[(Array[Byte], Array[Byte])]): Unit = {
    tiConf = conf;
    tiSession = TiSession.create(tiConf)

    // 2 sort
    val rdd2 = rdd.map { pair =>
      (new SerializableKey(pair._1), pair._2)
    }.sortByKey().persist(StorageLevel.DISK_ONLY)

    // 3 calculate regionSplitPoints
    val orderedSplitPoints = getRegionSplitPoints(rdd2)

    // 4 switch to normal mode
    val switchTiKVModeClient = new SwitchTiKVModeClient(tiSession.getPDClient, tiSession.getImporterRegionStoreClientBuilder)
    switchTiKVModeClient.switchTiKVToNormalMode()
    //    tiSession.getImportSSTClient.switchTiKVToNormalMode()

    // 5 call region split and scatter
    tiSession.splitRegionAndScatter(
      orderedSplitPoints.map(_.bytes).asJava,
      optionsSplitRegionBackoffMS,
      optionsScatterRegionBackoffMS,
      optionsScatterWaitMS)

    //    tiSession.getRegionManager().invalidateAll()
    // 7 switch to import mode
    switchTiKVModeClient.keepTiKVToImportMode()
    //    scheduledExecutorService.scheduleAtFixedRate(new Runnable {
    //      override def run(): Unit = {
    //        switchToImportMode()
    //      }
    //    }, 0, 2, TimeUnit.SECONDS)

    // 6 refetch region info
    val minKey = rdd2.map(p => p._1).min().getRowKey
    val maxKey = rdd2.map(p => p._1).max().getRowKey
    val orderedRegions = getRegionInfo(minKey, maxKey)
    logger.info("orderedRegions size = " + orderedRegions.size)

    //8  repartition rdd according region
    partitioner = new TiReginSplitPartitionerV2(orderedRegions)
    val rdd3 = rdd2.partitionBy(partitioner).persist(StorageLevel.DISK_ONLY)
    logger.info("rdd3.getNumPartitions = " + rdd3.getNumPartitions)

    // call writeAndIngest for each partition
    //    (1 to ingestNumber).foreach { i =>
    //      logger.info(s"writeAndIngest round: $i")
    rdd3.foreachPartition { itor =>
      writeAndIngest(itor.map(pair => (pair._1.bytes, pair._2)), partitioner)
    }
    //  }
    switchTiKVModeClient.stopKeepTiKVToImportMode()
    switchTiKVModeClient.switchTiKVToNormalMode()
  }

  private def writeAndIngest(iterator: Iterator[(Array[Byte], Array[Byte])], partitioner: TiReginSplitPartitionerV2): Unit = {
    val (itor1, tiro2) = iterator.duplicate

    var minKey: Key = Key.MAX
    var maxKey: Key = Key.MIN
    var region: TiRegion = null
    var key: Key = null
    val dataSize = itor1.map { itor =>
      key = Key.toRawKey(itor._1)

      if (region == null) {
        region = partitioner.getRegion(key)
      }

      if (key.compareTo(minKey) < 0) {
        minKey = key
      }
      if (key.compareTo(maxKey) > 0) {
        maxKey = key
      }
    }.size

    if (dataSize > 0) {
      if (region == null) {
        logger.warn("region == null, skip ingest this partition")
      } else {
        val uuid = genUUID()

        logger.warn(s"start to ingest this partition ${util.Arrays.toString(uuid)}")
        val pairsIterator = tiro2.map { keyValue => new Pair[ByteString, ByteString](ByteString.copyFrom(keyValue._1), ByteString.copyFrom(keyValue._2))

        }.asJava
        // TODO ttl

        var importerClient = new ImporterClient(tiSession, ByteString.copyFrom(uuid), minKey, maxKey, region, 12)
        importerClient.rawWrite(pairsIterator)
        //        val importSSTManager = new ImportSSTManager(uuid, TiSession.getInstance(tiConf), minKey, maxKey, region)
        //        importSSTManager.write(pairsIterator)
        logger.warn(s"finish to ingest this partition ${util.Arrays.toString(uuid)}")
      }
    }
  }

  // TODO get ByteString type uuid
  private def genUUID(): Array[Byte] = {
    val uuid = UUID.randomUUID()

    val out = new Array[Byte](16)
    val msb = uuid.getMostSignificantBits
    val lsb = uuid.getLeastSignificantBits
    for (i <- 0 until 8) {
      out(i) = ((msb >> ((7 - i) * 8)) & 0xff).toByte
    }
    for (i <- 8 until 16) {
      out(i) = ((lsb >> ((15 - i) * 8)) & 0xff).toByte
    }
    out
  }

  private def getRegionInfo(min: Key, max: Key): List[TiRegion] = {
    val regions = new mutable.ArrayBuffer[TiRegion]()

    tiSession.getRegionManager.invalidateAll()

    var current = min

    while (current.compareTo(max) <= 0) {
      val region = tiSession.getRegionManager.getRegionByKey(current.toByteString)
      regions.append(region)
      current = Key.toRawKey(region.getEndKey())
    }

    regions.toList
  }

  private def getRegionSplitPoints(rdd: RDD[(SerializableKey, Array[Byte])]): List[SerializableKey] = {
    val count = rdd.count()

    val regionSplitPointNum = if (optionsRegionSplitNum > 0) {
      optionsRegionSplitNum
    } else {
      Math.min(
        Math.max(
          optionsMinRegionSplitNum,
          Math.ceil(count.toDouble / optionsRegionSplitKeys).toInt),
        optionsMaxRegionSplitNum)
    }
    logger.info(s"regionSplitPointNum=$regionSplitPointNum")

    val sampleSize = (regionSplitPointNum + 1) * optionsSampleSplitFrac
    logger.info(s"sampleSize=$sampleSize")

    val sampleData = if (sampleSize < count) {
      rdd.sample(false, sampleSize.toDouble / count).collect()
    } else {
      rdd.collect()
    }
    logger.info(s"sampleData size=${sampleData.length}")

    val splitPointNumUsingSize = if (optionsRegionSplitUsingSize) {
      val avgSize = getAverageSizeInBytes(sampleData)
      logger.info(s"avgSize=$avgSize Bytes")
      if (avgSize <= optionsBytesPerRegion / optionsRegionSplitKeys) {
        regionSplitPointNum
      } else {
        Math.min(
          Math.floor((count.toDouble / optionsBytesPerRegion) * avgSize).toInt,
          sampleData.length / 10)
      }
    } else {
      regionSplitPointNum
    }
    logger.info(s"splitPointNumUsingSize=$splitPointNumUsingSize")

    val finalRegionSplitPointNum = Math.min(
      Math.max(optionsMinRegionSplitNum, splitPointNumUsingSize),
      optionsMaxRegionSplitNum)
    logger.info(s"finalRegionSplitPointNum=$finalRegionSplitPointNum")

    val sortedSampleData = sampleData
      .map(_._1)
      .sorted(new Ordering[SerializableKey] {
        override def compare(x: SerializableKey, y: SerializableKey): Int = {
          x.compareTo(y)
        }
      })
    val orderedSplitPoints = new Array[SerializableKey](finalRegionSplitPointNum)
    val step = Math.floor(sortedSampleData.length.toDouble / (finalRegionSplitPointNum + 1)).toInt
    for (i <- 0 until finalRegionSplitPointNum) {
      orderedSplitPoints(i) = sortedSampleData((i + 1) * step)
    }

    logger.info(s"orderedSplitPoints size=${orderedSplitPoints.length}")
    orderedSplitPoints.toList
  }

  private def getAverageSizeInBytes(keyValues: Array[(SerializableKey, Array[Byte])]): Int = {
    var avg: Double = 0
    var t: Int = 1
    keyValues.foreach { keyValue =>
      val keySize: Double = keyValue._1.bytes.length + keyValue._2.length
      avg = avg + (keySize - avg) / t
      t = t + 1
    }
    Math.ceil(avg).toInt
  }
}


class SerializableKey(val bytes: Array[Byte])
  extends Comparable[SerializableKey]
    with Serializable {
  override def toString: String = LogDesensitization.hide(KeyUtils.formatBytes(bytes))

  override def equals(that: Any): Boolean =
    that match {
      case that: SerializableKey => this.bytes.sameElements(that.bytes)
      case _ => false
    }

  override def hashCode(): Int =
    util.Arrays.hashCode(bytes)

  override def compareTo(o: SerializableKey): Int = {
    FastByteComparisons.compareTo(bytes, o.bytes)
  }

  def getRowKey: Key = {
    Key.toRawKey(bytes)
  }
}

object LogDesensitization {
  private val enableLogDesensitization = getLogDesensitization

  def hide(info: String): String = if (enableLogDesensitization) "*"
  else info

  /**
   * TiSparkLogDesensitizationLevel = 1 => disable LogDesensitization, otherwise enable
   * LogDesensitization
   *
   * @return true enable LogDesensitization, false disable LogDesensitization
   */
  private def getLogDesensitization: Boolean = {
    val tiSparkLogDesensitizationLevel = "TiSparkLogDesensitizationLevel"
    var tmp = System.getenv(tiSparkLogDesensitizationLevel)
    if (tmp != null && !("" == tmp)) return !("1" == tmp)
    tmp = System.getProperty(tiSparkLogDesensitizationLevel)
    if (tmp != null && !("" == tmp)) return !("1" == tmp)
    true
  }
}


class TiReginSplitPartitionerV2(orderedRegions: List[TiRegion])
  extends Partitioner {
  override def getPartition(key: Any): Int = {
    val serializableKey = key.asInstanceOf[SerializableKey]
    val rawKey = Key.toRawKey(serializableKey.bytes)

    if (orderedRegions.isEmpty) {
      0
    } else {
      val firstRegion = orderedRegions.head
      if (rawKey.compareTo(getRowStartKey(firstRegion)) < 0) {
        0
      } else {
        orderedRegions.indices.foreach { i =>
          val region = orderedRegions(i)
          if (rawKey.compareTo(getRowStartKey(region)) >= 0 && rawKey.compareTo(getRowEndKey(region)) < 0) {
            return i + 1
          }
        }
        orderedRegions.size + 1
      }
    }
  }

  def getRegion(key: Key): TiRegion = {
    orderedRegions.foreach { region =>
      if (key.compareTo(getRowStartKey(region)) >= 0 && key.compareTo(getRowEndKey(region)) < 0) {
        return region
      }
    }
    null
  }

  // not support in TiRegion, add manually
  private def getRowStartKey(region: TiRegion): Key = {
    if (region.getStartKey.isEmpty) return Key.MIN
    return Key.toRawKey(region.getStartKey)
  }

  private def getRowEndKey(region: TiRegion): Key = {
    return Key.toRawKey(region.getEndKey())
  }

  override def numPartitions: Int = {
    orderedRegions.size + 2
  }
}

class BytePairWrapper(val key: Array[Byte], val value: Array[Byte]) {
  def getKey: Array[Byte] = key

  def getValue: Array[Byte] = value
}
