package com.devangmundhra

import io.prediction.controller.PAlgorithm
import io.prediction.controller.Params
import io.prediction.controller.IPersistentModel
import io.prediction.controller.IPersistentModelLoader
import io.prediction.data.storage.BiMap

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.util.Random
import grizzled.slf4j.Logger

class RandomModel(val itemStringIntMap: BiMap[String, Int],
                  val items: Map[Int, Item]
                   ) extends IPersistentModel[Params] with Serializable {

  @transient lazy val itemIntStringMap = itemStringIntMap.inverse

  def save(id: String, params: Params,
           sc: SparkContext): Boolean = {
    sc.parallelize(Seq(itemStringIntMap))
      .saveAsObjectFile(s"/tmp/${id}/itemStringIntMap")
    sc.parallelize(Seq(items))
      .saveAsObjectFile(s"/tmp/${id}/items")
    true
  }

  override def toString = {
    s" itemStringIntMap: [${itemStringIntMap.size}]" +
      s"(${itemStringIntMap.take(2).toString}...)]" +
      s" items: [${items.size}]" +
      s"(${items.take(2).toString}...)]"
  }
}

object RandomModel
  extends IPersistentModelLoader[Params, RandomModel] {
  def apply(id: String, params: Params,
            sc: Option[SparkContext]) = {
    new RandomModel(
      itemStringIntMap = sc.get
        .objectFile[BiMap[String, Int]](s"/tmp/${id}/itemStringIntMap").first,
      items = sc.get
        .objectFile[Map[Int, Item]](s"/tmp/${id}/items").first)
  }
}

class RandomAlgorithm()
  extends PAlgorithm[PreparedData, RandomModel, Query, PredictedResult] {

  @transient lazy val logger = Logger[this.type]

  def train(data: PreparedData): RandomModel = {
    val itemStringIntMap = BiMap.stringInt(data.items.keys)
    val items: Map[Int, Item] = data.items.map { case (id, item) =>
      (itemStringIntMap(id), item)
    }.collectAsMap.toMap

    logger.info(s"DEBUG Random Algo \n")

    new RandomModel(
      itemStringIntMap = itemStringIntMap,
      items = items
    )
  }

  def predict(model: RandomModel, query: Query): PredictedResult = {
    // convert items to Int index
    val queryList: Set[Int] = query.items.map(model.itemStringIntMap.get(_))
      .flatten.toSet

    val whiteList: Option[Set[Int]] = query.whiteList.map(set =>
      set.map(model.itemStringIntMap.get(_)).flatten
    )
    val blackList: Option[Set[Int]] = query.blackList.map(set =>
      set.map(model.itemStringIntMap.get(_)).flatten
    )

    val items = model.items.filter { case (i, item) =>
      isCandidateItem(
        i = i,
        items = model.items,
        categories = query.categories,
        queryList = queryList,
        whiteList = whiteList,
        blackList = blackList
      )
    }

    val randomScores = Random.shuffle(items).take(query.num)

    val itemScores = randomScores.map { case (i, item) =>
      new ItemScore(
        item = model.itemIntStringMap(i),
        score = 0
      )
    }

    new PredictedResult(itemScores.toArray)
  }

  private
  def isCandidateItem(
                       i: Int,
                       items: Map[Int, Item],
                       categories: Option[Set[String]],
                       queryList: Set[Int],
                       whiteList: Option[Set[Int]],
                       blackList: Option[Set[Int]]
                       ): Boolean = {
    whiteList.map(_.contains(i)).getOrElse(true) &&
      blackList.map(!_.contains(i)).getOrElse(true) &&
      // discard items in query as well
      (!queryList.contains(i)) &&
      // filter categories
      categories.map { cat =>
        items(i).categories.map { itemCat =>
          // keep this item if has ovelap categories with the query
          !(itemCat.toSet.intersect(cat).isEmpty)
        }.getOrElse(false) // discard this item if it has no categories
      }.getOrElse(true)
  }
}