/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.iflytek.producer

import scala.collection.JavaConversions._
import joptsimple._
import java.util.{Properties, UUID}
import java.io._
import kafka.common._
import kafka.message._
import kafka.serializer._
import java.util.Properties
import kafka.producer.Producer
import kafka.producer.ProducerConfig
import kafka.producer.KeyedMessage

case class KafkaProducer(
  topic: String, 
  brokerList: String,
  clientId: String = UUID.randomUUID().toString,
  synchronously: Boolean = true,
  compress: Boolean = true,
  batchSize: Integer = 200,
  messageSendMaxRetries: Integer = 3,
  requestRequiredAcks: Integer = -1
  ) { 

  val props = new Properties()

  val codec = if(compress) DefaultCompressionCodec.codec else NoCompressionCodec.codec

  props.put("compression.codec", codec.toString)
  props.put("producer.type", if(synchronously) "sync" else "async")
  props.put("metadata.broker.list", brokerList)
  props.put("batch.num.messages", batchSize.toString)
  props.put("message.send.max.retries", messageSendMaxRetries.toString)
  props.put("request.required.acks",requestRequiredAcks.toString)
  props.put("client.id",clientId.toString)

  val producer = new Producer[AnyRef, AnyRef](new ProducerConfig(props))
  
  def kafkaMesssage(message: Array[Byte], partition: Array[Byte]): KeyedMessage[AnyRef, AnyRef] = {
     if (partition == null) {
       new KeyedMessage(topic,message)
     } else {
       new KeyedMessage(topic,partition,message)
     }
  }
  
  def send(message: String, partition: String = null): Unit = send(message.getBytes("UTF8"), if (partition == null) null else partition.getBytes("UTF8"))

  def send(message: Array[Byte], partition: Array[Byte]): Unit = {
    try {
      producer.send(kafkaMesssage(message, partition))
    } catch {
      case e: Exception =>
        e.printStackTrace
        System.exit(1)
    }        
  }
}
