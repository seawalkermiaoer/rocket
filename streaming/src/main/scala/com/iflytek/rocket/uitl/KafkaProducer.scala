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

package com.iflytek.rocket.uitl

import java.util.{Properties, UUID}

import kafka.message._
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

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
  
  def kafkaMesssage(message: String): KeyedMessage[AnyRef, AnyRef] = {
     new KeyedMessage(topic, message.getBytes())
  }
  
//  def send(message: String, partition: String = null): Unit =
//    send(message.getBytes("ISO-8859-1"), if (partition == null) null else partition.getBytes("ISO-8859-1"))

  def send(message: String): Unit = {
    try {

      producer.send(kafkaMesssage(message))
    } catch {
      case e: Exception =>
        e.printStackTrace
        System.exit(1)
    }        
  }
}
