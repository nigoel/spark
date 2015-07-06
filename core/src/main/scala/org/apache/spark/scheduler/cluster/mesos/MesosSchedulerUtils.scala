/*
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

package org.apache.spark.scheduler.cluster.mesos

import java.util.List

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

import org.apache.mesos.Protos._
import org.apache.mesos.Protos.Value.Type
import org.apache.mesos.{SchedulerDriver, Scheduler, MesosSchedulerDriver}
import org.apache.mesos.protobuf.ByteString

import org.apache.spark.{SparkException, Logging, SparkConf}

/**
 * Creates the MesosSchedulerDriver, which constructs the appropriate framework info
 * to register with Mesos. This also supports principal, secret and role.
 */
private[mesos] object MesosSchedulerUtils extends Logging {
  def createSchedulerDriver(
      scheduler: Scheduler,
      sparkUser: String,
      appName: String,
      masterUrl: String,
      conf: SparkConf): SchedulerDriver = {
    val fwInfoBuilder = FrameworkInfo.newBuilder().setUser(sparkUser).setName(appName)
    val credBuilder = Credential.newBuilder()
    conf.getOption("spark.mesos.principal").foreach { principal =>
      fwInfoBuilder.setPrincipal(principal)
      credBuilder.setPrincipal(principal)
    }
    conf.getOption("spark.mesos.secret").foreach { secret =>
      credBuilder.setSecret(ByteString.copyFromUtf8(secret))
    }
    if (credBuilder.hasSecret && !fwInfoBuilder.hasPrincipal) {
      throw new SparkException(
        "spark.mesos.principal must be configured when spark.mesos.secret is set")
    }
    conf.getOption("spark.mesos.role").foreach { role =>
      fwInfoBuilder.setRole(role)
    }
    if (credBuilder.hasPrincipal) {
      new MesosSchedulerDriver(
        scheduler, fwInfoBuilder.build(), masterUrl, credBuilder.build())
    } else {
      new MesosSchedulerDriver(scheduler, fwInfoBuilder.build(), masterUrl)
    }
  }

  /**
   * Return the value of the resource that matches the given name.
   */
  def getResource(res: List[Resource], name: String): Double = {
    // A resource can have multiple values in the offer since it can either be from
    // a specific role or wildcard.
    res.filter(_.getName == name).map(_.getScalar.getValue).sum
  }

  /**
   * Partition the existing set of resources into two groups, those remaining to be
   * scheduled and those requested to be used for a new task.
   * @param resources The full list of available resources
   * @param resourceName The name of the resource to take from the available resources
   * @param count The amount of resources to take from the available resources
   * @return The remaining resources list and the used resources list.
   */
  def partitionResources(
      resources: List[Resource],
      resourceName: String,
      count: Double): (List[Resource], List[Resource]) = {
    var remain = count
    var requestedResources = new ArrayBuffer[Resource]
    val remainingResources = resources.collect {
      case r => {
        if (remain > 0 &&
          r.getType == Type.SCALAR &&
          r.getScalar.getValue > 0.0 &&
          r.getName == resourceName) {
          val usage = Math.min(remain, r.getScalar.getValue)
          requestedResources += Resource.newBuilder()
            .setName(resourceName)
            .setRole(r.getRole)
            .setType(Value.Type.SCALAR)
            .setScalar(Value.Scalar.newBuilder().setValue(usage).build())
            .build()
          remain -= usage
          Resource.newBuilder()
            .setName(resourceName)
            .setRole(r.getRole)
            .setType(Value.Type.SCALAR)
            .setScalar(Value.Scalar.newBuilder().setValue(r.getScalar.getValue - usage).build())
            .build()
        } else {
          r
        }
      }
    }

    // Filter any resource that has depleted.
    (remainingResources.filter(r => r.getType != Type.SCALAR || r.getScalar.getValue > 0.0).toList,
      requestedResources.toList)
  }
}
