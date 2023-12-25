/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.execution.load;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFileNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFilePieceNode;
import org.apache.iotdb.db.queryengine.plan.scheduler.load.LoadTsFileScheduler.LoadCommand;
import org.apache.iotdb.mpp.rpc.thrift.TLoadCommandReq;
import org.apache.iotdb.mpp.rpc.thrift.TLoadResp;
import org.apache.iotdb.mpp.rpc.thrift.TTsFilePieceReq;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.thrift.TException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MB;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("java:S2925")
public class TsFileSplitSenderTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(TsFileSplitSenderTest.class);
  protected Map<TEndPoint, Map<ConsensusGroupId, Map<String, Map<File, Set<Integer>>>>>
      phaseOneResults = new ConcurrentSkipListMap<>();
  // the third key is UUid, the value is command type
  protected Map<TEndPoint, Map<ConsensusGroupId, Map<String, Integer>>> phaseTwoResults =
      new ConcurrentSkipListMap<>();
  // simulating network delay and packet loss
  private long dummyDelayMS = 0;
  private double packetLossRatio = 0.00;
  private Random random = new Random();
  private long maxSplitSize = 128 * 1024 * 1024L;
  // simulating jvm stall like GC
  private long minStuckIntervalMS = 50000;
  private long maxStuckIntervalMS = 100000;
  private long stuckDurationMS = 0;

  private long nodeThroughput = 10_000;

  protected Map<TEndPoint, Pair<Long, Long>> nextStuckTimeMap = new ConcurrentHashMap<>();
  private AtomicLong sumHandleTime = new AtomicLong();
  private AtomicLong decompressTime = new AtomicLong();
  private AtomicLong deserializeTime = new AtomicLong();
  private AtomicLong relayTime = new AtomicLong();
  private AtomicLong maxMemoryUsage = new AtomicLong();

  @Test
  public void test() throws IOException {
    Thread thread =
        new Thread(
            () -> {
              while (!Thread.interrupted()) {
                long preUsage = maxMemoryUsage.get();
                long newUsage =
                    Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
                if (preUsage < newUsage) {
                  maxMemoryUsage.set(newUsage);
                }
                try {
                  Thread.sleep(100);
                } catch (InterruptedException e) {
                  return;
                }
              }
            });
    thread.start();

    int filesPerNode = 2;
    int startFileIndex = 0;
    long start = System.currentTimeMillis();
    long transmissionTime = 0;
    while (startFileIndex < tsFileResources.size()) {
      int endFileIndex = Math.min(tsFileResources.size(), startFileIndex + filesPerNode);
      LoadTsFileNode loadTsFileNode =
          new LoadTsFileNode(new PlanNodeId("testPlanNode"), tsFileResources.subList(startFileIndex, endFileIndex));
      DataPartitionBatchFetcher partitionBatchFetcher = dummyDataPartitionBatchFetcher();
      TsFileSplitSender splitSender =
          new TsFileSplitSender(
              loadTsFileNode,
              partitionBatchFetcher,
              TimePartitionUtils.getTimePartitionInterval(),
              internalServiceClientManager,
              false,
              maxSplitSize,
              100,
              "root",
              "root");
      splitSender.start();
      transmissionTime += splitSender.getStatistic().getCompressedSize().get() / nodeThroughput;
      startFileIndex = endFileIndex;
    }

    long timeConsumption = System.currentTimeMillis() - start;
    thread.interrupt();

    printPhaseResult();
    System.out.printf(
        "Split ends after %dms + %dms (Transmission) = %dms\n",
        timeConsumption, transmissionTime, timeConsumption + transmissionTime);
    System.out.printf("Handle sum %dns\n", sumHandleTime.get());
    System.out.printf("Decompress sum %dns\n", decompressTime.get());
    System.out.printf("Deserialize sum %dns\n", deserializeTime.get());
    System.out.printf("Relay sum %dns\n", relayTime.get());
    System.out.printf("Memory usage %dMB\n", maxMemoryUsage.get() / MB);
  }

  public TLoadResp handleTsFilePieceNode(TTsFilePieceReq req, TEndPoint tEndPoint)
      throws TException, IOException {
    final long handleStart = System.nanoTime();
    if ((tEndPoint.getPort() - 10000) % 3 == 0
        && random.nextDouble() < packetLossRatio
        && req.relayTargets != null) {
      throw new TException("Packet lost");
    }
    if ((tEndPoint.getPort() - 10000) % 3 == 1
        && random.nextDouble() < packetLossRatio / 2
        && req.relayTargets != null) {
      throw new TException("Packet lost");
    }

    if ((tEndPoint.getPort() - 10000) % 3 == 0 && req.relayTargets != null && stuckDurationMS > 0) {
      Pair<Long, Long> nextStuckTime =
          nextStuckTimeMap.computeIfAbsent(
              tEndPoint,
              e ->
                  new Pair<>(
                      System.currentTimeMillis(), System.currentTimeMillis() + stuckDurationMS));
      long currTime = System.currentTimeMillis();
      if (currTime >= nextStuckTime.left && currTime < nextStuckTime.right) {
        logger.debug("Node{} stalls", tEndPoint.getPort() - 10000);
        try {
          Thread.sleep(nextStuckTime.right - currTime);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      } else if (currTime > nextStuckTime.right) {
        nextStuckTimeMap.compute(
            tEndPoint,
            (endPoint, newInterval) -> {
              if (newInterval != null && currTime < newInterval.right) {
                return newInterval;
              }
              long start =
                  currTime
                      + minStuckIntervalMS
                      + random.nextInt((int) (maxStuckIntervalMS - minStuckIntervalMS));
              return new Pair<>(start, start + stuckDurationMS);
            });
      }
    }

    long decompressStart = System.nanoTime();
    ByteBuffer buf = req.body.slice();
    if (req.isSetCompressionType()) {
      CompressionType compressionType = CompressionType.deserialize(req.compressionType);
      IUnCompressor unCompressor = IUnCompressor.getUnCompressor(compressionType);
      int uncompressedLength = req.getUncompressedLength();
      ByteBuffer allocate = ByteBuffer.allocate(uncompressedLength);
      unCompressor.uncompress(
          buf.array(), buf.arrayOffset() + buf.position(), buf.remaining(), allocate.array(), 0);
      allocate.limit(uncompressedLength);
      buf = allocate;
    }
    decompressTime.addAndGet(System.nanoTime() - decompressStart);

    long deserializeStart = System.nanoTime();
    LoadTsFilePieceNode pieceNode = (LoadTsFilePieceNode) PlanNodeType.deserialize(buf);
    deserializeTime.addAndGet(System.nanoTime() - deserializeStart);

    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.consensusGroupId);
    Set<Integer> splitIds =
        phaseOneResults
            .computeIfAbsent(
                tEndPoint,
                e -> new ConcurrentSkipListMap<>(Comparator.comparingInt(ConsensusGroupId::getId)))
            .computeIfAbsent(groupId, g -> new ConcurrentSkipListMap<>())
            .computeIfAbsent(req.uuid, id -> new ConcurrentSkipListMap<>())
            .computeIfAbsent(pieceNode.getTsFile(), f -> new ConcurrentSkipListSet<>());
    splitIds.addAll(
        pieceNode.getAllTsFileData().stream()
            .map(TsFileData::getSplitId)
            .collect(Collectors.toList()));

    if (dummyDelayMS > 0) {
      if ((tEndPoint.getPort() - 10000) % 3 == 0 && req.relayTargets != null) {
        try {
          Thread.sleep(dummyDelayMS);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      if ((tEndPoint.getPort() - 10000) % 3 == 1 && req.relayTargets != null) {
        try {
          Thread.sleep(dummyDelayMS / 2);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }

    // forward to other replicas in the group
    if (req.relayTargets != null) {
      long relayStart = System.nanoTime();
      TRegionReplicaSet regionReplicaSet = req.relayTargets;
      req.relayTargets = null;
      regionReplicaSet.getDataNodeLocations().stream()
          .parallel()
          .forEach(
              dataNodeLocation -> {
                TEndPoint otherPoint = dataNodeLocation.getInternalEndPoint();
                if (!otherPoint.equals(tEndPoint)) {
                  try {
                    handleTsFilePieceNode(req, otherPoint);
                  } catch (TException | IOException e) {
                    throw new RuntimeException(e);
                  }
                }
              });
      relayTime.addAndGet(System.nanoTime() - relayStart);
    }

    sumHandleTime.addAndGet(System.nanoTime() - handleStart);
    return new TLoadResp()
        .setAccepted(true)
        .setStatus(new TSStatus().setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
  }

  public TLoadResp handleTsLoadCommand(TLoadCommandReq req, TEndPoint tEndPoint) {
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(req.consensusGroupId);
    phaseTwoResults
        .computeIfAbsent(
            tEndPoint,
            e -> new ConcurrentSkipListMap<>(Comparator.comparingInt(ConsensusGroupId::getId)))
        .computeIfAbsent(groupId, g -> new ConcurrentSkipListMap<>())
        .computeIfAbsent(req.uuid, id -> req.commandType);

    // forward to other replicas in the group
    if (req.useConsensus) {
      req.useConsensus = false;
      TRegionReplicaSet regionReplicaSet = groupId2ReplicaSetMap.get(groupId);
      for (TDataNodeLocation dataNodeLocation : regionReplicaSet.getDataNodeLocations()) {
        TEndPoint otherPoint = dataNodeLocation.getInternalEndPoint();
        if (!otherPoint.equals(tEndPoint)) {
          handleTsLoadCommand(req, otherPoint);
        }
      }
    }

    return new TLoadResp()
        .setAccepted(true)
        .setStatus(new TSStatus().setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
  }

  public void printPhaseResult() {
    System.out.print("Phase one:\n");
    for (Entry<TEndPoint, Map<ConsensusGroupId, Map<String, Map<File, Set<Integer>>>>>
        endPointMapEntry : phaseOneResults.entrySet()) {
      TEndPoint endPoint = endPointMapEntry.getKey();
      for (Entry<ConsensusGroupId, Map<String, Map<File, Set<Integer>>>> consensusGroupIdMapEntry :
          endPointMapEntry.getValue().entrySet()) {
        ConsensusGroupId consensusGroupId = consensusGroupIdMapEntry.getKey();
        for (Entry<String, Map<File, Set<Integer>>> stringMapEntry :
            consensusGroupIdMapEntry.getValue().entrySet()) {
          String uuid = stringMapEntry.getKey();
          for (Entry<File, Set<Integer>> fileListEntry : stringMapEntry.getValue().entrySet()) {
            File tsFile = fileListEntry.getKey();
            Set<Integer> chunks = fileListEntry.getValue();
            System.out.printf(
                "%s - %s - %s - %s - %s chunks\n",
                endPoint, consensusGroupId, uuid, tsFile, chunks.size());
            //            if (consensusGroupId.getId() == 0) {
            //              // d1, non-aligned series
            //              assertEquals(expectedChunkNum() / 2, chunks.size());
            //            } else {
            //              // d2, aligned series
            //              assertEquals(expectedChunkNum() / 2 / seriesNum, chunks.size());
            //            }
          }
        }
      }
    }

    System.out.print("Phase two:\n");
    for (Entry<TEndPoint, Map<ConsensusGroupId, Map<String, Integer>>> endPointMapEntryValue :
        phaseTwoResults.entrySet()) {
      TEndPoint endPoint = endPointMapEntryValue.getKey();
      for (Entry<ConsensusGroupId, Map<String, Integer>> consensusGroupIdMapEntry :
          endPointMapEntryValue.getValue().entrySet()) {
        ConsensusGroupId consensusGroupId = consensusGroupIdMapEntry.getKey();
        for (Entry<String, Integer> stringMapEntry :
            consensusGroupIdMapEntry.getValue().entrySet()) {
          String uuid = stringMapEntry.getKey();
          int command = stringMapEntry.getValue();
          System.out.printf(
              "%s - %s - %s - %s\n",
              endPoint, consensusGroupId, uuid, LoadCommand.values()[command]);
          assertEquals(LoadCommand.EXECUTE.ordinal(), command);
        }
      }
    }
  }
}
