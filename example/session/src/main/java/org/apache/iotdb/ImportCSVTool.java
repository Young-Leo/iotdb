package org.apache.iotdb;

import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public class ImportCSVTool {

  private static final DateTimeFormatter formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

  private static SessionPool sessionPool;
  private static ExecutorService sessionService;

  private static ExecutorService loaderService;

  private static ScheduledExecutorService logger;

  private static final String attributesDB = "root.attributes";

  private static final Map<String, Long> timeStringCache = new ConcurrentHashMap<>();

  private static final Set<String> nameSet = ConcurrentHashMap.newKeySet();

  private static final ArrayBlockingQueue<Boolean> queue = new ArrayBlockingQueue<>(400);

  private static final AtomicLong totalPoints = new AtomicLong();

  private static final AtomicLong totalInsertTime = new AtomicLong();
  private static final AtomicLong totalParseTime = new AtomicLong();
  private static final AtomicLong totalInsertCount = new AtomicLong();

  private static int tabletSize;

  public static void main(String[] args)
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    int concurrentNum = Integer.parseInt(args[1]);
    constructRedirectSessionPool(args[0], concurrentNum);
    tabletSize = Integer.parseInt(args[2]);

    sessionService = Executors.newFixedThreadPool(concurrentNum);
    loaderService = Executors.newFixedThreadPool(22);

    try {
      sessionPool.createDatabase("root.readings");
      sessionPool.createDatabase("root.diagnostics");
      sessionPool.executeNonQueryStatement(
          "create schema template readings aligned (latitude DOUBLE, longitude DOUBLE, elevation INT32, velocity INT32, heading INT32, grade INT32,fuel_consumption DOUBLE);");
      sessionPool.executeNonQueryStatement(
          "create schema template diagnostics aligned (fuel_state DOUBLE, current_load INT32, status INT32);");
      sessionPool.executeNonQueryStatement("set schema template readings to root.readings;");
      sessionPool.executeNonQueryStatement("set schema template diagnostics to root.diagnostics;");
      System.out.println("Create schema finished.");
    } catch (Exception ignore) {
      // do nothing
    }

    String folder = "/data/tsbs/csvFile";
    if (args.length >= 4) {
      folder = args[3];
    }
    List<Future<?>> futures = new LinkedList<>();
    File folderFile = SystemFileFactory.INSTANCE.getFile(folder);
    Date currentDate = new Date();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    String currentTime = sdf.format(currentDate);
    System.out.println("Start importing at " + currentTime);
    long startTime = System.currentTimeMillis();

    logger = Executors.newScheduledThreadPool(1);
    logger.scheduleAtFixedRate(
        () -> {
          long points = totalPoints.get();
          long time = (System.currentTimeMillis() - startTime) / 1000;
          double throughout = (double) points / time;
          System.out.println("Current points: " + points);
          System.out.println("Current Time cost: " + time + " s");
          System.out.println("Throughput: " + throughout + " points/s");
          long insertTime = totalInsertTime.get();
          long insertCount = totalInsertCount.get();
          if (insertCount > 0) {
            System.out.println("Avg insertTablet CPU time: " + (insertTime / insertCount) + " ms");
            System.out.println("Total insertTablet CPU time: " + insertTime + " ms");
          }
          long parseTime = totalParseTime.get();
          System.out.println("Total parse CPU time: " + parseTime + " ms");
        },
        0,
        1,
        TimeUnit.MINUTES);
    if (folderFile.isDirectory()) {
      File[] files =
          FSFactoryProducer.getFSFactory().listFilesBySuffix(folderFile.getAbsolutePath(), ".csv");

      for (File file : files) {
        if (file.getName().contains("reading")) {
          futures.add(
              loaderService.submit(() -> importCsvReading(file, file.getName().contains("null"))));
        } else {
          futures.add(
              loaderService.submit(
                  () -> importCsvDiagnostics(file, file.getName().contains("null"))));
        }
      }
      for (Future<?> task : futures) {
        try {
          task.get();
        } catch (ExecutionException | InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      currentDate = new Date();
      currentTime = sdf.format(currentDate);
      System.out.println(
          "Import "
              + folder
              + " finished at "
              + currentTime
              + " Total cost: "
              + (System.currentTimeMillis() - startTime)
              + " ms");
    } else {
      if (folderFile.getName().contains("reading")) {
        futures.add(
            loaderService.submit(
                () -> importCsvReading(folderFile, folderFile.getName().contains("null"))));
      } else {
        futures.add(
            loaderService.submit(
                () -> importCsvDiagnostics(folderFile, folderFile.getName().contains("null"))));
      }

      for (Future<?> task : futures) {
        try {
          task.get();
        } catch (ExecutionException | InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      System.out.println(
          "Import "
              + folder
              + " finished. Total cost "
              + (System.currentTimeMillis() - startTime)
              + " ms");
    }
    loaderService.shutdown();
    sessionService.shutdown();
    logger.shutdown();
  }

  private static void constructRedirectSessionPool(String host, int concurrentNum) {
    sessionPool =
        new SessionPool.Builder()
            .host(host)
            .port(6667)
            .user("root")
            .password("root")
            .maxSize(concurrentNum)
            .build();
  }

  public static void importCsvReading(File file, boolean isNameNull) {
    Map<String, Tablet> tabletMap = new ConcurrentHashMap<>();
    String database = "root.readings";
    List<MeasurementSchema> schemas = new ArrayList<>();
    schemas.add(new MeasurementSchema("latitude", TSDataType.DOUBLE));
    schemas.add(new MeasurementSchema("longitude", TSDataType.DOUBLE));
    schemas.add(new MeasurementSchema("elevation", TSDataType.INT32));
    schemas.add(new MeasurementSchema("velocity", TSDataType.INT32));
    schemas.add(new MeasurementSchema("heading", TSDataType.INT32));
    schemas.add(new MeasurementSchema("grade", TSDataType.INT32));
    schemas.add(new MeasurementSchema("fuel_consumption", TSDataType.DOUBLE));

    try (Stream<String> lines = Files.lines(Paths.get(file.getAbsolutePath()))) {
      long startTime = System.currentTimeMillis();
      long total = file.length();
      long progressInterval = total / 100;
      AtomicInteger currentProgress = new AtomicInteger();
      AtomicLong position = new AtomicLong();
      final String[] currentDeviceId = {null};
      lines.forEach(
          line -> {
            if (position.getAndAdd(line.length()) > 0) {
              long parseStart = System.currentTimeMillis();
              String[] recordObj = line.split(",");
              String name = isNameNull ? "r_null" : recordObj[8];
              String deviceId =
                  database
                      + "."
                      + recordObj[9]
                      + ".`"
                      + recordObj[11]
                      + "`."
                      + name
                      + "."
                      + recordObj[10];
              if (!deviceId.equals(currentDeviceId[0])) {
                if (!nameSet.contains(name)) {
                  nameSet.add(name);
                  String attrDevice = attributesDB + "." + name + ".attr";
                  Map<String, String> attr = new HashMap<>(4);
                  attr.put("device_version", recordObj[12]);
                  attr.put("load_capacity", recordObj[13]);
                  attr.put("fuel_capacity", recordObj[14]);
                  attr.put("nominal_fuel_consumption", recordObj[15]);
                  sessionService.submit(
                      () -> {
                        try {
                          sessionPool.createTimeseries(
                              attrDevice,
                              TSDataType.BOOLEAN,
                              TSEncoding.RLE,
                              CompressionType.LZ4,
                              null,
                              null,
                              attr,
                              null);
                        } catch (IoTDBConnectionException | StatementExecutionException e) {
                          throw new RuntimeException(e);
                        }
                      });
                }
                if (currentDeviceId[0] != null) {
                  Tablet oldTablet = tabletMap.remove(currentDeviceId[0]);
                  try {
                    queue.put(true);
                  } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                  }
                  sessionService.submit(
                      () -> {
                        try {
                          long start = System.currentTimeMillis();
                          sessionPool.insertAlignedTablet(oldTablet);
                          totalInsertTime.addAndGet(System.currentTimeMillis() - start);
                          totalPoints.addAndGet(
                              (long) oldTablet.rowSize * oldTablet.getSchemas().size());
                          totalInsertCount.addAndGet(1);
                        } catch (IoTDBConnectionException | StatementExecutionException e) {
                          throw new RuntimeException(e);
                        } finally {
                          try {
                            queue.take();
                          } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                          }
                        }
                      });
                }
                currentDeviceId[0] = deviceId;
              } else {
                Tablet tablet = tabletMap.get(currentDeviceId[0]);
                if (tablet.rowSize == tablet.getMaxRowNumber()) {
                  Tablet oldTablet = tabletMap.remove(currentDeviceId[0]);
                  try {
                    queue.put(true);
                  } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                  }
                  sessionService.submit(
                      () -> {
                        try {
                          long start = System.currentTimeMillis();
                          sessionPool.insertAlignedTablet(oldTablet);
                          totalInsertTime.addAndGet(System.currentTimeMillis() - start);
                          totalPoints.addAndGet(
                              (long) oldTablet.rowSize * oldTablet.getSchemas().size());
                          totalInsertCount.addAndGet(1);
                        } catch (IoTDBConnectionException | StatementExecutionException e) {
                          throw new RuntimeException(e);
                        } finally {
                          try {
                            queue.take();
                          } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                          }
                        }
                      });
                }
              }
              Tablet tablet =
                  tabletMap.computeIfAbsent(
                      deviceId, k -> new Tablet(deviceId, schemas, tabletSize));
              tablet.addTimestamp(tablet.rowSize, castTime(recordObj[0]));
              for (int i = 0; i < schemas.size(); i++) {
                MeasurementSchema schema = schemas.get(i);
                tablet.addValue(
                    schema.getMeasurementId(),
                    tablet.rowSize,
                    castValue(schema.getType(), recordObj[i + 1]));
              }
              tablet.rowSize++;
              if (position.get() / progressInterval > currentProgress.get()) {
                currentProgress.set((int) (position.get() / progressInterval));
                System.out.println(
                    file.getName()
                        + " Progress: "
                        + currentProgress.get()
                        + "% cost "
                        + (System.currentTimeMillis() - startTime)
                        + " ms");
              }
              totalParseTime.addAndGet(System.currentTimeMillis() - parseStart);
            }
          });
      if (!tabletMap.isEmpty()) {
        for (Tablet tablet : tabletMap.values()) {
          long start = System.currentTimeMillis();
          sessionPool.insertAlignedTablet(tablet);
          totalInsertTime.addAndGet(System.currentTimeMillis() - start);
          totalPoints.addAndGet((long) tablet.rowSize * tablet.getSchemas().size());
          totalInsertCount.addAndGet(1);
        }
        tabletMap.clear();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void importCsvDiagnostics(File file, boolean isNameNull) {
    Map<String, Tablet> tabletMap = new ConcurrentHashMap<>();
    String database = "root.diagnostics";
    List<MeasurementSchema> schemas = new ArrayList<>();
    schemas.add(new MeasurementSchema("fuel_state", TSDataType.DOUBLE));
    schemas.add(new MeasurementSchema("current_load", TSDataType.INT32));
    schemas.add(new MeasurementSchema("status", TSDataType.INT32));

    try (Stream<String> lines = Files.lines(Paths.get(file.getAbsolutePath()))) {
      long startTime = System.currentTimeMillis();
      long total = file.length();
      long progressInterval = total / 100;
      AtomicInteger currentProgress = new AtomicInteger();
      AtomicLong position = new AtomicLong();
      final String[] currentDeviceId = {null};
      lines.forEach(
          line -> {
            long parseStart = System.currentTimeMillis();
            if (position.getAndAdd(line.length()) > 0) {
              String[] recordObj = line.split(",");
              String name = isNameNull ? "d_null" : recordObj[4];
              String deviceId =
                  database
                      + "."
                      + recordObj[5]
                      + ".`"
                      + recordObj[7]
                      + "`."
                      + name
                      + "."
                      + recordObj[6];
              if (!deviceId.equals(currentDeviceId[0])) {
                if (!nameSet.contains(name)) {
                  nameSet.add(name);
                  String attrDevice = attributesDB + "." + name + ".attr";
                  Map<String, String> attr = new HashMap<>(4);
                  attr.put("device_version", recordObj[8]);
                  attr.put("load_capacity", recordObj[9]);
                  attr.put("fuel_capacity", recordObj[10]);
                  attr.put("nominal_fuel_consumption", recordObj[11]);
                  sessionService.submit(
                      () -> {
                        try {
                          sessionPool.createTimeseries(
                              attrDevice,
                              TSDataType.BOOLEAN,
                              TSEncoding.RLE,
                              CompressionType.LZ4,
                              null,
                              null,
                              attr,
                              null);
                        } catch (IoTDBConnectionException | StatementExecutionException e) {
                          throw new RuntimeException(e);
                        }
                      });
                }
                if (currentDeviceId[0] != null) {
                  Tablet oldTablet = tabletMap.remove(currentDeviceId[0]);
                  try {
                    queue.put(true);
                  } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                  }
                  sessionService.submit(
                      () -> {
                        try {
                          long start = System.currentTimeMillis();
                          sessionPool.insertAlignedTablet(oldTablet);
                          totalInsertTime.addAndGet(System.currentTimeMillis() - start);
                          totalPoints.addAndGet(
                              (long) oldTablet.rowSize * oldTablet.getSchemas().size());
                          totalInsertCount.addAndGet(1);
                        } catch (IoTDBConnectionException | StatementExecutionException e) {
                          throw new RuntimeException(e);
                        } finally {
                          try {
                            queue.take();
                          } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                          }
                        }
                      });
                }
                currentDeviceId[0] = deviceId;
              } else {
                Tablet tablet = tabletMap.get(currentDeviceId[0]);
                if (tablet.rowSize == tablet.getMaxRowNumber()) {
                  Tablet oldTablet = tabletMap.remove(currentDeviceId[0]);
                  try {
                    queue.put(true);
                  } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                  }
                  sessionService.submit(
                      () -> {
                        try {
                          long start = System.currentTimeMillis();
                          sessionPool.insertAlignedTablet(oldTablet);
                          totalInsertTime.addAndGet(System.currentTimeMillis() - start);
                          totalPoints.addAndGet(
                              (long) oldTablet.rowSize * oldTablet.getSchemas().size());
                          totalInsertCount.addAndGet(1);
                        } catch (IoTDBConnectionException | StatementExecutionException e) {
                          throw new RuntimeException(e);
                        } finally {
                          try {
                            queue.take();
                          } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                          }
                        }
                      });
                }
              }
              Tablet tablet =
                  tabletMap.computeIfAbsent(
                      deviceId, k -> new Tablet(deviceId, schemas, tabletSize));
              tablet.addTimestamp(tablet.rowSize, castTime(recordObj[0]));
              for (int i = 0; i < schemas.size(); i++) {
                MeasurementSchema schema = schemas.get(i);
                tablet.addValue(
                    schema.getMeasurementId(),
                    tablet.rowSize,
                    castValue(schema.getType(), recordObj[i + 1]));
              }
              tablet.rowSize++;
              if (position.get() / progressInterval > currentProgress.get()) {
                currentProgress.set((int) (position.get() / progressInterval));
                System.out.println(
                    file.getName()
                        + " Progress: "
                        + currentProgress.get()
                        + "% cost "
                        + (System.currentTimeMillis() - startTime)
                        + " ms");
              }
              totalParseTime.addAndGet(System.currentTimeMillis() - parseStart);
            }
          });
      if (!tabletMap.isEmpty()) {
        for (Tablet tablet : tabletMap.values()) {
          long start = System.currentTimeMillis();
          sessionPool.insertAlignedTablet(tablet);
          totalInsertTime.addAndGet(System.currentTimeMillis() - start);
          totalPoints.addAndGet((long) tablet.rowSize * tablet.getSchemas().size());
          totalInsertCount.addAndGet(1);
        }
        tabletMap.clear();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static Object castValue(TSDataType dataType, String str) {
    try {
      if (dataType == TSDataType.INT32) {
        return Integer.parseInt(str);
      } else if (dataType == TSDataType.DOUBLE) {
        return Double.parseDouble(str);
      }
    } catch (NumberFormatException e) {
      if ("NULL".equals(str)) {
        return null;
      }
      throw e;
    }
    return str;
  }

  private static long castTime(String str) {
    return timeStringCache.computeIfAbsent(
        str, k -> OffsetDateTime.parse(str, formatter).toInstant().toEpochMilli());
  }
}
