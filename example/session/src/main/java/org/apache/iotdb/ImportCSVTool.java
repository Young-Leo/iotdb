package org.apache.iotdb;

import java.io.FileInputStream;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class ImportCSVTool {

  private static final DateTimeFormatter formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

  private static SessionPool sessionPool;
  private static ExecutorService sessionService;

  private static ExecutorService loaderService;

  private static final String attributesDB = "root.attributes";

  private static final Map<String, Long> timeStringCache = new ConcurrentHashMap<>();

  private static final Set<String> nameSet = ConcurrentHashMap.newKeySet();

  public static void main(String[] args)
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    constructRedirectSessionPool();

    sessionService = Executors.newFixedThreadPool(20);
    loaderService = Executors.newFixedThreadPool(20);

    try {
      sessionPool.createDatabase("root.readings");
      sessionPool.createDatabase("root.diagnostics");
      sessionPool.executeNonQueryStatement("create schema template readings aligned (latitude DOUBLE, longitude DOUBLE, elevation INT32, velocity INT32, heading INT32, grade INT32,fuel_consumption DOUBLE);");
      sessionPool.executeNonQueryStatement("create schema template diagnostics aligned (fuel_state DOUBLE, current_load INT32, status INT32);");
      sessionPool.executeNonQueryStatement("set schema template readings to root.readings;");
      sessionPool.executeNonQueryStatement("set schema template diagnostics to root.diagnostics;");
    } catch (Exception ignore) {
      // do nothing
    }

    String folder = "/Volumes/ExtHD/ExtDocuments/csv";
    if (args.length >= 1) {
      folder = args[0];
    }
    List<Future<?>> futures = new LinkedList<>();
    File folderFile = SystemFileFactory.INSTANCE.getFile(folder);
    long startTime = System.currentTimeMillis();
    if (folderFile.isDirectory()) {
      File[] files =
          FSFactoryProducer.getFSFactory().listFilesBySuffix(folderFile.getAbsolutePath(), ".csv");

      for (File file : files) {
        if (file.getName().contains("reading")) {
          futures.add(loaderService.submit(() -> importCsvReading(file, file.getName().contains("null"))));
        } else {
          futures.add(loaderService.submit(() -> importCsvDiagnostics(file, file.getName().contains("null"))));
        }
      }
      for (Future<?> task : futures) {
        try {
          task.get();
        } catch (ExecutionException | InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      System.out.println("Import " + folder + " finished. Total cost: " + (System.currentTimeMillis() - startTime));
    } else {
      if (folderFile.getName().contains("reading")) {
        futures.add(loaderService.submit(() -> importCsvReading(folderFile, folderFile.getName().contains("null"))));
      } else {
        futures.add(loaderService.submit(() -> importCsvDiagnostics(folderFile, folderFile.getName().contains("null"))));
      }
      for (Future<?> task : futures) {
        try {
          task.get();
        } catch (ExecutionException | InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      System.out.println("Import " + folder + " finished. Total cost: " + (System.currentTimeMillis() - startTime));
    }
    loaderService.shutdown();
    sessionService.shutdown();
  }

  private static void constructRedirectSessionPool() {
    List<String> nodeUrls = new ArrayList<>();
    nodeUrls.add("127.0.0.1:6667");
    //    nodeUrls.add("127.0.0.1:6668");
    sessionPool =
        new SessionPool.Builder()
            .nodeUrls(nodeUrls)
            .user("root")
            .password("root")
            .maxSize(20)
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

    try (CSVParser csvRecords = readCsvFile(file.getAbsolutePath())) {
      long startTime = System.currentTimeMillis();
      long total = file.length();
      long progressInterval = total / 100;
      Stream<CSVRecord> records = csvRecords.stream();
      AtomicInteger currentProgress = new AtomicInteger();

      final String[] currentDeviceId = {null};
      records.forEach(
          recordObj -> {
            String name = isNameNull ? "r_null" : recordObj.get(8);
            String deviceId =
                database
                    + "."
                    + recordObj.get(9)
                    + ".`"
                    + recordObj.get(11)
                    + "`."
                    + name
                    + "."
                    + recordObj.get(10);
            if (!deviceId.equals(currentDeviceId[0])) {
              if (!nameSet.contains(name)) {
                nameSet.add(name);
                String attrDevice = attributesDB + "." + name + ".attr";
                Map<String, String> attr = new HashMap<>(4);
                attr.put("device_version", recordObj.get(12));
                attr.put("load_capacity", recordObj.get(13));
                attr.put("fuel_capacity", recordObj.get(14));
                attr.put("nominal_fuel_consumption", recordObj.get(15));
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
                sessionService.submit(
                    () -> {
                      try {
                        sessionPool.insertAlignedTablet(oldTablet);
                      } catch (IoTDBConnectionException | StatementExecutionException e) {
                        throw new RuntimeException(e);
                      }
                      oldTablet.reset();
                    });
              }
              currentDeviceId[0] = deviceId;
            } else {
              Tablet tablet = tabletMap.get(currentDeviceId[0]);
              if (tablet.rowSize == tablet.getMaxRowNumber()) {

                Tablet oldTablet = tabletMap.remove(currentDeviceId[0]);
                sessionService.submit(
                    () -> {
                      try {
                        sessionPool.insertAlignedTablet(oldTablet);
                      } catch (IoTDBConnectionException | StatementExecutionException e) {
                        throw new RuntimeException(e);
                      }
                      oldTablet.reset();
                    });
              }
            }
            Tablet tablet = tabletMap.computeIfAbsent(deviceId, k -> new Tablet(deviceId, schemas));
            tablet.addTimestamp(tablet.rowSize, castTime(recordObj.get(0)));
            for (int i = 0; i < schemas.size(); i++) {
              MeasurementSchema schema = schemas.get(i);
              tablet.addValue(
                  schema.getMeasurementId(),
                  tablet.rowSize,
                  castValue(schema.getType(), recordObj.get(i + 1)));
            }
            tablet.rowSize++;
            if (recordObj.getCharacterPosition() / progressInterval > currentProgress.get()) {
              currentProgress.set((int) (recordObj.getCharacterPosition() / progressInterval));
              System.out.println(
                  file.getName()
                      + " Progress: "
                      + currentProgress.get()
                      + "% cost "
                      + (System.currentTimeMillis() - startTime));
            }
          });
      if (!tabletMap.isEmpty()) {
        for (Tablet tablet : tabletMap.values()) {
          sessionPool.insertAlignedTablet(tablet);
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

    try (CSVParser csvRecords = readCsvFile(file.getAbsolutePath())) {
      long startTime = System.currentTimeMillis();
      long total = file.length();
      long progressInterval = total / 100;
      Stream<CSVRecord> records = csvRecords.stream();
      AtomicInteger currentProgress = new AtomicInteger();

      final String[] currentDeviceId = {null};
      records.forEach(
          recordObj -> {
            String name = isNameNull ? "d_null" : recordObj.get(4);
            String deviceId =
                database
                    + "."
                    + recordObj.get(5)
                    + ".`"
                    + recordObj.get(7)
                    + "`."
                    + name
                    + "."
                    + recordObj.get(6);
            if (!deviceId.equals(currentDeviceId[0])) {
              if (!nameSet.contains(name)) {
                nameSet.add(name);
                String attrDevice = attributesDB + "." + name + ".attr";
                Map<String, String> attr = new HashMap<>(4);
                attr.put("device_version", recordObj.get(8));
                attr.put("load_capacity", recordObj.get(9));
                attr.put("fuel_capacity", recordObj.get(10));
                attr.put("nominal_fuel_consumption", recordObj.get(11));
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
                sessionService.submit(
                    () -> {
                      try {
                        sessionPool.insertAlignedTablet(oldTablet);
                      } catch (IoTDBConnectionException | StatementExecutionException e) {
                        throw new RuntimeException(e);
                      }
                      oldTablet.reset();
                    });
              }
              currentDeviceId[0] = deviceId;
            } else {
              Tablet tablet = tabletMap.get(currentDeviceId[0]);
              if (tablet.rowSize == tablet.getMaxRowNumber()) {

                Tablet oldTablet = tabletMap.remove(currentDeviceId[0]);
                sessionService.submit(
                    () -> {
                      try {
                        sessionPool.insertAlignedTablet(oldTablet);
                      } catch (IoTDBConnectionException | StatementExecutionException e) {
                        throw new RuntimeException(e);
                      }
                      oldTablet.reset();
                    });
              }
            }
            Tablet tablet = tabletMap.computeIfAbsent(deviceId, k -> new Tablet(deviceId, schemas));
            tablet.addTimestamp(tablet.rowSize, castTime(recordObj.get(0)));
            for (int i = 0; i < schemas.size(); i++) {
              MeasurementSchema schema = schemas.get(i);
              tablet.addValue(
                  schema.getMeasurementId(),
                  tablet.rowSize,
                  castValue(schema.getType(), recordObj.get(i + 1)));
            }
            tablet.rowSize++;
            if (recordObj.getCharacterPosition() / progressInterval > currentProgress.get()) {
              currentProgress.set((int) (recordObj.getCharacterPosition() / progressInterval));
              System.out.println(
                  file.getName()
                      + " Progress: "
                      + currentProgress.get()
                      + "% cost "
                      + (System.currentTimeMillis() - startTime));
            }
          });
      if (!tabletMap.isEmpty()) {
        for (Tablet tablet : tabletMap.values()) {
          sessionPool.insertAlignedTablet(tablet);
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

  private static CSVParser readCsvFile(String path) throws IOException {
    return CSVFormat.Builder.create(CSVFormat.DEFAULT)
        .setHeader()
        .setSkipHeaderRecord(true)
        .setQuote('`')
        .setEscape('\\')
        .setIgnoreEmptyLines(true)
        .build()
        .parse(new InputStreamReader(new FileInputStream(path)));
  }
}
