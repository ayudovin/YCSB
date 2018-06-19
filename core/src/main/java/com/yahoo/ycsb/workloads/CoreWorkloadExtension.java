/**
 * Copyright (c) 2010 Yahoo! Inc., Copyright (c) 2016-2017 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.RandomByteIterator;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.DiscreteGenerator;
import com.yahoo.ycsb.generator.NumberGenerator;
import com.yahoo.ycsb.generator.UniformLongGenerator;
import com.yahoo.ycsb.generator.ZipfianGenerator;
import com.yahoo.ycsb.model.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Vector;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The core benchmark scenario. Represents a set of clients doing simple CRUD operations. The
 * relative proportion of different kinds of operations, and other properties of the workload,
 * are controlled by parameters specified at runtime.
 * <p>
 * Properties to control the client:
 * <UL>
 * <LI><b>fieldcount</b>: the number of fields in a record (default: 10)
 * <LI><b>fieldlength</b>: the size of each field (default: 100)
 * <LI><b>readallfields</b>: should reads read all fields (true) or just one (false) (default: true)
 * <LI><b>writeallfields</b>: should updates and read/modify/writes update all fields (true) or just
 * one (false) (default: false)
 * <LI><b>readproportion</b>: what proportion of operations should be reads (default: 0.95)
 * <LI><b>updateproportion</b>: what proportion of operations should be updates (default: 0.05)
 * <LI><b>insertproportion</b>: what proportion of operations should be inserts (default: 0)
 * <LI><b>scanproportion</b>: what proportion of operations should be scans (default: 0)
 * <LI><b>readmodifywriteproportion</b>: what proportion of operations should be read a record,
 * modify it, write it back (default: 0)
 * <LI><b>requestdistribution</b>: what distribution should be used to select the records to operate
 * on - uniform, zipfian, hotspot, sequential, exponential or latest (default: uniform)
 * <LI><b>maxscanlength</b>: for scans, what is the maximum number of records to scan (default: 1000)
 * <LI><b>scanlengthdistribution</b>: for scans, what distribution should be used to choose the
 * number of records to scan, for each scan, between 1 and maxscanlength (default: uniform)
 * <LI><b>insertstart</b>: for parallel loads and runs, defines the starting record for this
 * YCSB instance (default: 0)
 * <LI><b>insertcount</b>: for parallel loads and runs, defines the number of records for this
 * YCSB instance (default: recordcount)
 * <LI><b>zeropadding</b>: for generating a record sequence compatible with string sort order by
 * 0 padding the record number. Controls the number of 0s to use for padding. (default: 1)
 * For example for row 5, with zeropadding=1 you get 'user5' key and with zeropading=8 you get
 * 'user00000005' key. In order to see its impact, zeropadding needs to be bigger than number of
 * digits in the record number.
 * <LI><b>insertorder</b>: should records be inserted in order by key ("ordered"), or in hashed
 * order ("hashed") (default: hashed)
 * </ul>
 */
public class CoreWorkloadExtension extends CoreWorkload {

  private static final BiFunction<String, Integer, String> PARSE = (field, type) -> field.split(":")[type];

  /**
   * The default value of a field.
   */
  private static final String FIELD_MODEL_PROPERTY_DEFAULT = "field0:STRING;field1:STRING;field2:STRING;" +
      "field3:STRING;field4:STRING;field5:STRING;field6:STRING;field7:STRING;field8:STRING;field9:STRING;";

  /**
   * The name of property for the model.
   */
  private static final String FIELD_MODEL_PROPERTY = "model";

  /**
   * The name of the property for the max query length (to use in LIMIT clause).
   */
  public static final String MAX_QUERY_LENGTH_PROPERTY = "maxquerylength";

  /**
   * The default max query length.
   */
  public static final String MAX_QUERY_LENGTH_PROPERTY_DEFAULT = "1000";

  /**
   * The name of the property for the query llength distribution. Options are "uniform" and "zipfian"
   * (favoring short queries)
   */
  public static final String QUERY_LENGTH_DISTRIBUTION_PROPERTY = "querylengthdistribution";

  /**
   * The default query length distribution.
   */
  public static final String QUERY_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";

  /**
   * The name of the property for the max query offset (to use in OFFSET clause).
   */
  public static final String MAX_QUERY_OFFSET_PROPERTY = "maxqueryoffset";

  /**
   * The default max query offset.
   */
  public static final String MAX_QUERY_OFFSET_PROPERTY_DEFAULT = "20";

  /**
   * The name of the property for the query offset distribution. Options are "uniform" and "zipfian"
   * (favoring short queries)
   */
  public static final String QUERY_OFFSET_DISTRIBUTION_PROPERTY = "queryoffsetdistribution";

  /**
   * The default query offset distribution.
   */
  public static final String QUERY_OFFSET_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";

  /**
   * The name of the property for the proportion of transactions that are of query 1 type -
   * Filter​ with OFFSET and LIMIT.
   */
  public static final String QUERY_1_PROPORTION_PROPERTY = "query1proportion";

  /**
   * The default proportion of transactions that are of query 1 type.
   */
  public static final String QUERY_1_PROPORTION_PROPERTY_DEFAULT = "0";

  /**
   * The name of the property defining field to filter by in query 1 workload.
   */
  public static final String QUERY_1_FILTER_FIELD_NAME = "query1filterfield";

  /**
   * The name of the property defining existing field values to filter by in
   * query 1 workload.
   */
  public static final String QUERY_1_FILTER_FIELD_VALUES = "query1filtervalues";

  /**
   * The name of the property for the proportion of transactions that are of query 2 type -
   * ​JOIN​ with GROUP BY and ORDER BY.
   */
  public static final String QUERY_2_PROPORTION_PROPERTY = "query2proportion";

  /**
   * The default proportion of transactions that are of query 2 type.
   */
  public static final String QUERY_2_PROPORTION_PROPERTY_DEFAULT = "0";

  /**
   * The name of the property defining the first field to filter by in query 2 workload.
   */
  public static final String QUERY_2_FILTER_FIELD_1_NAME = "query2filterfield1";

  /**
   * The name of the property defining existing field values to filter by first field in
   * query 2 workload.
   */
  public static final String QUERY_2_FILTER_FIELD_VALUES_1 = "query2filtervalues1";

  /**
   * The name of the property defining the second field to filter by in query 2 workload.
   */
  public static final String QUERY_2_FILTER_FIELD_2_NAME = "query2filterfield2";

  /**
   * The name of the property defining existing field values to filter by second field in
   * query 2 workload.
   */
  public static final String QUERY_2_FILTER_FIELD_VALUES_2 = "query2filtervalues2";

  private static final Integer NAME = 0;
  private static final Integer TYPE = 1;

  private Map<String, Type> model;

  private NumberGenerator queryoffset;
  private NumberGenerator querylength;
  private String query1FilterField;
  private List<String> query1FilterValues;
  private NumberGenerator query1FilterValueIndexGenerator;

  private String query2FilterField1;
  private List<String> query2FilterValues1;
  private NumberGenerator query2FilterValue1IndexGenerator;
  private String query2FilterField2;
  private List<String> query2FilterValues2;
  private NumberGenerator query2FilterValue2IndexGenerator;

  @Override
  public void init(Properties p) throws WorkloadException {
    super.init(p);

    fieldcount = Long.parseLong(p.getProperty(FIELD_COUNT_PROPERTY, FIELD_COUNT_PROPERTY_DEFAULT));

    model = Stream.of(p.getProperty(FIELD_MODEL_PROPERTY, FIELD_MODEL_PROPERTY_DEFAULT).split(";"))
        .collect(Collectors.toMap(s -> PARSE.apply(s, NAME), s -> Type.valueOf(PARSE.apply(s, TYPE))));

    fieldnames = new ArrayList<>(model.keySet());
    fieldcount = Math.min(this.fieldcount, this.fieldnames.size());

    fieldchooser = new UniformLongGenerator(0, fieldcount - 1);

    query1FilterField = p.getProperty(QUERY_1_FILTER_FIELD_NAME);
    query1FilterValues = Arrays.asList(p.getProperty(QUERY_1_FILTER_FIELD_VALUES, "").split(","));

    int maxquerylength =
        Integer.parseInt(p.getProperty(MAX_QUERY_LENGTH_PROPERTY, MAX_QUERY_LENGTH_PROPERTY_DEFAULT));
    String querylengthdistrib =
        p.getProperty(QUERY_LENGTH_DISTRIBUTION_PROPERTY, QUERY_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);

    if (querylengthdistrib.compareTo("uniform") == 0) {
      querylength = new UniformLongGenerator(1, maxquerylength);
    } else if (querylengthdistrib.compareTo("zipfian") == 0) {
      querylength = new ZipfianGenerator(1, maxquerylength);
    } else {
      throw new WorkloadException(
          "Distribution \"" + querylengthdistrib + "\" not allowed for query length");
    }

    int maxoffsetlength =
        Integer.parseInt(p.getProperty(MAX_QUERY_OFFSET_PROPERTY, MAX_QUERY_OFFSET_PROPERTY_DEFAULT));
    String queryoffsetdistrib =
        p.getProperty(QUERY_OFFSET_DISTRIBUTION_PROPERTY, QUERY_OFFSET_DISTRIBUTION_PROPERTY_DEFAULT);

    if (queryoffsetdistrib.compareTo("uniform") == 0) {
      queryoffset = new UniformLongGenerator(1, maxoffsetlength);
    } else if (queryoffsetdistrib.compareTo("zipfian") == 0) {
      queryoffset = new ZipfianGenerator(1, maxoffsetlength);
    } else {
      throw new WorkloadException(
          "Distribution \"" + queryoffsetdistrib + "\" not allowed for query offset");
    }

    boolean isQuery1 = Double.parseDouble(
        p.getProperty(QUERY_1_PROPORTION_PROPERTY, QUERY_1_PROPORTION_PROPERTY_DEFAULT)) > 0;
    if (isQuery1) {
      if (Objects.isNull(query1FilterField) || query1FilterField.length() == 0) {
        throw new WorkloadException(
            "Query 1 filter field must be specified via " + QUERY_1_FILTER_FIELD_NAME + " property"
        );
      }

      if (query1FilterValues.size() == 0) {
        throw new WorkloadException(
            "Please, specify at least 1 query 1 filter value via " + QUERY_1_FILTER_FIELD_VALUES + " property"
        );
      }

      query1FilterValueIndexGenerator = new UniformLongGenerator(0, query1FilterValues.size() - 1);
    }

    query2FilterField1 = p.getProperty(QUERY_2_FILTER_FIELD_1_NAME);
    query2FilterValues1 = Arrays.asList(p.getProperty(QUERY_2_FILTER_FIELD_VALUES_1, "").split(","));
    query2FilterField2 = p.getProperty(QUERY_2_FILTER_FIELD_2_NAME);
    query2FilterValues2 = Arrays.asList(p.getProperty(QUERY_2_FILTER_FIELD_VALUES_2, "").split(","));
    boolean isQuery2 = Double.parseDouble(
        p.getProperty(QUERY_2_PROPORTION_PROPERTY, QUERY_2_PROPORTION_PROPERTY_DEFAULT)) > 0;
    if (isQuery2) {
      if (Objects.isNull(query2FilterField1) || query2FilterField1.length() == 0) {
        throw new WorkloadException(
            "Query 2 filter field 1 must be specified via " + QUERY_2_FILTER_FIELD_1_NAME + " property"
        );
      }
      if (Objects.isNull(query2FilterField2) || query2FilterField2.length() == 0) {
        throw new WorkloadException(
            "Query 2 filter field 2 must be specified via " + QUERY_2_FILTER_FIELD_2_NAME + " property"
        );
      }

      if (query2FilterValues1.size() == 0) {
        throw new WorkloadException(
            "Please, specify at least 1 query 2 filter value 1 via " + QUERY_2_FILTER_FIELD_VALUES_1 + " property"
        );
      }

      if (query2FilterValues2.size() == 0) {
        throw new WorkloadException(
            "Please, specify at least 1 query 2 filter value 2 via " + QUERY_2_FILTER_FIELD_VALUES_2 + " property"
        );
      }

      query2FilterValue1IndexGenerator = new UniformLongGenerator(0, query2FilterValues1.size() - 1);
      query2FilterValue2IndexGenerator = new UniformLongGenerator(0, query2FilterValues2.size() - 1);
    }

    operationchooser = createOperationGenerator(p);
  }

  /**
   * Creates a weighted discrete values with database operations for a workload to perform.
   * Weights/proportions are read from the properties list and defaults are used
   * when values are not configured.
   * Current operations are "READ", "UPDATE", "INSERT", "SCAN" and "READMODIFYWRITE".
   *
   * @param p The properties list to pull weights from.
   * @return A generator that can be used to determine the next operation to perform.
   * @throws IllegalArgumentException if the properties object was null.
   */
  protected static DiscreteGenerator createOperationGenerator(final Properties p) {
    DiscreteGenerator operationchooser = CoreWorkload.createOperationGenerator(p);

    final double query1Proportion = Double.parseDouble(
        p.getProperty(QUERY_1_PROPORTION_PROPERTY, QUERY_1_PROPORTION_PROPERTY_DEFAULT));
    final double query2Proportion = Double.parseDouble(
        p.getProperty(QUERY_2_PROPORTION_PROPERTY, QUERY_2_PROPORTION_PROPERTY_DEFAULT));

    if (query1Proportion > 0) {
      operationchooser.addValue(query1Proportion, "QUERY_1");
    }

    if (query2Proportion > 0) {
      operationchooser.addValue(query2Proportion, "QUERY_2");
    }

    return operationchooser;
  }

  @Override
  public boolean doInsert(DB db, Object threadstate) {
    int keynum = keysequence.nextValue().intValue();
    String dbkey = buildKeyName(keynum);
    HashMap<String, ByteIterator> values = buildValues(dbkey);

    Status status;
    int numOfRetries = 0;
    do {
      status = db.insert(table, dbkey, values, model);
      if (null != status && status.isOk()) {
        break;
      }
      // Retry if configured. Without retrying, the load process will fail
      // even if one single insertion fails. User can optionally configure
      // an insertion retry limit (default is 0) to enable retry.
      if (++numOfRetries <= insertionRetryLimit) {
        System.err.println("Retrying insertion, retry count: " + numOfRetries);
        try {
          // Sleep for a random number between [0.8, 1.2)*insertionRetryInterval.
          int sleepTime = (int) (1000 * insertionRetryInterval * (0.8 + 0.4 * Math.random()));
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          break;
        }

      } else {
        System.err.println("Error inserting, not retrying any more. number of attempts: " + numOfRetries +
            "Insertion Retry Limit: " + insertionRetryLimit);
        break;

      }
    } while (true);

    return null != status && status.isOk();
  }

  /**
   * Do one transaction operation. Because it will be called concurrently from multiple client
   * threads, this function must be thread safe. However, avoid synchronized, or the threads will block waiting
   * for each other, and it will be difficult to reach the target throughput. Ideally, this function would
   * have no side effects other than DB operations.
   */
  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    String operation = operationchooser.nextString();
    if(operation == null) {
      return false;
    }

    switch (operation) {
    case "READ":
      doTransactionRead(db);
      break;
    case "UPDATE":
      doTransactionUpdate(db);
      break;
    case "INSERT":
      doTransactionInsert(db);
      break;
    case "SCAN":
      doTransactionScan(db);
      break;
    case "QUERY_1":
      doTransactionQuery1(db);
      break;
    case "QUERY_2":
      doTransactionQuery2(db);
      break;
    default:
      doTransactionReadModifyWrite(db);
    }

    return true;
  }

  /**
   * Do one transaction operation. Because it will be called concurrently from multiple client
   * threads, this function must be thread safe. However, avoid synchronized, or the threads will block waiting
   * for each other, and it will be difficult to reach the target throughput. Ideally, this function would
   * have no side effects other than DB operations.
   */
  @Override
  public String doTransactionByQuery(DB db, Object threadstate) {
    String operation = operationchooser.nextString();
    if(operation == null) {
      return "FAILED";
    }

    switch (operation) {
    case "READ":
      doTransactionRead(db);
      return "READ";
    case "UPDATE":
      doTransactionUpdate(db);
      return "UPDATE";
    case "INSERT":
      doTransactionInsert(db);
      return "INSERT";
    case "SCAN":
      doTransactionScan(db);
      return "SCAN";
    case "QUERY_1":
      doTransactionQuery1(db);
      return "QUERY_1";
    case "QUERY_2":
      doTransactionQuery2(db);
      return "QUERY_2";
    default:
      doTransactionReadModifyWrite(db);
      return "READ-MODIFY-WRITE";
    }
  }

  @Override
  public void doTransactionInsert(DB db) {
    // choose the next key
    long keynum = transactioninsertkeysequence.nextValue();

    try {
      String dbkey = buildKeyName(keynum);

      HashMap<String, ByteIterator> values = buildValues(dbkey);
      db.insert(table, dbkey, values, model);
    } finally {
      transactioninsertkeysequence.acknowledge(keynum);
    }
  }

  @Override
  public void doTransactionUpdate(DB db) {
    // choose a random key
    long keynum = nextKeynum();

    String keyname = buildKeyName(keynum);

    HashMap<String, ByteIterator> values;

    if (writeallfields) {
      // new data for all the fields
      values = buildValues(keyname);
    } else {
      // update a random field
      values = buildSingleValue(keyname);
    }

    db.update(table, keyname, values, model);
  }

  public void doTransactionQuery1(DB db) {
    // choose filter value
    String filtervalue = query1FilterValues.get(query1FilterValueIndexGenerator.nextValue().intValue());

    // choose a random query offset
    int offset = queryoffset.nextValue().intValue();

    // choose a random query length
    int len = querylength.nextValue().intValue();

    HashSet<String> fields = null;

    if (!readallfields) {
      // read a random field
      String fieldname = fieldnames.get(fieldchooser.nextValue().intValue());

      fields = new HashSet<>();
      fields.add(fieldname);
    }

    db.query1(table, query1FilterField, filtervalue, offset, len, fields, new Vector<>());
  }

  public void doTransactionQuery2(DB db) {
    // choose filter value 1
    String filtervalue1 = query2FilterValues1.get(query2FilterValue1IndexGenerator.nextValue().intValue());
    // choose filter value 1
    String filtervalue2 = query2FilterValues2.get(query2FilterValue2IndexGenerator.nextValue().intValue());

    HashSet<String> fields = null;

    if (!readallfields) {
      // read a random field
      String fieldname = fieldnames.get(fieldchooser.nextValue().intValue());

      fields = new HashSet<>();
      fields.add(fieldname);
    }

    db.query2(table, query2FilterField1, filtervalue1, query2FilterField2, filtervalue2, fields, new Vector<>());
  }

  /**
   * Cleanup the scenario. Called once, in the main client thread, after all operations have completed.
   */
  @Override
  public void cleanup() throws WorkloadException {
    super.cleanup();
  }

  /**
   * Builds values for all fields.
   */
  @Override
  protected HashMap<String, ByteIterator> buildValues(String key) {
    HashMap<String, ByteIterator> values = new HashMap<>();

    for (String fieldkey : fieldnames) {
      ByteIterator data;

      data = model.get(fieldkey).getByteIterator(key, fieldkey, fieldlengthgenerator);
      values.put(fieldkey, data);
    }
    return values;
  }

  /**
   * Builds a value for a randomly chosen field.
   */
  @Override
  protected HashMap<String, ByteIterator> buildSingleValue(String key) {
    HashMap<String, ByteIterator> value = new HashMap<>();

    String fieldkey = fieldnames.get(fieldchooser.nextValue().intValue());
    ByteIterator data;
    if (dataintegrity) {
      data = model.get(fieldkey).getByteIterator(key, fieldkey, fieldlengthgenerator);
    } else {
      // fill with random data
      data = new RandomByteIterator(fieldlengthgenerator.nextValue().longValue());
    }
    value.put(fieldkey, data);

    return value;
  }
}
