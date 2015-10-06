/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.converter.filter;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.AvroToAvroConverterBase;
import gobblin.converter.DataConversionException;
import gobblin.converter.EmptyIterable;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


/**
 * Basic implementation of a filter converter for Avro data. Based on a specified values within the avro
 * record, it returns either the record itself or an empty record.
 * Specific filters need to subclass this class.
 *
 * @author mwol
 */
public abstract class AbstractCustomAvroFilterConverter extends AvroToAvroConverterBase {

  protected abstract boolean accept(final GenericRecord inputRecord);

  @Override
  public Schema convertSchema(Schema inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    return inputSchema;
  }

  @Override
  public Iterable<GenericRecord> convertRecord(
      Schema outputSchema,
      GenericRecord inputRecord,
      WorkUnitState workUnit) throws DataConversionException {

    if (accept(inputRecord)) {
      return new SingleRecordIterable<GenericRecord>(inputRecord);
    }

    return new EmptyIterable<GenericRecord>();
  }
}
