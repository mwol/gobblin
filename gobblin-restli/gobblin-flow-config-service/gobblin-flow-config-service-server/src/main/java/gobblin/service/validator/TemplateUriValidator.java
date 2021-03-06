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

package gobblin.service.validator;

import java.net.URI;
import java.net.URISyntaxException;

import com.google.common.base.Splitter;
import com.linkedin.data.DataMap;
import com.linkedin.data.element.DataElement;
import com.linkedin.data.message.Message;
import com.linkedin.data.schema.validator.AbstractValidator;
import com.linkedin.data.schema.validator.ValidatorContext;

/**
 * Validates the String value to ensure that it is a comma separated list of FS scheme URIs
 */
public class TemplateUriValidator extends AbstractValidator
{
  private static final String FS_SCHEME = "FS";

  public TemplateUriValidator(DataMap config)
  {
    super(config);
  }

  @Override
  public void validate(ValidatorContext ctx)
  {
    DataElement element = ctx.dataElement();
    Object value = element.getValue();
    String str = String.valueOf(value);
    boolean valid = true;

    try {
      Iterable<String> uriStrings = Splitter.on(",").omitEmptyStrings().trimResults().split(str);

      for (String uriString : uriStrings) {
        URI uri = new URI(uriString);

        if (!uri.getScheme().equalsIgnoreCase(FS_SCHEME)) {
          throw new URISyntaxException(uriString, "Scheme is not FS");
        }
      }
    }
    catch (URISyntaxException e) {
      valid = false;
    }

    if (!valid)
    {
      ctx.addResult(new Message(element.path(), "\"%1$s\" is not a well-formed comma-separated list of URIs", str));
    }
  }
}