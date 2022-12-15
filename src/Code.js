//  Copyright notice
//
//  (c) 2019 Gabriël Ramaker <gabriel@lingewoud.nl>, Lingewoud
//
//  All rights reserved
//
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
//  This copyright notice MUST APPEAR in all copies of the script!

/* eslint-disable prefer-rest-params */
/* eslint-disable prefer-spread */


/* We instantiate a TOP-LEVEL DataStudioApp Connector; To be used throughout the script*/
const cc = DataStudioApp.createCommunityConnector();

/**
 * Throws and logs script exceptions.
 *
 * @param {String} message The exception message
 */
function sendUserError(message) {
  cc.newUserError()
    .setText(message)
    .throwException();
}


/**
 * function  `getAuthType()`
 *
 * @returns {Object} `AuthType` used by the connector.
 */
function getAuthType() {
  const AuthTypes = cc.AuthType;
  return cc.newAuthTypeResponse().setAuthType(AuthTypes.NONE).build();
}


/**
 * function  `isAdminUser()`
 *
 * @returns {Boolean} Currently just returns false. Should return true if the current authenticated user at the time
 *                    of function execution is an admin user of the connector.
 */
function isAdminUser() {
  return true;
}


/**
 * Returns the user configurable options for the connector.
 *
 * Required function for Community Connector.
 *
 * @param   {Object} request  Config request parameters.
 * @returns {Object}          Connector configuration to be displayed to the user.
 */
function getConfig(request) {
  let config = cc.getConfig();

  config
    .newInfo()
    .setId('instructions')
    .setText('Fill out the form to connect to a JSON data source.');

  config
    .newTextInput()
    .setId('url')
    .setName('Enter the URL of a JSON data source')
    .setHelpText('e.g. hhttps://jsonplaceholder.typicode.com/users')
    .setPlaceholder('https://jsonplaceholder.typicode.com/users');

  config
    .newCheckbox()
    .setId('cache')
    .setName('Cache response')
    .setHelpText('Usefull with big datasets. Response is cached for 5 minutes')
    .setAllowOverride(true);

  config
    .newTextInput()
    .setId("cache_expiry_time")
    .setName("How long should response be cached (in minutes)")
    .setHelpText("eg. '3' to cache data for 3 minutes. Also 'Cached response' has to be enabled for caching to start.")
    .setPlaceholder("5");

  config.setDateRangeRequired(false);

  return config.build();
}


/**
 * Gets UrlFetch response and parses JSON
 *
 * @param   {string} url  The URL to get the data from
 * @returns {Object}      The response object
 */
function fetchJSON(url) {
  try {
    var response = UrlFetchApp.fetch(url);
  } catch (e) {
    sendUserError('"' + url + '" returned an error:' + e);
  }

  try {
    var content = JSON.parse(response);
  } catch (e) {
    sendUserError('Invalid JSON format. ' + e);
  }

  return content;
}


/**
 * Gets cached response. If the response has not been cached, make
 * the fetchJSON call, then cache and return the response.
 *
 * @param   {string} url  The URL to get the data from
 * @returns {Object}      The response object
 */
function getCachedData(url, cache_expiry_time) {
  const expiryTime = parseInt(cache_expiry_time);
  const cacheExpiryTime = isNaN(expiryTime) ? 300 : isFinite(expiryTime) ? expiryTime * 60 : 300;
  const cache = CacheService.getUserCache();
  const cacheKey = url.replace(/[^a-zA-Z0-9]+/g, '');
  const cacheKeyString = cache.get(cacheKey + '.keys');
  const cacheKeys = cacheKeyString !== null ? cacheKeyString.split(',') : [];
  let cacheData = {};
  let content = [];

  if (cacheKeyString !== null && cacheKeys.length > 0) {
    cacheData = cache.getAll(cacheKeys);

    for (let key in cacheKeys) {
      if (cacheData[cacheKeys[key]] != undefined) {
        content.push(JSON.parse(cacheData[cacheKeys[key]]));
      }
    }
  } else {
    content = fetchJSON(url);

    for (let key in content) {
      cacheData[cacheKey + '.' + key] = JSON.stringify(content[key]);
    }

    cache.putAll(cacheData);
    cache.put(cacheKey + '.keys', Object.keys(cacheData), cacheExpiryTime);
  }
  return content;
}


/**
 * Do not enable cache if API response will be bigger than 100kb limit.
 * 
 * Fetches data. Either by calling getCachedData or fetchJSON, depending on the cache configuration parameter.
 * 
 * @param   {Object}  configParams  The URL to get the data from
 * @returns {Object}                The response object
 */
function fetchData(configParams) {
  const url = configParams.url;
  const cache = configParams.cache;

  if (!url || !url.match(/^https?:\/\/.+$/g)) {
    sendUserError('"' + url + '" is not a valid url.');
  }
  try {
    var content = cache ? getCachedData(url, configParams.cache_expiry_time) : fetchJSON(url);
  } catch (e) {
    sendUserError(
      'Your request could not be cached. The rows of your dataset probably exceed the 100KB cache limit.' + e
    );
  }
  if (!content) sendUserError('"' + url + '" returned no content.');
  return content;
}


/**
 * Matches the field value to a semantic
 *
 * @param   {Mixed}   value   The field value
 * @param   {Object}  types   The list of types
 * @return  {string}          The semantic type
 */
function getSemanticType(value, types) {
  if (!isNaN(parseFloat(value)) && isFinite(value)) {
    return types.NUMBER;
  } else if (value === true || value === false) {
    return types.BOOLEAN;
  } else if (typeof value != 'object' && value != null) {
    if (
      value.match(
        new RegExp(
          /[-a-zA-Z0-9@:%_\+.~#?&//=]{2,256}\.[a-z]{2,4}\b(\/[-a-zA-Z0-9@:%_\+.~#?&//=]*)?/gi
        )
      )
    ) {
      return types.URL;
    } else if (!isNaN(Date.parse(value))) {
      return types.YEAR_MONTH_DAY_HOUR;
    }
  }
  return types.TEXT;
}


/**
 *  Creates the fields
 *
 * @param   {Object}  fields  The list of fields
 * @param   {Object}  types   The list of types
 * @param   {String}  key     The key value of the current element
 * @param   {Mixed}   value   The value of the current element
 */
function createField(fields, types, key, value) {
  let semanticType = getSemanticType(value, types);
  let field =
    semanticType == types.NUMBER ? fields.newMetric() : fields.newDimension();

  field.setType(semanticType);
  field.setId(key.replace(/\s/g, '_').toLowerCase());
  field.setName(key);
}


/**
 * Handles keys for recursive fields
 *
 * @param   {String}  currentKey  The key value of the current element
 * @param   {Mixed}   key         The key value of the parent element
 * @returns {String}  if true
 */
function getElementKey(key, currentKey) {
  if (currentKey == '' || currentKey == null) {
    return;
  }
  if (key != null) {
    return key + '.' + currentKey.replace('.', '_');
  }
  return currentKey.replace('.', '_');
}


/**
 * Extracts the objects recursive fields and adds it to fields
 *
 * @param   {Object}  fields  The list of fields
 * @param   {Object}  types   The list of types
 * @param   {String}  key     The key value of the current element
 * @param   {Mixed}   value   The value of the current element
 * @param   {boolean} isInline if true
 */
function createFields(fields, types, key, value, isInline) {
  if (typeof value === 'object' && !Array.isArray(value) && value !== null) {
    Object.keys(value).forEach(function (currentKey) {
      let elementKey = getElementKey(key, currentKey);

      if (isInline && value[currentKey] != null) {
        createFields(fields, types, elementKey, value[currentKey], isInline);
      } else {
        createField(fields, types, currentKey, value);
      }
    });
  } else if (key !== null) {
    createField(fields, types, key, value);
  }
}


/**
 * Parses first line of content to determine the data schema
 *
 * @param   {Object}  request getSchema/getData request parameter.
 * @param   {Object}  content The content object
 * @return  {Object}           An object with the connector configuration
 */
function getFields(request, content) {
  const fields = cc.getFields();
  const types = cc.FieldType;
  const isInline = true;

  if (!Array.isArray(content)) content = [content];

  if (typeof content[0] !== 'object' || content[0] === null) {
    sendUserError('Invalid JSON format');
  }
  try {
    createFields(fields, types, null, content[0], isInline);
  } catch (e) {
    sendUserError('Unable to identify the data format of one of your fields.');
  }
  return fields;
}


/**
 * Returns the schema for the given request.
 *
 * @param   {Object} request Schema request parameters.
 * @returns {Object} Schema for the given request.
 */
function getSchema(request) {
  const content = fetchData(request.configParams);
  const fields = getFields(request, content).build();
  return { schema: fields };
}


/**
 *  Converts date strings to YYYYMMDDHH:mm:ss
 *
 * @param   {String} val  Date string
 * @returns {String}      Converted date string
 */
function convertDate(val) {
  let date = new Date(val);
  return (
    date.getUTCFullYear() +
    ('0' + (date.getUTCMonth() + 1)).slice(-2) +
    ('0' + date.getUTCDate()).slice(-2) +
    ('0' + date.getUTCHours()).slice(-2)
  );
}


/**
 * Validates the row values. Only numbers, boolean, date and strings are allowed
 *
 * @param   {Field} field The field declaration
 * @param   {Mixed} val   The value to validate
 * @returns {Mixed}       Either a string or number
 */
function validateValue(field, val) {
  if (field.getType() == 'YEAR_MONTH_DAY_HOUR') {
    val = convertDate(val);
  }

  switch (typeof val) {
    case 'string':
    case 'number':
    case 'boolean':
      return val;
    case 'object':
      return JSON.stringify(val);
  }
  return '';
}


/**
 * Returns the (nested) values for requested columns
 *
 * @param   {Object} valuePaths       Field name. If nested; field name and parent field name
 * @param   {Object} row              Current content row
 * @returns {Mixed}                   The field values for the columns
 */
function getColumnValue(valuePaths, row) {
  for (let index in valuePaths) {
    const currentPath = valuePaths[index];

    if (row[currentPath] === null) {
      return '';
    }

    if (row[currentPath] !== undefined) {
      row = row[currentPath];
      continue;
    }
    let keys = Object.keys(row);

    for (let index_keys in keys) {
      let key = keys[index_keys].replace(/\s/g, '_').toLowerCase();
      if (key == currentPath) {
        row = row[keys[index_keys]];
        break;
      }
    }
  }
  return row;
}


/**
 * Returns an object containing only the requested columns
 *
 * @param   {Object} content          The content object
 * @param   {Object} requestedFields  Fields requested in the getData request.
 * @returns {Object}                  An object only containing the requested columns.
 */
function getColumns(content, requestedFields) {
  if (!Array.isArray(content)) content = [content];

  return content.map(function (row) {
    const rowValues = [];

    requestedFields.asArray().forEach(function (field) {
      const valuePaths = field.getId().split('.');
      const fieldValue = row === null ? '' : getColumnValue(valuePaths, row);

      rowValues.push(validateValue(field, fieldValue));
    });
    return { values: rowValues };
  });
}


/**
 * Returns the tabular data for the given request.
 *
 * @param   {Object} request  Data request parameters.
 * @returns {Object}          Contains the schema and data for the given request.
 */
function getData(request) {
  const content = fetchData(request.configParams);
  const fields = getFields(request, content);
  const requestedFieldIds = request.fields.map(function (field) {
    return field.name;
  });
  const requestedFields = fields.forIds(requestedFieldIds);

  return {
    schema: requestedFields.build(),
    rows: getColumns(content, requestedFields)
  };
}