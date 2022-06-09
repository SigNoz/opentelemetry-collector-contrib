# Attributes Processor

Supported pipeline types: traces, logs.

The attributes processor modifies attributes of a span or log. Please refer to
[config.go](./config.go) for the config spec.

This processor also supports the ability to filter and match spans/logs to determine
if they should be [included or excluded](#includeexclude-filtering) for specified actions.

It takes a list of actions which are performed in order specified in the config.
The supported actions are:
- `insert`: Inserts a new attribute in spans/logs where the key does not already exist.
- `update`: Updates an attribute in spans/logs where the key does exist.
- `upsert`: Performs insert or update. Inserts a new attribute in spans/logs where the
  key does not already exist and updates an attribute in spans/logs where the key
  does exist.
- `delete`: Deletes an attribute from a span/log.
- `hash`: Hashes (SHA1) an existing attribute value.
- `extract`: Extracts values using a regular expression rule from the input key
  to target keys specified in the rule. If a target key already exists, it will
  be overridden. Note: It behaves similar to the Span Processor `to_attributes`
  setting with the existing attribute as the source.

For the actions `insert`, `update` and `upsert`,
 - `key`  is required
 - one of `value`, `from_attribute` or `from_context` is required
 - `action` is required.
```yaml
  # Key specifies the attribute to act upon.
- key: <key>
  action: {insert, update, upsert}
  # Value specifies the value to populate for the key.
  # The type is inferred from the configuration.
  value: <value>

  # Key specifies the attribute to act upon.
- key: <key>
  action: {insert, update, upsert}
  # FromAttribute specifies the attribute from the span/log to use to populate
  # the value. If the attribute doesn't exist, no action is performed.
  from_attribute: <other key>

  # Key specifies the attribute to act upon.
- key: <key>
  action: {insert, update, upsert}
  # FromContext specifies the context value to use to populate the attribute value. 
  # The context values would be searched in receiver's transport protocol additional information
  # like GRPC Metadata or HTTP Headers. 
  # If the key doesn't exist, no action is performed.
  # If the key has multiple values the values will be joined with `;` separator.
  from_context: <other key>
```

For the `delete` action,
 - `key` is required
 - `action: delete` is required.
```yaml
# Key specifies the attribute to act upon.
- key: <key>
  action: delete
```


For the `hash` action,
 - `key` is required
 - `action: hash` is required.
```yaml
# Key specifies the attribute to act upon.
- key: <key>
  action: hash
```


For the `extract` action,
 - `key` is required
 - `pattern` is required.
 ```yaml
 # Key specifies the attribute to extract values from.
 # The value of `key` is NOT altered.
- key: <key>
  # Rule specifies the regex pattern used to extract attributes from the value
  # of `key`.
  # The submatchers must be named.
  # If attributes already exist, they will be overwritten.
  pattern: <regular pattern with named matchers>
  action: extract

 ```

The list of actions can be composed to create rich scenarios, such as
back filling attribute, copying values to a new key, redacting sensitive information.
The following is a sample configuration.

```yaml
processors:
  attributes/example:
    actions:
      - key: db.table
        action: delete
      - key: redacted_span
        value: true
        action: upsert
      - key: copy_key
        from_attribute: key_original
        action: update
      - key: account_id
        value: 2245
      - key: account_password
        action: delete
      - key: account_email
        action: hash

```

Refer to [config.yaml](./testdata/config.yaml) for detailed
examples on using the processor.

## Include/Exclude Filtering

The [attribute processor](README.md) exposes
an option to provide a set of properties of a span or log record to match against to determine
if the span/log should be included or excluded from the processor. To configure
this option, under `include` and/or `exclude` at least `match_type` and one of the following
is required:
- For spans, one of `services`, `span_names`, `attributes`, `resources`, or `libraries` must be specified with a non-empty value for a valid configuration. The `log_names` field is invalid. 
- For logs, one of `log_names`, `attributes`, `resources`, or `libraries` must be specified with a
non-empty value for a valid configuration. The `span_names` and `services` fields are invalid.

Note: If both `include` and `exclude` are specified, the `include` properties
are checked before the `exclude` properties.

```yaml
attributes:
    # include and/or exclude can be specified. However, the include properties
    # are always checked before the exclude properties.
    {include, exclude}:
      # At least one of services, span_names or attributes must be specified.
      # It is supported to have more than one specified, but all of the specified
      # conditions must evaluate to true for a match to occur.

      # match_type controls how items in "services" and "span_names" arrays are
      # interpreted. Possible values are "regexp" or "strict".
      # This is a required field.
      match_type: {strict, regexp}

      # regexp is an optional configuration section for match_type regexp.
      regexp:
        # < see "Match Configuration" below >

      # services specify an array of items to match the service name against.
      # A match occurs if the span service name matches at least one of the items.
      # This is an optional field.
      services: [<item1>, ..., <itemN>]

      # resources specify an array of items to match the resources against.
      # A match occurs if the span/log resources matches at least one of the items.
      resources: [<item1>, ..., <itemN>]

      # libraries specify an array of items to match the implementation library against.
      # A match occurs if the span/log implementation library matches at least one of the items.
      libraries: [<item1>, ..., <itemN>]

      # The span name must match at least one of the items.
      # This is an optional field.
      span_names: [<item1>, ..., <itemN>]

      # The log name must match at least one of the items.
      # This is an optional field.
      log_names: [<item1>, ..., <itemN>]

      # Attributes specifies the list of attributes to match against.
      # All of these attributes must match exactly for a match to occur.
      # This is an optional field.
      attributes:
          # Key specifies the attribute to match against.
        - key: <key>
          # Value specifies the exact value to match against.
          # If not specified, a match occurs if the key is present in the attributes.
          value: {value}
```

### Match Configuration

Some `match_type` values have additional configuration options that can be
specified. The `match_type` value is the name of the configuration section.
These sections are optional.

```yaml
# regexp is an optional configuration section for match_type regexp.
regexp:
  # cacheenabled determines whether match results are LRU cached to make subsequent matches faster.
  # Cache size is unlimited unless cachemaxnumentries is also specified.
  cacheenabled: <bool>
  # cachemaxnumentries is the max number of entries of the LRU cache; ignored if cacheenabled is false.
  cachemaxnumentries: <int>
```
