const schema = {
  index: {
    mapping: {
      total_fields: {
        limit: undefined,
      },
    },
    number_of_shards: undefined,
    shard: {
      check_on_startup: undefined,
    },
    codec: undefined,
    routing_partition_size: undefined,
    number_of_replicas: undefined,
    auto_expand_replicas: undefined,
    refresh_interval: undefined,
    max_result_window: undefined,
    max_inner_result_window: undefined,
    max_rescore_window: undefined,
    max_docvalue_fields_search: undefined,
    max_script_fields: undefined,
    max_ngram_diff: undefined,
    max_shingle_diff: undefined,
    blocks: {
      read_only: undefined,
      read_only_allow_delete: undefined,
      read: undefined,
      write: undefined,
      metadata: undefined,
    },
    max_refresh_listeners: undefined,
    highlight: {
      max_analyzed_offset: undefined,
    },
    max_terms_count: undefined,
    routing: {
      allocation: {
        enable: undefined,
      },
      rebalance: {
        enable: undefined,
      },
    },
    gc_deletes: undefined,
    max_regex_length: undefined,
    default_pipeline: undefined,
  },
};

export function validize(obj: any, subschema = schema as any) {
  for (const [ k, v ] of Object.entries(obj)) {
    if (!subschema.hasOwnProperty(k)) delete obj[k];
    else if (typeof subschema[k] === 'object') {
      if (typeof v === 'object') {
        validize(obj[k], subschema[k]);
      } else {
        delete obj[k];
      }
    }
  }
}
