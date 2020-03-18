Elasticsearch Updater
---------------------

Elasticsearch Updater takes care of automatically updating index mappings, running data transformation scripts, and re-targeting aliases to new indices when a schema change needs to be made. It assumes that all mutable logical indices are actually represented by aliases pointing to some concrete backing index, and that there is a one-to-one relationship between aliases and active backing indices. Updates are performed on an alias by re-indexing its current backing index, leaving the originaindex behind as a back-up, and then re-targeting the alias at the new active index.

Elasticsearch Cloner
---------------------

Elasticsearch Cloner takes care of preparing a target index with the appropriate mappings and index settings, re-indexing to copy data from a remote instance alias, and setting up the alias for the cloned index. It requires that your target instance already have the source whitelisted.

**Usage**

* `esmigrate init [endpoint]` -- Generate a migration file that will produce a database from scratch implementing the same schema as the target instance running at `endpoint`, and set the schema version on `endpoint` if it has not been set before. Note that this is a global command, which acts on all aliases in the target remote instance.

* `esmigrate clone [source] [target] [alias] [size? = Infinity]` -- Copy documents from the given alias in the source instance to a backing index with the same name as the source backing index in the target instance, and set up alias to point to that index in the target instance. If provided, the optional `size` argument limits the maximum number of documents to be cloned. 

* `esmigrate validate [path? = './migrations']` -- Check that migration files in the specified folder are all valid.

* `esmigrate project [endpoint] [project]` -- Migrate the Elasticsearch instance at `endpoint` to the latest schema version, using a project-specific schema-version index.

* `esmigrate project [endpoint] [project] YYYY-MM-DD(.V)` -- Migrate the Elasticsearch instance at `endpoint` to the schema as of the given date (with an optional sub-version if the schema was updated multiple time in a single day), using a project-specific schema-version index.

* `esmigrate [endpoint]` -- Migrate the Elasticsearch instance at `endpoint` to the latest schema version, using a global schema-version index (with document `_id` '1').

* `esmigrate [endpoint] YYYY-MM-DD(.V)` -- Migrate the Elasticsearch instance at `endpoint` to the schema as of the given date (with an optional sub-version if the schema was updated multiple time in a single day), using a global schema-version index (with document `_id` '1').

Note that the tool does not do any checking to ensure that project-specific migrations only touch project-specific indices/aliases. You must ensure that different projects using the same instance avoid stepping on each other's toes manually. To that end, it is recommended that you not mix project-specific and global migrations in the same instance.

**Config format**

Migration files are specified in a `/migrations` directory, with the naming scheme `YYYY-MM-DD(.V).{human-readable-name}.esmigration`.

At the top level, migration files are divided into "ups" and "downs"--commands for advancing or rewinding the schema version. Each section is introduced with as `#UPS:` or `#DOWNS:` line, respectively.
Within each section, the updates to apply to each alias are introduced with an `#alias: {alias-name}` line. Within each `#alias` section, one can specify any or all of:

* a new mapping
* a migration script
* new settings
* a new name

`#alias` sections with mappings included are treated as upserts--if the alias does not currently exist, a new index and associated alias will be created; if it does, the existing alias will have its backing index updated.

Mapping updates are specified beginning with a `#mappings:` tag. The content of the `#mappings` section is JSON corresponding to the contents of the `mappings` field in the Elasticsearch API request to create an index. For updates to existing logical indexes, though, it is not necessary to re-specify the complete mapping in every migration; instead, you can just include the fields that are to be added or changed. Including a field with a value of `null` will be interpreted as a command to delete that field from the pre-existing mappings. Mappings are required when creating a new index from scratch rather than updating an existing index.

Data-transform scripts are specified with a `#script({lang})` tag. If no language is specified, it defaults to `painless`. The content of a `#script` section is just the literal source code for the tranform script.

Settings updates are specified beginning with a `#settings:` tag. The content of the `#settings` section is JSON corresponding to the contents of the `settings` field in the Elasticsearch API request to create an index. For updates to existing logical indexes, though, it is not necessary to re-specify the complete settings in every migration; instead, you can just include the fields that are to be added or changed. Including a field with a value of `null` will be interpreted as a command to delete that field from the pre-existing settings. If no settings are specified when creating a new index, Elasticsearch defaults will be used.

New names for an alias are specified beginning with a `#rename: {alias-name}` tag. This section has no further content.

`#delete: {alias-name}` lines indicate that the named alias should be removed during the given migration. These can occur anywhere in an `#UPS` or `#DOWNS` section (though they will be most useful in the `DOWNS` section), and override any `#script` or `#mapping` commands that might also be associated with that alias.
