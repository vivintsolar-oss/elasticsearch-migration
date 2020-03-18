import * as fs from 'fs';
import { promisify } from 'util';
import { send, retargetAlias, sleep, progress } from './lib';
import { validize } from './settings';

type AliasRemove = { remove: { index: string, alias: string } };
type AliasOp = AliasRemove | { add: { index: string, alias: string } };

type Version = {
  datems: number;
  ord: number;
};

type MigrationSpec = {
  name: string;
  datems: number;
  ord: number;
}

type Update = {
  alias: string;
  delete: boolean;
  rename?: string|null;
  mappings?: {};
  settings?: {};
  script?: Script;
};

type MigrationUpdates = {
  name: string;
  datems: number;
  ord: number;
  updates: Update[];
};

type Script = {
  lang: string;
  source: string;
};

async function getCurrentVersion(endpoint: string, project: string = '1'): Promise<Version> {
  const { json: { _source: doc } } = await send(endpoint, 'GET', `schema_version/_doc/${ project }`);

  return doc || { datems: 0, ord: 0 };
}

function setCurrentVersion(endpoint: string, version: Version, project: string = '1') {
  return send(endpoint, 'POST', `schema_version/_doc/${ project }/_update`, {
    doc: version,
    doc_as_upsert: true,
  });
}

let existingIndices: Set<string>|undefined;

async function getExistingIndices(endpoint: string) {
  if (!existingIndices) {
    const { data } = await send(endpoint, 'GET', '_cat/indices?s=i&v');
    const idxs = data.split(/(?:\n|\r)+/)
      .map((line) => line.split(/\s+/)[2]);

    existingIndices = new Set(idxs);
  }

  return existingIndices;
}

async function getAliasedIndex(endpoint: string, alias: string) {
  const { data } = await send(endpoint, 'GET', '_cat/aliases?s=a&v');
  const records = data.split(/(?:\n|\r)+/)
    .map((line) => line.split(/\s+/));

  const indices = records.filter(([ a ]) => a === alias);

  if (indices.length === 0) {
    return alias;
  }

  if (indices.length > 1) {
    throw new Error(`Alias ${ alias } does not reference exactly one index.`);
  }

  const index = indices[0][1];

  const aliases = records.filter(([ , i ]) => i === index).map(([ a ]) => a);

  if (aliases.length > 1) {
    throw new Error(`Cannot modify alias ${ alias } with shared backing index ${ index } (shared with ${ aliases.join(', ') })`);
  }

  return index;
}

async function newIndexName(endpoint: string, alias: string) {
  const now = new Date();
  const base = `${ alias }-${ now.toISOString().split('T')[0] }`;

  let index = base;
  let counter = 0;

  const existing = await getExistingIndices(endpoint);

  while (existing.has(index)) {
    index = `${ base }-${ ++counter }`;
  }

  existing.add(index);

  return index;
}

function deepUpdate(obj: any, update: any) {
  if (typeof update !== 'object') return;
  for (const [ k, v ] of Object.entries(update)) {
    if (v === null) {
      delete obj[k];
    } else if (typeof v === 'object' && typeof obj[k] === 'object') {
      deepUpdate(obj[k], v);
    } else {
      obj[k] = v;
    }
  }
}

async function getUpdatedMappingsAndSettings(endpoint: string, index: string, nmap: {}, nset: {}) {
  const {
    json: {
      [index]: data = { mappings: {}, settings: {} },
    },
  } = await send(endpoint, 'GET', index);
  const { mappings, settings } = data;

  deepUpdate(mappings, nmap);
  deepUpdate(settings, nset);
  validize(settings);

  return data;
}

async function delAlias(endpoint: string, index: string, alias: string) {
  const { json } = await send(endpoint, 'DELETE', `${ index }/_alias/${ alias }`);

  if (json.hasOwnProperty('error')) {
    throw new Error(`Error deleting alias ${ alias }: ${ JSON.stringify(json.error) }`);
  }
}

async function createIndex(endpoint: string, index: string, { mappings, settings }: { mappings: {}, settings: {} }) {
  const { json } = await send(endpoint, 'PUT', index, { mappings, settings });

  if (json.hasOwnProperty('error')) {
    throw new Error(`Error creating index ${ index }: ${ JSON.stringify(json.error) }`);
  }
}

async function reindex(endpoint: string, source: string, dest: string, script: Script|undefined) {
  const { json } = await send(endpoint, 'POST', '_reindex?wait_for_completion=false', {
    script,
    source: { index: source },
    dest: {
      index: dest,
      // Ensures that we don't re-copy existing
      // and up-to-date objects during the second
      // round of reindexing.
      version_type: 'external',
    },
  });

  if (json.hasOwnProperty('error')) {
    const { json: delJson } = await send(endpoint, 'DELETE', dest);

    let msg = `Error reindexing ${ source } to ${ dest }: ${ JSON.stringify(json.error) }`;

    if (delJson.hasOwnProperty('error')) {
      msg += `; Error deleting temporary index: ${ JSON.stringify(delJson.error) }`;
    }

    throw new Error(msg);
  }

  const { task } = json;
  const start = +new Date();
  let interval = 5000;
  let printed = false;

  for (;;) {
    await sleep(interval);
    const { json: task_data } = await send(endpoint, 'GET', `_tasks/${ task }`);

    if (task_data.error) throw new Error(JSON.stringify(task_data.error, null, 2));
    if (task_data.completed) break;

    const { running_time_in_nanos, status: { total, created } } = task_data.task;
    const time = (Date.now() - start) / 1000;

    printed = true;

    if (created === 0) {
      progress(`Waiting... (${ time }s)`);
      continue;
    }

    const per_doc = running_time_in_nanos / created;
    const remaining = (total - created) * per_doc;
    const rms = remaining / 1000000;
    const rs = (rms / 1000).toFixed(1);
    const pc = (100 * created / total).toFixed(1);

    interval = Math.min(rms, 30000);
    progress(`${ time }s... Estimated remaining time: ${ rs }s; ${ created } / ${ total } finished [${ pc }%]`);
  }

  if (printed) console.log('');
}

async function replaceAlias(endpoint: string, index: string, oalias: string, nalias: string) {
  const { json } = await send(endpoint, 'POST', '_aliases', {
    actions: [
      { remove: { index, alias: oalias } },
      { add: { index, alias: nalias } },
    ],
  });

  if (json.hasOwnProperty('error')) {
    throw new Error(`Error renaming alias ${ oalias } to ${ nalias }: ${ JSON.stringify(json.error) }`);
  }
}

async function updateIndex(endpoint: string, update: Update, rollbacks: AliasOp[]) {
  let { alias, rename, mappings, settings, script, delete: toDelete } = update;
  const index = await getAliasedIndex(endpoint, alias);

  if (toDelete) {
    delAlias(endpoint, index, alias);
    rollbacks.push({ add: { index, alias } });

    return;
  }

  const create = alias === index;

  if (rename) {
    if (!create) {
      replaceAlias(endpoint, index, alias, rename);
      rollbacks.push(
        { remove: { index, alias: rename } },
        { add: { index, alias } },
      );
    }
    // If we try to create and rename
    // an alias at the same time, just
    // create it with the new name.
    alias = rename;
  }

  if (!mappings) {
    if (!script && !settings) {
      // no mapping or setting updates and no transform script--nothing to do, so we do nothing.
      return;
    }

    if (create) {
      throw new Error(`Cannot create new index ${ alias } without mappings.`);
    }
  }

  const nindex = await newIndexName(endpoint, alias);

  // Copy mappings and settings from old index, possibly with updates
  const updated = await getUpdatedMappingsAndSettings(endpoint, index, mappings||{}, settings||{});
  await createIndex(endpoint, nindex, updated);

  if (create) {
    console.log(`Creating ${ alias }....`);

    // Create a new alias, no re-indexing or retargeting required
    const { json } = await send(endpoint, 'POST', '_aliases', {
      actions: [ { add: { index: nindex, alias } } ],
    });

    if (json.hasOwnProperty('error')) {
      throw new Error(`Error creating new alias ${ alias }: ${ JSON.stringify(json.error) }`);
    }
  } else {
    // We re-index twice to ensure we don't drop data
    // that was still going to the original index
    // during the first re-index and retargeting.
    console.log(`Reindexing ${ alias }....`);
    rollbacks.push({ remove: { index: nindex, alias } }, { add: { index, alias } });
    await reindex(endpoint, index, nindex, script);
    await retargetAlias(endpoint, alias, index, nindex);
    await reindex(endpoint, index, nindex, script);
  }
}

async function partitionMigrationChunks(fname: string, upOrDown: 'up'|'down') {
  let direction = '';

  return (await promisify(fs.readFile)(fname, 'utf8')).split('#')
    .filter((chunk) => {
      if (chunk.startsWith('UPS:')) {
        direction = 'up';

        return false;
      }

      if (chunk.startsWith('DOWNS:')) {
        direction = 'down';

        return false;
      }

      return direction === upOrDown;
    });
}

async function parseMigrationFile({ name, datems, ord }: MigrationSpec, upOrDown: 'up'|'down'): Promise<MigrationUpdates> {
  let alias: string|null = null;

  const mappings = new Map<string, {}>();
  const settings = new Map<string, {}>();
  const scripts = new Map<string, Script>();
  const aliases = new Set<string>();
  const deletions = new Set<string>();
  let rename = null;

  function tryParseSection(section: string, parse: () => {}) {
    try {
      parse();
    } catch (e) {
      throw new Error(`Failed to parse '${ name }' in '${ section }' section of ${ upOrDown } migrations for ${ alias }: ${ e.message }`);
    }
  }

  for (const chunk of await partitionMigrationChunks(`migrations/${ name }`, upOrDown)) {
    let match = chunk.match(/alias:\s*(\S+)\s*/);

    if (match) {
      alias = match[1];
      aliases.add(alias);
      continue;
    }

    match = chunk.match(/delete:\s*(\S+)\s*/);
    if (match) {
      aliases.add(match[1]);
      deletions.add(match[1]);
    }

    if (alias === null) {
      continue;
    }

    match = chunk.match(/rename:\s*(\S+)\s*/);
    if (match) {
      rename = match[1];
    }

    match = chunk.match(/mappings:\s*([^]*)/m);
    if (match) {
      tryParseSection('mappings', () => mappings.set(alias as string, JSON.parse((match as RegExpMatchArray)[1])));
      continue;
    }

    match = chunk.match(/settings:\s*([^]*)/m);
    if (match) {
      tryParseSection('settings', () => settings.set(alias as string, JSON.parse((match as RegExpMatchArray)[1])));
      continue;
    }

    match = chunk.match(/script(?:\(([a-z]*)\))?:\s*([^]*)/m);
    if (match) {
      scripts.set(alias, { lang: match[1] || 'painless', source: match[2] });
    }
  }

  const updates: Update[] = [];

  for (const aliasName of aliases) {
    updates.push({
      rename,
      alias: aliasName,
      delete: deletions.has(aliasName),
      mappings: mappings.get(aliasName),
      settings: settings.get(aliasName),
      script: scripts.get(aliasName),
    });
  }

  return { name, datems, ord, updates };
}

function cmpDateOrd(a: Version, b: Version) {
  const dateCmp = a.datems - b.datems;

  return Math.sign(dateCmp === 0 ? a.ord - b.ord : dateCmp);
}

async function getMigrations(currentVersion: Version, target: Version) {
  const mfilePattern = /(\d{4,})-(0?[123456789]|10|11|12)-([012]?\d|3[01])(?:\.(\d+))?(?:\.[-\w]+)?\.esmigration/;
  const allMigrations = (await promisify(fs.readdir)('migrations'))
    .map((name) => {
      const match = name.match(mfilePattern);

      if (match) {
        const year = parseInt(match[1], 10);
        const month = parseInt(match[2], 10) - 1;
        const day = parseInt(match[3], 10);
        const ord = parseInt(match[4], 10) || 0;

        const datems = new Date(year, month, day).valueOf();

        return { name, datems, ord };
      }

      return null;
    })
    .filter((migration) => migration) as MigrationSpec[];

  for (let i = allMigrations.length - 1; i > 0; i--) {
    const a = allMigrations[i];

    for (let j = i - 1; j >= 0; j--) {
      if (cmpDateOrd(a, allMigrations[j]) === 0) {
        throw new Error(`Duplicate migrations: ${ a.name } & ${ allMigrations[j].name }.`);
      }
    }
  }

  let migrations: MigrationSpec[];
  let select: 'up'|'down';

  switch (cmpDateOrd(target, currentVersion)) {
    case 0: return [];
    case 1:
      migrations = allMigrations
        .filter((migration) => cmpDateOrd(migration, currentVersion) > 0 && cmpDateOrd(migration, target) <= 0)
        .sort(cmpDateOrd);

      select = 'up';
      break;
    default:
      migrations = allMigrations
        .filter((migration) => cmpDateOrd(migration, currentVersion) <= 0 && cmpDateOrd(migration, target) > 0)
        .sort((a, b) => cmpDateOrd(b, a));

      select = 'down';
  }

  return Promise.all(migrations.map(async(mig) => parseMigrationFile(mig, select)));
}

async function applyMigrations(endpoint: string, currentVersion: Version, target: Version, project?: string) {
  const migrations = await getMigrations(currentVersion, target);

  for (const { name, datems, ord, updates } of migrations) {
    console.log(`Applying ${ name }`);
    const rollbacks: AliasOp[] = [];

    try {
      for (const update of updates) {
        await updateIndex(endpoint, update, rollbacks);
      }
    } catch (e) {
      console.log(e);
      if (rollbacks.length > 0) {
        const { json } = await send(endpoint, 'POST', '_aliases', {
          actions: rollbacks.reverse(),
        });

        if (json.hasOwnProperty('error')) {
          throw new Error(`Error rolling back partial updates for ${ name }; manual intervention may be required: ${ JSON.stringify(json.error) }`);
        }
      }

      const removals = rollbacks.filter(op => op.hasOwnProperty('remove')) as AliasRemove[];

      for (const { remove: { index } } of removals) {
        const { json: djson } = await send(endpoint, 'DELETE', index);

        if (djson.hasOwnProperty('error')) {
          console.log(`Error removing new index ${ index } during rollback: ${ JSON.stringify(djson.error) }`);
        }
      }

      throw e;
    }

    await setCurrentVersion(endpoint, { datems, ord }, project);
  }
}

export async function update(endpoint: string, { target: targ, project }: { target?: string, project?: string }) {
  let target;

  if (targ) {
    const match = targ.match(/(\d{4,})-(0?[123456789]|10|11|12)-(0?[1-9]|[12]\d|3[01])(?:\.(\d+))?/);

    if (match) {
      target = {
        datems: new Date(parseInt(match[1], 10), parseInt(match[2], 10), parseInt(match[3], 10)).valueOf(),
        ord: parseInt(match[4], 10) || 0,
      };
    }
  }

  if (!target) {
    target = { datems: Infinity, ord: Infinity };
  }

  const currentVersion = await getCurrentVersion(endpoint, project);

  if (cmpDateOrd(currentVersion, target) !== 0) {
    await applyMigrations(endpoint, currentVersion, target, project);
  }

  console.log('Up to date');
}

