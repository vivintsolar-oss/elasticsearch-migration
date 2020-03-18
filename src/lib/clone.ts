import * as fs from 'fs';
import { send, sleep, progress, retargetAlias } from './lib';
import { validize } from './settings';

async function getRemoteConfig(remote: string, alias: string): Promise<[ string, { mappings: {}, settings: {} }]> {
  const { json } = await send(remote, 'GET', alias);
  const keys = Object.keys(json);

  if (keys.includes('error')) throw new Error('Could not retrieve alias info from remote.');

  const index = keys[0];
  const { mappings, settings } = json[index];

  validize(settings);

  return [ index, { mappings, settings } ];
}

async function getLocalIndex(local: string, alias: string) {
  const { json } = await send(local, 'GET', alias);
  const keys = Object.keys(json);

  return keys.includes('error') ? null : keys[0];
}

async function createIndex(local: string, index: string, config: {}) {
  await send(local, 'DELETE', index); // clear old data if it exists
  const { json } = await send(local, 'PUT', index, config);

  if (json.hasOwnProperty('error')) {
    throw new Error(`Error creating index ${ index }: ${ JSON.stringify(json.error) }`);
  }
}

async function reindex(local: string, remote: string, index: string, size?: number) {
  const { json } = await send(local, 'POST', '_reindex?wait_for_completion=false', {
    size,
    source: {
      index,
      remote: {
        host: remote,
      },
    },
    dest: { index },
  });

  if (json.hasOwnProperty('error')) {
    throw new Error(`Error copying ${ index } from ${ remote }: ${ json.error }`);
  }

  const { task } = json;
  const start = +new Date();
  let interval = 5000;

  let printed = false;

  for (;;) {
    await sleep(interval);
    const { json: task_data } = await send(local, 'GET', `_tasks/${ task }`);

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

async function copyIndex(local: string, remote: string, alias: string, doclimit?: number) {
  console.log(`Cloning ${ alias }`);

  const [ [ rindex, config ], lindex ] = await Promise.all([
    getRemoteConfig(remote, alias),
    getLocalIndex(local, alias),
  ]) as [ [ string, {} ], string | null ];

  await createIndex(local, rindex, config);
  await reindex(local, remote, rindex, doclimit);
  await retargetAlias(local, alias, lindex, rindex);
}

export async function clone(remote: string, local: string, alias: string, doclimit?: number) {
  if (fs.existsSync(alias)) {
    for (const line of fs.readFileSync(alias, 'utf8').split('\n')) {
      const [ name, limit ] = line.split(/\s+/);

      if (name) await copyIndex(local, remote, name, parseInt(limit, 10)||undefined);
    }

    try {
      const { json: { _source: { datems, ord } } } = await send(remote, 'GET', 'schema_version/_doc/1');

      if (typeof datems === 'number') {
        await send(local, 'POST', 'schema_version/_doc/1/_update', {
          doc: { datems, ord },
          doc_as_upsert: true,
        });
      }
    } finally { }
  } else {
    copyIndex(local, remote, alias, doclimit);
  }
}