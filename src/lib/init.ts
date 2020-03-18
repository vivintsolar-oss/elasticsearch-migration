import * as fs from 'fs';
import { promisify } from 'util';
import { send } from './lib';

export async function generateInitialMigration(endpoint: string) {
  const aliases = new Set();

  const table = (await send(endpoint, 'GET', '_cat/aliases?s=a&v')).data
    .split(/(?:\n|\r)+/);

  table.shift(); // get rid of column headers

  const mappingPromises = table
    .map((line) => line.split(/\s+/))
    .filter(([ alias ]) => alias && !aliases.has(alias) && (aliases.add(alias), true))
    .map(async([ alias, index ]) => [ alias, (await send(endpoint, 'GET', index)).json[index].mappings ]);

  const aliasMappings = await Promise.all(mappingPromises);

  const ups = aliasMappings.map(([ alias, mappings ]) => `#alias: ${ alias }\n#mappings: ${ JSON.stringify(mappings, null, 2) }`).join('\n\n');

  const downs = aliasMappings.map(([ alias ]) => `#delete: ${ alias }`).join('\n');

  const now = new Date();

  const filePromise = promisify(fs.writeFile)(
    `migrations/init.${ now.getFullYear() }-${ now.getMonth() + 1 }-${ now.getDate() }.esmigration`,
    `#UPS:\n\n${ ups }\n\n#DOWNS:\n\n${ downs }`
  );

  // If the target endpoint does not contain a version index, set
  // set the version to match the initial migration that we just created.
  if ((await send(endpoint, 'GET', 'schema_version')).json.error) {
    now.setHours(0);
    now.setMinutes(0);
    now.setSeconds(0);
    now.setMilliseconds(0);

    await send(endpoint, 'POST', 'schema_version/_doc/1/_update', {
      doc: { datems: now.valueOf(), ord: 0 },
      doc_as_upsert: true,
    });
  }

  await filePromise;
}