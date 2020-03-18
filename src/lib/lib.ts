import { spawn } from 'child_process';

function exec(command: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const stdout: string[] = [];
    const stderr: string[] = [];
    const child = spawn(command, [], { shell: true });

    child.stdout.on('data', (data) => {
      stdout.push(data);
    });

    child.stderr.on('data', (data) => {
      stderr.push(data);
    });

    child.on('close', (code) => {
      if (code === 0) {
        resolve(stdout.join(''));
      } else {
        reject(new Error(stderr.join('')));
      }
    });
  });
}

export async function send(endpoint: string, method: string, esPath: string, doc: unknown = undefined) {
  const command = doc ?
    `curl -X${ method } '${ endpoint }/${ esPath }' -H 'Content-Type: application/json' -d'${ JSON.stringify(doc) }'` :
    `curl -X${ method } '${ endpoint }/${ esPath }'`;

  const data = await exec(command);

  try {
    return { data, json: JSON.parse(data) };
  } catch (_) {
    return { data, json: {} };
  }
}

export function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function progress(msg: string) {
  if (typeof process.stdout.clearLine === 'function') {
    process.stdout.clearLine(0);
    process.stdout.cursorTo(0);
    process.stdout.write(msg);
  } else {
    console.log(msg);
  }
}

export async function retargetAlias(local: string, alias: string, oindex: string | null, nindex: string) {
  const { json } = await send(local, 'POST', '_aliases', {
    actions: oindex === null ?
      [ { add: { index: nindex, alias } } ] :
      [ { remove: { index: oindex, alias } }, { add: { index: nindex, alias } } ],
  });

  if (json.hasOwnProperty('error')) {
    throw new Error(`Error moving alias from ${ oindex } to ${ nindex }: ${ JSON.stringify(json.error) }`);
  }
}