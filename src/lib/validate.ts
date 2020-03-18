import * as fs from 'fs';
import { promisify } from 'util';

async function validateMigrationFile(path: string, name: string) {
  let alias = '?';
  let upOrDown = '?';
  let hasUps = false;
  let hasDowns = false;
  const errors = [];

  const chunks = (await promisify(fs.readFile)(`${ path }/${ name }`, 'utf8')).split('#');

  function makeErrorMessage(section: string, msg: string) {
    return `Failed to parse '${ name }' in '${ section }' section of ${ upOrDown } migrations for alias '${ alias }':\n${ msg }`;
  }

  function tryParseSection(section: string, regex: RegExp, parse: (a: RegExpMatchArray) => string[], chunk: string): [boolean, string[]] {
    const match = chunk.match(regex);

    if (!match) return [ false, [] ];
    try {
      return [
        true,
        parse(match).map(makeErrorMessage.bind(null, section)),
      ];
    } catch (e) {
      return [
        true,
        [ makeErrorMessage(section, e.message) ],
      ];
    }
  }

  const sections: [string, RegExp, (a: RegExpMatchArray) => string[]][] = [
    [
      'UPS',
      /UPS:.*/,
      () => {
        upOrDown = 'UPS';
        alias = '?';
        hasUps = true;

        return [];
      },
    ],
    [
      'DOWNS',
      /DOWNS:.*/m,
      () => {
        upOrDown = 'DOWNS';
        alias = '?';
        hasDowns = true;

        return [];
      },
    ],
    [
      'alias',
      /alias:\s*(\S+)\s*/,
      (match: RegExpMatchArray) => {
        alias = match[1];
        if (upOrDown === '?') throw new Error('Missing UPS or DOWNS specification.');

        return [];
      },
    ],
    [
      'delete',
      /delete:\s*(\S+)\s*/,
      () => {
        if (upOrDown === '?') throw new Error('Missing UPS or DOWNS specification.');

        return [];
      },
    ],
    [
      'rename',
      /rename:\s*(\S+)\s*/,
      () => {
        if (alias === '?') throw new Error('Cannot specify new alias name without an alias.');

        return [];
      },
    ],
    [
      'mappings',
      /mappings:\s*([^]*)/m,
      (match: RegExpMatchArray) => {
        const errs = [];

        if (alias === '?') errs.push('Cannot specify mappings without an alias.');
        try {
          JSON.parse(match[1]);
        } catch ({ message }) {
          errs.push(message.replace(/ at position.*/, ''));
        }

        return errs;
      },
    ],
    [
      'settings',
      /settings:\s*([^]*)/m,
      (match: RegExpMatchArray) => {
        const errs = [];

        if (alias === '?') errs.push('Cannot specify settings without an alias.');
        try {
          JSON.parse(match[1]);
        } catch ({ message }) {
          errs.push(message.replace(/ at position.*/, ''));
        }

        return errs;
      },
    ],
    [
      'script',
      /script(?:\(([a-z]*)\))?:\s*([^]*)/m,
      // eslint-disable-next-line no-confusing-arrow
      () => alias === '?' ? [ 'Cannot specify script without an alias.' ] : [],
    ],
  ];

  /* eslint-disable no-labels */
  chunk: for (const chunk of chunks) {
    if (/^\s*$/.test(chunk)) continue;
    for (const [section, regex, parse] of sections) {
      const [ matched, errs ] = tryParseSection(section, regex, parse, chunk);

      if (matched) {
        errors.push(...errs);
        continue chunk;
      }
    }

    errors.push(makeErrorMessage('NONE', `Unrecognized section: ${ chunk.split('\n')[0].substr(0, 10) }...`));
  }

  if (!hasUps) errors.push(makeErrorMessage('NONE', 'Missing UPS Section'));
  if (!hasDowns) errors.push(makeErrorMessage('NONE', 'Missing DOWNS Section'));

  return errors;
}

const mfilePattern = /(\d{4,})-(0?[123456789]|10|11|12)-([012]?\d|3[01])(?:\.(\d+))?(?:\.[-\w]+)?\.esmigration/;

export async function validate(path = 'migrations') {
  const migrations = (await promisify(fs.readdir)(path))
    .filter((name) => mfilePattern.test(name));

  const errors = [];

  for (const ferrs of await Promise.all(migrations.map((name) => validateMigrationFile(path, name)))) {
    for (const err of ferrs) {
      errors.push(err);
    }
  }

  return errors;
};
