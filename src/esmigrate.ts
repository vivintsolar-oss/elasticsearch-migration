import {
  clone,
  update,
  validate,
  generateInitialMigration,
} from './index';

async function main() {
  let [ , , cmd, ...args] = process.argv;

  switch (cmd) {
    case 'init': {
      if (args.length < 1) throw new Error("usage: `esmigrate init` [endpoint]");
      const endpoint = args[0];
      await generateInitialMigration(endpoint.replace(/\/$/, ''));
      break;
    }
    case 'clone': {
      if (args.length < 2) throw new Error("usage: esmigrate clone [src] [dst] [alias] [size? = Infinity]");
      const [remote, local, alias, doclimit] = args;
      await clone(remote.replace(/\/$/, ''), local.replace(/\/$/, ''), alias, parseInt(doclimit, 10)||undefined);
      break;
    }
    case 'validate': {
      const errors = await validate(args[0]);
      if (errors.length === 0) {
        console.log('No errors found.');
        break;
      }
      for (const err of errors) {
        console.error(err);
      } 
      process.exit(1);
    }
    case 'project': {
      if (args.length < 2) throw new Error("usage: `esmigrate project [endpoint] [project] [target? = latest]");
      const [endpoint, project, target] = args;
      await update(endpoint.replace(/\/$/, ''), { project, target });
      break;
    }
    default: {
      await update(cmd.replace(/\/$/, ''), { target: args[0] });
      break;
    }
  }
}

main().catch((e) => {
  console.error(e.message);
  console.error(e.stack);
  process.exit(1);
});
