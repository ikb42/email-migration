const { ImapFlow } = require('imapflow');
const logger = require('./logger');

function createClient(provider, auth) {
  if (!provider || !provider.host || !provider.port) {
    throw new Error('Provider should be an object with { host: string, port: number }');
  }

  if (!auth || !auth.email || !auth.password) {
    throw new Error('Auth should be an object with { email: string, password: string }');
  }

  const imapLogger = logger.child({ level: 'warn', module: 'ImapFlow' });

  return new ImapFlow({
    host: provider.host,
    port: provider.port,
    secure: false,
    // tls: {
    //   minVersion: 'TLSv1.2'
    // },
    auth: {
      user: auth.email,
      pass: auth.password
    },
    logger: imapLogger,
  });
}

async function connectClient(provider, account, name) {
  const label = `${name} - ${account.email}`;

  const client = createClient(provider, account);

  logger.info(`${label}: Waiting for client to connect...`);

  client.on('connected', async (...args) => {
    logger.info(`${label}: Client connected`);
  });

  client.on('close', async (...args) => {
    logger.warn({ ...args }, `${label}: Client closed - trying reconnect...`);
    await client.connect();
    logger.warn({ ...args }, `${label}: Client closed - reconnected`);
  });

  client.on('error', (err) => {
    logger.error(err, `${label}: Error on client`);
  });

  await client.connect();

  return client;
}

function findMailboxOrCreateIt(mailboxList, mailboxToFind, client) {
  const mailbox = mailboxList.find(
    mailbox =>
      mailbox.name === mailboxToFind.name
      && mailbox.parent.join('.') === mailboxToFind.parent.join('.')
  )

  if (mailbox) {
    return Promise.resolve(mailbox)
  }

  return client.mailboxCreate([...mailboxToFind.parent, mailboxToFind.name])
}

module.exports = {
  connectClient,
  findMailboxOrCreateIt
}
