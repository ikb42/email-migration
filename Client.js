const { ImapFlow } = require('imapflow');
const logger = require('./logger');


class Client {
  label = ''
  provider = null
  auth = null
  attempts = 0

  logger = null
  connected = false
  imap = null

  constructor(provider, auth, name) {
    if (!provider || !provider.host || !provider.port) {
      throw new Error('Provider should be an object with { host: string, port: number }');
    }
    
    if (!auth || !auth.email || !auth.password) {
      throw new Error('Auth should be an object with { email: string, password: string }');
    }
    
    this.label = `${name} - ${auth.email}`;
    this.provider = provider
    this.auth = auth

    this.logger = logger.child({ module: 'Client', label: this.label });

    this.createClient()
  }

  createClient() {
    const imapLogger = this.logger.child({ level: 'warn', module: 'ImapFlow', label: this.label });
    
    this.imap = new ImapFlow({
      host: this.provider.host,
      port: this.provider.port,
      secure: false,
      // tls: {
        //   minVersion: 'TLSv1.2'
        // },
        auth: {
          user: this.auth.email,
          pass: this.auth.password
        },
        logger: imapLogger,
    });
  }
    
  async connectClient(reconnect = false) {
    this.imap.on('open', async (...args) => {
      this.connected = true
      this.logger.info(`OPEN`);
    });
    
    this.imap.on('close', async (...args) => {
      this.connected = false
      this.logger.debug(`CLOSED`);
    });
    
    this.imap.on('error', (err) => {
      this.logger.error(err, `ERROR`);
    });
    
    this.attempts += 1
    this.logger.debug(`Waiting for client to ${reconnect ? 'reconnect' : 'connect'}.... ${this.attempts > 1 ? this.attempts : ''}`);
    await this.imap.connect();
  }

  async reCreateAndConnectClient() {
    this.createClient()
    return this.connectClient(true)
  }

  static findMailboxOrCreateIt(mailboxList, mailboxToFind, imapClient) {
    const mailbox = mailboxList.find(
      mailbox =>
        mailbox.name === mailboxToFind.name
        && mailbox.parent.join('.') === mailboxToFind.parent.join('.')
    )

    if (mailbox) return Promise.resolve(mailbox)

    return imapClient.mailboxCreate([...mailboxToFind.parent, mailboxToFind.name])
  }
}

module.exports = {
  Client,
  findMailboxOrCreateIt: Client.findMailboxOrCreateIt
}
