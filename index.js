const { addCleanupListener } = require('async-cleanup');
const Transferred = require('./data')
const transferData = require('./transferEmails.json')

const logger = require('./logger');
const { connectClient, findMailboxOrCreateIt } = require('./connectClient');
const { wait } = require('./utils');


function downloadMessageAndUpload(msg, mailboxTo, fromClient, toClient) {

  return fromClient
    .download(msg.uid, undefined, { uid: true })
    .then(({ content: contentStream }) => new Promise((resolve, reject) => {
      const buffers = []

      contentStream.on('data', (data) => buffers.push(data))

      contentStream.on('end', async () => {
        try {
          const flags = [...msg.flags].filter(flag => flag !== '\\RECEIPTCHECKED')
          let added = await toClient.append(mailboxTo.path, Buffer.concat(buffers), flags, msg.date)
          resolve(added)
        }
        catch (err) {
          logger.error({ err, message: msg }, 'downloadMessageAndUpload - toClient.append - error')
          logger.warn(`toClient.usable `, toClient.usable)
          logger.warn(`fromClient.usable `, fromClient.usable)
          reject(err)
        }
      })

      contentStream.on('close', () => {
        // logger.warn("close................................. ", msg.date)
      })

      contentStream.on('error', (err) => reject(err))
    }))
}

async function getAndUpload(itr, state, transferred, toMailbox, fromClient, toClient) {
  let nextMsg = itr.next()
  while (nextMsg && nextMsg.value) {
    if (closing) break

    try {
      state.running += 1
      const [idx, message] = nextMsg.value

      const msgAdded = await downloadMessageAndUpload(message, toMailbox, fromClient, toClient)

      state.messagesUploadedCount += 1
      transferred.msgIds.push({ sourceUid: message.uid, destUid: msgAdded.uid })

      if (state.messagesUploadedCount % 10 === 0) {
        transferred.saveDoc()
        if (state.messagesUploadedCount % 100 === 0) {
          logger.info(`${state.fromEmail}: Messages uploaded: ${state.messagesUploadedCount} of ${state.total}  ${state.messagesErrorCount ? '(errors: ' + state.messagesErrorCount + ')' : ''}`)
        }
      }
    }
    catch (er) {
      state.messagesErrorCount += 1
      state.messagesToRetry.push(message)
      logger.error(er, `getAndUpload - error - errorCount: ${state.messagesErrorCount}  (uploaded: ${state.messagesUploadedCount} of ${state.total})`)
    }
    finally {
      state.running -= 1
    }

    nextMsg = itr.next()
  }
}


async function process(acc) {
  const fromAcc = acc.from
  const toAcc = acc.to

  const log = logger.child({ fromEmail: fromAcc.email })

  log.info(`Migrating to: ${toAcc.email}`)

  const mailboxesCountMessageFromEmail = {
    emailFrom: fromAcc.email,
    emailTo: toAcc.email,
    mailboxesMessages: []
  }

  let transferred = await Transferred.findOne({ email: fromAcc.email })
  if (!transferred) transferred = new Transferred({ email: fromAcc.email, mailboxes: {} })
  transferring.push(transferred)

  let fromClient, toClient
  try {
    log.debug(`Creating and connecting clients`)
    fromClient = await connectClient(transferData.providers[fromAcc.provider], fromAcc, 'from')
    toClient = await connectClient(transferData.providers[toAcc.provider], toAcc, 'to')

    const fromQuota = await fromClient.getQuota()
    const toQuota = await toClient.getQuota()

    if (fromQuota && toQuota && fromQuota.storage.used > toQuota.storage.available) {
      throw new Error('Disk quota used in the From email is bigger than the To email')
    }

    // Get all emails mailboxes
    const mailboxesOfFromEmail = await fromClient.list()
    const mailboxesOfToEmail = await toClient.list()

    // Go through each mailbox of the main email 
    log.debug('Going through mailboxes (skips flagged *Junk / *Trash)')

    mailboxLoop: for (const fromMailbox of mailboxesOfFromEmail) {
      if (!transferred.mailboxes[fromMailbox.pathAsListed]) transferred.mailboxes[fromMailbox.pathAsListed] = { messagesUploadedCount: 0 }

      const mailbox = transferred.mailboxes[fromMailbox.pathAsListed]

      if (fromMailbox.flags.has('\\Junk') || fromMailbox.flags.has('\\Trash')) {
        log.debug('Skipping Flagged Mailbox: ', fromMailbox.path)
        mailbox.skipped = true
        continue mailboxLoop
      }
      if (['spam', 'junk', 'trash'].find(t => fromMailbox.path.toLowerCase().indexOf(t) > -1)) {
        log.debug('Skipping ? Mailbox: ', fromMailbox.path)
        mailbox.skipped = true
        continue mailboxLoop
      }

      try {
        const toMailbox = await findMailboxOrCreateIt(mailboxesOfToEmail, fromMailbox, toClient)

        let fromMailboxLock
        let toMailboxLock

        try {
          fromMailboxLock = await fromClient.getMailboxLock(fromMailbox.path)
          toMailboxLock = await toClient.getMailboxLock(toMailbox.path)

          const { messages: numOfMessages } = await fromClient.status(fromMailbox.path, { messages: true })
          mailbox.numOfMessages = numOfMessages

          let messagesUploadedCount = 0

          log.debug(`Getting ${numOfMessages} messages from mailbox: ${fromMailbox.pathAsListed}`)

          try {
            const messages = []

            // Getting messages from "from" mailbox
            for await (let msg of fromClient.fetch(`1:*`, { uid: true, internalDate: true, flags: true })) {
              messages.push({
                uid: msg.uid,
                date: msg.internalDate,
                flags: msg.flags
              })
            }

            const msgTotal = messages.length
            log.debug(`Messages to upload from ${fromMailbox.pathAsListed} - before check: `, msgTotal)

            const msgsToProcess = []
            // Check already imported
            for (let [idx, msg] of messages.entries()) {
              const has = transferred.msgIds.find(o => o.sourceUid === msg.uid)
              if (!has) msgsToProcess.push(msg)
            }

            mailbox.msgsToProcess = msgsToProcess.length

            log.debug(`Messages to upload from ${fromMailbox.pathAsListed} - after check: ${msgsToProcess.length}  (${msgTotal - msgsToProcess.length} - db has: ${transferred.msgIds.length})`)

            const state = {
              fromEmail: fromAcc.email,
              total: msgsToProcess.length,
              running: 0,
              messagesUploadedCount: 0,
              messagesErrorCount: 0,
              messagesToRetry: [],
              attempts: 0
            }

            let msgsToProcessNow = msgsToProcess

            while (true) {
              state.attempts += 1

              const itr = msgsToProcessNow.entries()
              const promises = []

              promises.push(getAndUpload(itr, state, transferred, toMailbox, fromClient, toClient))
              promises.push(getAndUpload(itr, state, transferred, toMailbox, fromClient, toClient))

              try {
                await Promise.allSettled(promises)
                
                if (state.messagesToRetry.length && state.attempts < 2) {
                  await wait(1000 * 3)
                  log.info('Re-trying failed messages for Mailbox: ', fromMailbox.path, state.messagesToRetry.length)
                  msgsToProcessNow = state.messagesToRetry
                  continue
                }
                else {
                  break
                }
              } 
              catch (error) {
                log.error(error, 'while error')
                break
              }
            }
          } 
          catch (error) {
            log.error(error, 'Error getting messages from mailbox path: ', fromMailbox.path)
          }
          
          log.info('Completed Mailbox: ', fromMailbox.path)

          mailboxesCountMessageFromEmail.mailboxesMessages.push({
            fromMailbox: fromMailbox.path,
            toMailbox: toMailbox.path,
            totalMessages: numOfMessages,
            messagesUploaded: messagesUploadedCount
          })

          if (messagesUploadedCount !== numOfMessages) {
            log.error('Messages uploaded doen\'t matches number of total messages of', fromMailbox.path)
          }

        } 
        catch (error) {
          log.error(error, 'Error getting lock for mailboxes: ', fromMailbox.path)
        } 
        finally {
          if (fromMailboxLock) fromMailboxLock.release()
          if (toMailboxLock) toMailboxLock.release()
        }

      } 
      catch (error) {
        log.error(error, 'Error creating new mailbox: ', fromMailbox.path)
      }
    }
  }
  catch (error) {
    log.error(error, `Process error for ${fromAcc.email}`)
  }
  finally {
    if (fromClient && fromClient.authenticated) fromClient.logout()
    if (toClient && toClient.authenticated) toClient.logout()
  }

  const idx = transferring.find((t => t.email === fromAcc.email))
  if (idx > -1) transferring.splice(idx, 1)

  return mailboxesCountMessageFromEmail
}


let closing = false
const transferring = []
const main = async () => {
  const promises = []

  for (let [index, acc] of Object.entries(transferData.accounts)) {
    promises.push( process(acc) )
  }

  const mailboxesFromEmailsCountMessage = await Promise.all(promises)

  const finalLog = mailboxesFromEmailsCountMessage.reduce((prevString, email) => {
    return `
      ${prevString}
    ----------------------------------
      Email from -> to: ${email.emailFrom}  ->  ${email.emailTo}
      Mailboxes:
        ${email.mailboxesMessages.reduce((mailboxPrevString, mailbox) => {
      return `
              ${mailboxPrevString}
              - Mailbox from -> to: ${mailbox.fromMailbox}  ->  ${mailbox.toMailbox}
                Messages uploaded: ${mailbox.messagesUploaded}  of  ${mailbox.totalMessages}
            `
    }, '').trim()
      }
    `
  }, '')

  logger.info(finalLog)
}


async function close() {
  closing = true
  for (let transferred of transferring) {
    try {
      await transferred.saveDoc()
    }
    catch (err) {
      logger.error(err, 'close - error', transferred);
    }
  }
}
addCleanupListener(async () => await close())


main().catch(err => console.error(err))
