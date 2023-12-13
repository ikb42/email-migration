const { addCleanupListener } = require('async-cleanup');
const Transferred = require('./data')
const transferData = require('./transferEmails.json')

const logger = require('./logger');
const { Client, findMailboxOrCreateIt } = require('./Client');
const { wait } = require('./utils');


function downloadMessageAndUpload(msg, toMailbox, fromClient, toClient) {

  return fromClient.imap
    .download(msg.uid, undefined, { uid: true })
    .then(({ content: contentStream }) => new Promise((resolve, reject) => {
      const buffers = []

      contentStream.on('data', (data) => buffers.push(data))

      contentStream.on('end', async () => {
        try {
          const flags = [...msg.flags].filter(flag => flag !== '\\RECEIPTCHECKED')
          let added = await toClient.imap.append(toMailbox.path, Buffer.concat(buffers), flags, msg.date)
          resolve(added)
        }
        catch (err) {
          logger.error({ err, message: msg, 'toClient_usable': toClient.imap.usable, 'fromClient_usable': fromClient.imap.usable }, `downloadMessageAndUpload for ${fromClient.label} - toClient.append - error`)
          reject({'toClient_usable': toClient.imap.usable, 'fromClient_usable': fromClient.imap.usable})
        }
      })

      contentStream.on('close', () => {
        // logger.warn("close................................. ", msg.date)
      })

      contentStream.on('error', (err) => {
        logger.error(err, `downloadMessageAndUpload for ${oClient.label} - error`)
        reject(err)
      })
    }))
}

async function getAndUpload(itr, state, transferred, toMailbox, fromClient, toClient) {
  let noticeTime = Date.now()
  let nextMsg = itr.next()

  const log = logger.child({ fromEmail: state.fromEmail })

  while (nextMsg && nextMsg.value) {
    if (closing) break
    
    const [idx, message] = nextMsg.value
    try {
      state.running += 1
      const msgAdded = await downloadMessageAndUpload(message, toMailbox, fromClient, toClient)
      
      state.messagesUploadedCount += 1
      transferred.msgIds.push({ sourceUid: message.uid, destUid: msgAdded.uid })

      if (state.messagesUploadedCount % 10 === 0 || (Date.now() - noticeTime) > (1000 * 60)) {
        transferred.saveDoc()
        if (state.messagesUploadedCount % 100 === 0 || (Date.now() - noticeTime) > (1000 * 60)) {
          log.info(`Messages uploaded: ${state.messagesUploadedCount} of ${state.total}  ${state.messagesErrorCount ? '(errors: ' + state.messagesErrorCount + ')' : ''}`)
          noticeTime = Date.now()
        }
      }
    }
    catch (er) {
      state.messagesErrorCount += 1
      state.messagesToRetry.push(message)
      log.error(er, `getAndUpload - error - errorCount: ${state.messagesErrorCount}  (uploaded: ${state.messagesUploadedCount} of ${state.total})`)

      if (er.fromClient_usable !== true) {
        await fromClient.reCreateAndConnectClient()
      }
      if (er.toClient_usable !== true) {
        await toClient.reCreateAndConnectClient()
      }
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
    fromClient = new Client(transferData.providers[fromAcc.provider], fromAcc, 'from')
    await fromClient.connectClient()
    toClient = new Client(transferData.providers[toAcc.provider], toAcc, 'to')
    await toClient.connectClient()

    const fromQuota = await fromClient.imap.getQuota()
    const toQuota = await toClient.imap.getQuota()

    if (fromQuota && toQuota && fromQuota.storage.used > toQuota.storage.available) {
      throw new Error('Disk quota used in the From email is bigger than the To email')
    }

    // Get all emails mailboxes
    const mailboxesOfFromEmail = await fromClient.imap.list()
    const mailboxesOfToEmail = await toClient.imap.list()

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


      let toMailbox
      try {
        toMailbox = await findMailboxOrCreateIt(mailboxesOfToEmail, fromMailbox, toClient.imap)
      } 
      catch (error) {
        log.error(error, 'Error creating new mailbox: ', fromMailbox.path)
      }

      if (toMailbox) {
        let fromMailboxLock
        let toMailboxLock

        try {
          fromMailboxLock = await fromClient.imap.getMailboxLock(fromMailbox.path)
          toMailboxLock = await toClient.imap.getMailboxLock(toMailbox.path)
        }
        catch (error) {
          log.error(error, 'Error getting lock for mailboxes: ', fromMailbox.path)
        }

        if (fromMailboxLock) {
          const { messages: numOfMessages } = await fromClient.imap.status(fromMailbox.path, { messages: true })
          mailbox.numOfMessages = numOfMessages

          log.debug(`Getting ${numOfMessages} messages from mailbox: ${fromMailbox.pathAsListed}`)

          const state = {
            fromEmail: fromAcc.email,
            total: 0,
            running: 0,
            messagesUploadedCount: 0,
            messagesErrorCount: 0,
            messagesToRetry: [],
            attempts: 0
          }

          try {
            const messages = []

            // Getting messages from "from" mailbox
            for await (let msg of fromClient.imap.fetch(`1:*`, { uid: true, internalDate: true, flags: true })) {
              messages.push({
                uid: msg.uid,
                date: msg.internalDate,
                flags: msg.flags
              })
            }

            const msgTotal = messages.length
            const msgsToProcess = []
            let alreadyUploaded = 0
            // Check already imported
            for (let [idx, msg] of messages.entries()) {
              const has = transferred.msgIds.find(o => o.sourceUid === msg.uid)
              if (!has) msgsToProcess.push(msg)
              else alreadyUploaded += 1
            }

            mailbox.msgsToProcess = msgsToProcess.length
            mailbox.alreadyUploaded = alreadyUploaded
            state.total = msgsToProcess.length

            log.debug(`Messages to upload from ${fromMailbox.pathAsListed}: ${msgsToProcess.length}  (total ${msgTotal} & db has: ${alreadyUploaded})`)

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
            messagesUploaded: state.messagesUploadedCount,
            previouslyUploaded: mailbox.alreadyUploaded
          })
        }

        if (fromMailboxLock) fromMailboxLock.release()
        if (toMailboxLock) toMailboxLock.release()
      }
      
    }

    log.info('Migration complete')
  }
  catch (error) {
    log.error(error, `Process error for ${fromAcc.email}`)
  }
  finally {
    if (fromClient && fromClient.imap.authenticated) fromClient.imap.logout()
    if (toClient && toClient.imap.authenticated) toClient.imap.logout()
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
    if (acc.include === false) continue
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
                Messages uploaded: ${mailbox.messagesUploaded}  (prev. uploaded ${mailbox.previouslyUploaded})  of  ${mailbox.totalMessages}
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
