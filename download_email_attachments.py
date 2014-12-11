import email, imaplib, os, datetime, sys

if len(sys.argv) < 3:
    sys.exit('Usage: %s directory email-label' % sys.argv[0])

detach_dir = str(sys.argv[1]) # directory where to save attachments (default: current)
email_label = str(sys.argv[2])

user = '' # GMail Username
pwd = '' # GMail Password

# connecting to the gmail imap server
m = imaplib.IMAP4_SSL("imap.gmail.com")
m.login(user,pwd)
m.select(email_label) # here you a can choose a mail box like INBOX instead
# use m.list() to get all the mailboxes

#resp, items = m.search(None, 'FROM', '"Impact Stats Script"') # you could filter using the IMAP rules here (check http://www.example-code.com/csharp/imap-search-critera.asp)
resp, items = m.search(None, "UNSEEN")
items = items[0].split() # getting the mails id
file_counter = 1

for emailid in items:
    resp, data = m.fetch(emailid, "(RFC822)") # fetching the mail, "`(RFC822)`" means "get the whole stuff", but you can ask for headers only, etc
    email_body = data[0][1] # getting the mail content
    mail = email.message_from_bytes(email_body) # parsing the mail content to get a mail object

    # Check if any attachments at all
    if mail.get_content_maintype() != 'multipart':
        continue

    print('[' + mail["From"] + '] :' + mail["Subject"])

    # we use walk to create a generator so we can iterate on the parts and forget about the recursive headache
    for part in mail.walk():
        # multipart are just containers, so we skip them
        if part.get_content_maintype() == 'multipart':
            continue

        # is this part an attachment ?
        if part.get('Content-Disposition') is None:
            continue

        filename = part.get_filename()
        counter = 1

        # if there is no filename, we create one with a counter to avoid duplicates
        if not filename:
            filename = 'part-%03d%s' % (counter, 'bin')
            counter += 1
        else:
            new_filename, file_extension = os.path.splitext(filename)
            time_now = datetime.datetime.now()
            new_filename += '-'
            new_filename += time_now.strftime('%Y%m%d-%H%M%S')
            new_filename += '-'
            new_filename += str(file_counter)
            new_filename += file_extension
            print(new_filename)
            filename = new_filename

        att_path = os.path.join(detach_dir, filename)

        #Check if its already there
        if not os.path.isfile(att_path) :
            # finally write the stuff
            fp = open(att_path, 'wb')
            fp.write(part.get_payload(decode=True))
            fp.close()
            file_counter += 1

        # Remove the label
        resp, data = m.store(emailid , '+FLAGS', '(\Deleted)')