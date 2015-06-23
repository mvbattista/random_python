import email, imaplib, os, datetime, sys
from pprint import pprint

detach_dir = str(sys.argv[1]) # directory where to save attachments (default: current)
email_file = str(sys.argv[2])
file_counter = 1

with open (email_file, "r") as myfile:
    email_body=myfile.read()
 
mail = email.message_from_string(email_body) # parsing the mail content to get a mail object
pprint(mail)
# Check if any attachments at all
if mail.get_content_maintype() != 'multipart':
    exit()

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
    #resp, data = m.store(emailid , '+FLAGS', '(\Deleted)')