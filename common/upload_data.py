import helpers

if __name__ == '__main__':

    block = 'morena'
    file_from = './nrega_output/' + block + '.csv'
    file_to = ''
    to_dropbox = 0
    helpers.upload_data(block, file_from, file_to, to_dropbox)