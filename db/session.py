import os
import getpass
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker



def savedb(name):
    ''' Saves the given database name in a file named dbpaths.py, so that it
        can be imported automatically next time
    '''
    filepath = '%s/dbpaths.py' % os.path.dirname(os.path.abspath(__file__))
    with open(filepath, 'w') as f:
        f.write('dbpath = "%s"' % name)
    print('Successfully wrote database url to file db/dbpaths.py, please edit if necessary')


def getdb():
    ''' Prompts the user to enter the database config manually
    '''
    try:
        from .dbpaths import dbpath
        return dbpath
    except ImportError:
        # The user hasn't set up a local dbpaths.py file yet, so here we ask if
        # they want to have it wet up manually:
        shouldprompt = raw_input('Error: Database Path Not Configured, Enter Manually? y/N: ').strip() == 'y'
        if shouldprompt:
            # Get host, username, and password for database:
            host = raw_input('Please Enter The Database Host: ').strip()
            username = raw_input('Please Enter The Database Username: ').strip()
            password = getpass.getpass('Please Enter The Database Password: ')
            dbpath = 'postgresql://%s:%s@%s/findb' % (username, password, host)

            # Ask the user if they want their new configuration to be saved:
            shouldsave = raw_input('Should we save this DB config for next time? y/N: ').strip() == 'y'
            if shouldsave:
                savedb(dbpath)
            return dbpath

        raise Exception('No Database Available')



dbpath = getdb()
engine = create_engine(dbpath)
Session = sessionmaker(bind=engine)
Session.configure(bind=engine)

session = Session()
