import MySQLdb

CREATE_TABLE_QUERY = '''
CREATE TABLE IF NOT EXISTS `Tickets64_%s` (
  `id` bigint(20) unsigned NOT NULL auto_increment,
  `stub` char(1) NOT NULL default '',
  PRIMARY KEY  (`id`),
  UNIQUE KEY `stub` (`stub`)
) ENGINE=MyISAM
'''

SET_AUTO_INCREMENT_INCREMENT_QUERY = '''
SET @@auto_increment_increment=%s
'''

SET_AUTO_INCREMENT_OFFSET_QUERY = '''
SET @@auto_increment_offset=%s
'''

RESET_AUTO_INCREMENT_INCREMENT_QUERY = '''
SET @@auto_increment_increment=1
'''

RESET_AUTO_INCREMENT_OFFSET_QUERY = '''
SET @@auto_increment_increment=1
'''

NEW_ID_REPLACE_QUERY = '''
REPLACE INTO Tickets64_%s (stub) VALUES ('a')
'''

NEW_ID_SELECT_QUERY = '''
SELECT LAST_INSERT_ID()
'''

class IdWorker(object):
    def __init__(self, worker_id, worker_increment, worker_offset, mysql_host, mysql_user, mysql_passwd, mysql_db):
        assert worker_id >= 0
        assert worker_increment >= 1
        assert worker_offset >= 0
        
        self.worker_id = worker_id
        self.worker_increment = worker_increment
        self.worker_offset = worker_offset
        
        self.db = MySQLdb.connect(host=mysql_host, user=mysql_user, passwd=mysql_passwd, db=mysql_db)
        self.cursor = self.db.cursor()
        self.init_db()
        
        print "worker starting. worker_id %d, worker_increment %d, worker_offset %d" \
                %(self.worker_id, self.worker_increment, self.worker_offset)
    
    def init_db(self):
        self.cursor.execute(CREATE_TABLE_QUERY %self.worker_id)
        self.cursor.execute(SET_AUTO_INCREMENT_INCREMENT_QUERY %self.worker_increment)
        self.cursor.execute(SET_AUTO_INCREMENT_OFFSET_QUERY %self.worker_offset)
    
    def reset_auto_increment(self):
        self.cursor.execute(RESET_AUTO_INCREMENT_INCREMENT_QUERY)
        self.cursor.execute(RESET_AUTO_INCREMENT_OFFSET_QUERY)
    
    def close_db(self):
        try:
            self.cursor.close()
        except:
            pass
        try:
            self.db.close()
        except:
            pass
    
    def __del__(self):
        try:
            self.reset_auto_increment()
            self.close_db()
        except:
            pass
    
    def get_id(self):
        return self.next_id()
    
    def get_worker_id(self):
        return self.worker_id
    
    def get_worker_increment(self):
        return self.worker_increment
    
    def get_worker_offset(self):
        return self.worker_offset
    
    def next_id(self):
        self.cursor.execute(NEW_ID_REPLACE_QUERY %self.worker_id)
        self.cursor.execute(NEW_ID_SELECT_QUERY)
        return self.cursor.fetchone()[0]