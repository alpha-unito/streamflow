CREATE TABLE step (id INTEGER PRIMARY KEY,
                   name TEXT,
                   status INTEGER);
CREATE TABLE command (id INTEGER PRIMARY KEY,
                      step INTEGER,
                      cmd TEXT,
                      output BLOB,
                      status INTEGER,
                      start_time INTEGER,
                      end_time INTEGER,
                      FOREIGN KEY(step) REFERENCES step(id));
