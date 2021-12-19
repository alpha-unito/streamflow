CREATE TABLE step
(
    id     INTEGER PRIMARY KEY,
    name   TEXT,
    status INTEGER
);

CREATE TABLE command
(
    id         INTEGER PRIMARY KEY,
    step       INTEGER,
    cmd        TEXT,
    output     BLOB,
    status     INTEGER,
    start_time INTEGER,
    end_time   INTEGER,
    FOREIGN KEY (step) REFERENCES step (id)
);

CREATE TABLE dependency
(
    dependee INTEGER,
    depender INTEGER,
    PRIMARY KEY (dependee, depender),
    FOREIGN KEY (dependee) REFERENCES step (id),
    FOREIGN KEY (depender) REFERENCES step (id)
)
