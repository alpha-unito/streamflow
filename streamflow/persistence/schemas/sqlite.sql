CREATE TABLE IF NOT EXISTS workflow
(
    id         INTEGER PRIMARY KEY,
    name       TEXT,
    params     TEXT,
    status     INTEGER,
    type       TEXT,
    start_time INTEGER,
    end_time   INTEGER
);

CREATE TABLE IF NOT EXISTS step
(
    id       INTEGER PRIMARY KEY,
    name     TEXT,
    workflow INTEGER,
    status   INTEGER,
    type     TEXT,
    params   TEXT,
    FOREIGN KEY (workflow) REFERENCES workflow (id)
);

CREATE TABLE IF NOT EXISTS port
(
    id       INTEGER PRIMARY KEY,
    name     TEXT,
    workflow INTEGER,
    type     TEXT,
    params   TEXT,
    FOREIGN KEY (workflow) REFERENCES workflow (id)
);


CREATE TABLE IF NOT EXISTS dependency
(
    step INTEGER,
    port INTEGER,
    type INTEGER,
    name TEXT,
    PRIMARY KEY (step, port, type, name),
    FOREIGN KEY (step) REFERENCES step (id),
    FOREIGN KEY (port) REFERENCES port (id)
);


CREATE TABLE IF NOT EXISTS command
(
    id         INTEGER PRIMARY KEY,
    step       INTEGER,
    tag        TEXT,
    cmd        TEXT,
    output     BLOB,
    status     INTEGER,
    start_time INTEGER,
    end_time   INTEGER,
    FOREIGN KEY (step) REFERENCES step (id)
);


CREATE TABLE IF NOT EXISTS token
(
    id    INTEGER PRIMARY KEY,
    port  INTEGER,
    tag   TEXT,
    type  TEXT,
    value BLOB,
    FOREIGN KEY (port) REFERENCES port (id)
);


CREATE TABLE IF NOT EXISTS provenance
(
    dependee INTEGER,
    depender INTEGER,
    PRIMARY KEY (dependee, depender),
    FOREIGN KEY (dependee) REFERENCES token (id),
    FOREIGN KEY (depender) REFERENCES token (id)
);


CREATE TABLE IF NOT EXISTS deployment
(
    id       INTEGER PRIMARY KEY,
    name     TEXT,
    type     TEXT,
    config   TEXT,
    external INTEGER,
    lazy     INTEGER,
    workdir  TEXT
);


CREATE TABLE IF NOT EXISTS target
(
    id         INTEGER PRIMARY KEY,
    deployment INTEGER,
    type       TEXT,
    locations  INTEGER,
    service    TEXT,
    workdir    TEXT,
    params     TEXT,
    FOREIGN KEY (deployment) REFERENCES deployment (id)
);
