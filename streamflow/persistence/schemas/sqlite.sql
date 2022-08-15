CREATE TABLE workflow
(
    id     INTEGER PRIMARY KEY,
    name   TEXT UNIQUE,
    params TEXT,
    status INTEGER,
    type   TEXT
);

CREATE TABLE step
(
    id       INTEGER PRIMARY KEY,
    name     TEXT,
    workflow INTEGER,
    status   INTEGER,
    type     TEXT,
    params   TEXT,
    FOREIGN KEY (workflow) REFERENCES workflow (id)
);

CREATE TABLE port
(
    id       INTEGER PRIMARY KEY,
    name     TEXT,
    workflow INTEGER,
    type     TEXT,
    params   TEXT,
    FOREIGN KEY (workflow) REFERENCES workflow (id)
);


CREATE TABLE dependency
(
    step INTEGER,
    port INTEGER,
    type INTEGER,
    name TEXT,
    PRIMARY KEY (step, port, type, name),
    FOREIGN KEY (step) REFERENCES step (id),
    FOREIGN KEY (port) REFERENCES port (id)
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


CREATE TABLE token
(
    id    INTEGER PRIMARY KEY,
    port  INTEGER,
    tag   TEXT,
    type  TEXT,
    value BLOB,
    FOREIGN KEY (port) REFERENCES port (id)
);


CREATE TABLE provenance
(
    dependee INTEGER,
    depender INTEGER,
    PRIMARY KEY (dependee, depender),
    FOREIGN KEY (dependee) REFERENCES token (id),
    FOREIGN KEY (depender) REFERENCES token (id)
);


CREATE table deployment
(
    id       INTEGER PRIMARY KEY,
    name     TEXT,
    type     TEXT,
    config   TEXT,
    external INTEGER,
    lazy     INTEGER,
    workdir  TEXT
);


CREATE table target
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
