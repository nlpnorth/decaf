CREATE TABLE atoms (
    id INTEGER PRIMARY KEY,
    start INTEGER NOT NULL ,
    end INTEGER NOT NULL ,
    value TEXT NOT NULL
);

CREATE TABLE structures (
    id INTEGER PRIMARY KEY,
    start INTEGER NOT NULL ,
    end INTEGER NOT NULL ,
    value TEXT,
    type TEXT NOT NULL,
    subsumes INTEGER NOT NULL
);