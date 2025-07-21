CREATE TABLE literals (
    id INTEGER PRIMARY KEY,
    start INTEGER NOT NULL,
    end INTEGER NOT NULL,
    value TEXT NOT NULL
);

CREATE TABLE structures (
    id INTEGER PRIMARY KEY,
    start INTEGER NOT NULL,
    end INTEGER NOT NULL,
    type TEXT NOT NULL,
    value TEXT
);

CREATE TABLE structure_literals (
    structure INTEGER,
    literal INTEGER,
    FOREIGN KEY (structure) REFERENCES structures(id) ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (literal) REFERENCES literals(id) ON UPDATE CASCADE ON DELETE CASCADE,
    PRIMARY KEY (structure, literal)
);

CREATE TABLE hierarchical_structures (
    parent INTEGER,
    child INTEGER,
    FOREIGN KEY (parent) REFERENCES structures(id) ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (child) REFERENCES structures(id) ON UPDATE CASCADE ON DELETE CASCADE,
    PRIMARY KEY (parent, child)
);