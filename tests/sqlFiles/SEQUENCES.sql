CREATE TABLE "SEQUENCES_DATA" (
    "NAME"  TEXT NOT NULL,
    "SEQ_VAL"   INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY("NAME")
);

INSERT INTO SEQUENCES_DATA (NAME) VALUES('DESFILE_SEQ');
INSERT INTO SEQUENCES_DATA (NAME) VALUES('PFW_WRAPPER_SEQ');
INSERT INTO SEQUENCES_DATA (NAME) VALUES('PFW_EXEC_SEQ');
INSERT INTO SEQUENCES_DATA (NAME) VALUES('PFW_ATTEMPT_SEQ');
INSERT INTO SEQUENCES_DATA (NAME) VALUES('TASK_SEQ');
INSERT INTO SEQUENCES_DATA (NAME) VALUES('SEMINFO_SEQ');

CREATE TABLE "DUMMY" (
    "NAME"  TEXT NOT NULL,
    "JUNK"  INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY("NAME")
);

INSERT INTO DUMMY (NAME) VALUES('DESFILE_SEQ');
INSERT INTO DUMMY (NAME) VALUES('PFW_WRAPPER_SEQ');
INSERT INTO DUMMY (NAME) VALUES('PFW_EXEC_SEQ');
INSERT INTO DUMMY (NAME) VALUES('PFW_ATTEMPT_SEQ');
INSERT INTO DUMMY (NAME) VALUES('TASK_SEQ');
INSERT INTO DUMMY (NAME) VALUES('SEMINFO_SEQ');

CREATE VIEW SEQUENCES AS SELECT d.NAME, s.SEQ_VAL, d.JUNK from DUMMY d, SEQUENCES_DATA s where s.NAME=d.NAME;

CREATE TRIGGER UpdateSeq
INSTEAD OF UPDATE ON SEQUENCES
BEGIN
UPDATE SEQUENCES_DATA SET SEQ_VAL = SEQ_VAL + 1 WHERE NAME=NEW.NAME;
end;
