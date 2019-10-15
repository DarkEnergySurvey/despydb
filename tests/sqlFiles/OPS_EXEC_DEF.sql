CREATE TABLE "OPS_EXEC_DEF" (
"NAME" TEXT NOT NULL,
"PACKAGE" TEXT,
"VERSION_FLAG" TEXT,
"VERSION_PATTERN" TEXT
);
INSERT INTO OPS_EXEC_DEF (NAME,"PACKAGE",VERSION_FLAG,VERSION_PATTERN) VALUES ('fpack','finalcut','-V','(.+)');
INSERT INTO OPS_EXEC_DEF (NAME,"PACKAGE",VERSION_FLAG,VERSION_PATTERN) VALUES ('mkbleedmask','desdmsoft','-version','c (\d+ \d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d.)');
INSERT INTO OPS_EXEC_DEF (NAME,"PACKAGE",VERSION_FLAG,VERSION_PATTERN) VALUES ('mkmask','desdmsoft','-version','c (\d+ \d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d.)');
INSERT INTO OPS_EXEC_DEF (NAME,"PACKAGE",VERSION_FLAG,VERSION_PATTERN) VALUES ('imcorrect','desdmsoft','-version','c (\d+ \d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d.)');
INSERT INTO OPS_EXEC_DEF (NAME,"PACKAGE",VERSION_FLAG,VERSION_PATTERN) VALUES ('scamp','desdmstack','-v','version ([^\s]+)');
INSERT INTO OPS_EXEC_DEF (NAME,"PACKAGE",VERSION_FLAG,VERSION_PATTERN) VALUES ('psfex','desdmstack','-v','version ([^\s]+)');
INSERT INTO OPS_EXEC_DEF (NAME,"PACKAGE",VERSION_FLAG,VERSION_PATTERN) VALUES ('sex','desdmstack','-v','version ([^\s]+)');
INSERT INTO OPS_EXEC_DEF (NAME,"PACKAGE",VERSION_FLAG,VERSION_PATTERN) VALUES ('DECam_crosstalk','desdmsoft','-version','c (\d+ \d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d.)');
