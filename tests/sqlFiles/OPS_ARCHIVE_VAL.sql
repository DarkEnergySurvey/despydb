CREATE TABLE "OPS_ARCHIVE_VAL" (
"NAME" TEXT NOT NULL,
"KEY" TEXT NOT NULL,
"VAL" TEXT
);
INSERT INTO OPS_ARCHIVE_VAL (NAME,"KEY",VAL) VALUES ('decarchive','input_transfer_semname_home','decahttp-input');
INSERT INTO OPS_ARCHIVE_VAL (NAME,"KEY",VAL) VALUES ('decarchive','output_transfer_semname_home','decahttp-output');
INSERT INTO OPS_ARCHIVE_VAL (NAME,"KEY",VAL) VALUES ('decarchive','transfer_stats','filemgmt.transfer_stats_db.TransferStatsDB');
INSERT INTO OPS_ARCHIVE_VAL (NAME,"KEY",VAL) VALUES ('decarchive','endpoint','decahttp');
INSERT INTO OPS_ARCHIVE_VAL (NAME,"KEY",VAL) VALUES ('decarchive','root_http','http://decahttp.ncsa.illinois.edu/deca_archive');
