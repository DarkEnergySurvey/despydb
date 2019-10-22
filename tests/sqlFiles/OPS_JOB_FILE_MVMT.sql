CREATE TABLE "OPS_JOB_FILE_MVMT" (
"SITE" TEXT,
"HOME_ARCHIVE" TEXT,
"TARGET_ARCHIVE" TEXT,
"MVMTCLASS" TEXT NOT NULL,
PRIMARY KEY("SITE", "HOME_ARCHIVE", "TARGET_ARCHIVE")
);


INSERT INTO OPS_JOB_FILE_MVMT ('SITE','HOME_ARCHIVE','MVMTCLASS') VALUES ('descampuscluster','desar2home','filemgmt.job_mvmt_http.JobArchiveHttp');
INSERT INTO OPS_JOB_FILE_MVMT ('SITE','HOME_ARCHIVE','MVMTCLASS') VALUES ('descmp1','desar2home','filemgmt.job_mvmt_http.JobArchiveHttp');
INSERT INTO OPS_JOB_FILE_MVMT ('SITE','HOME_ARCHIVE','MVMTCLASS') VALUES ('desccp-dev','desar2home','filemgmt.job_mvmt_http.JobArchiveHttp');
INSERT INTO OPS_JOB_FILE_MVMT ('SITE','HOME_ARCHIVE','MVMTCLASS') VALUES ('iforge-dev','desar2home','filemgmt.job_mvmt_http.JobArchiveHttp');
INSERT INTO OPS_JOB_FILE_MVMT ('SITE','HOME_ARCHIVE','MVMTCLASS') VALUES ('bluewaters','desar2home','filemgmt.job_mvmt_http.JobArchiveHttp');
INSERT INTO OPS_JOB_FILE_MVMT ('SITE','HOME_ARCHIVE','MVMTCLASS') VALUES ('fermigrid','desar2home','filemgmt.job_mvmt_http.JobArchiveHttp');
INSERT INTO OPS_JOB_FILE_MVMT ('SITE','HOME_ARCHIVE','MVMTCLASS') VALUES ('descmp2','desar2home','filemgmt.job_mvmt_http.JobArchiveHttp');
INSERT INTO OPS_JOB_FILE_MVMT ('SITE','HOME_ARCHIVE','MVMTCLASS') VALUES ('descmp3','desar2home','filemgmt.job_mvmt_http.JobArchiveHttp');
INSERT INTO OPS_JOB_FILE_MVMT ('SITE','HOME_ARCHIVE','MVMTCLASS') VALUES ('nersc','desar2home','filemgmt.job_mvmt_http.JobArchiveHttp');
INSERT INTO OPS_JOB_FILE_MVMT ('SITE','HOME_ARCHIVE','MVMTCLASS') VALUES ('descmp4','desar2home','filemgmt.job_mvmt_http.JobArchiveHttp');
INSERT INTO OPS_JOB_FILE_MVMT ('SITE','HOME_ARCHIVE','MVMTCLASS') VALUES ('dessub','desar2home','filemgmt.job_mvmt_http.JobArchiveHttp');
