CREATE TABLE "OPS_DIRECTORY_PATTERN" (
"NAME" TEXT NOT NULL,
"OPS" TEXT NOT NULL,
"RUNTIME" TEXT NOT NULL
);
INSERT INTO OPS_DIRECTORY_PATTERN (NAME,OPS,RUNTIME) VALUES ('pst','${ops_run_dir}/${ops_enddir}','${rundir}');
INSERT INTO OPS_DIRECTORY_PATTERN (NAME,OPS,RUNTIME) VALUES ('runtime','tbd','${rundir}');
INSERT INTO OPS_DIRECTORY_PATTERN (NAME,OPS,RUNTIME) VALUES ('precal','${ops_run_dir}/${ops_enddir}','${rundir}');
INSERT INTO OPS_DIRECTORY_PATTERN (NAME,OPS,RUNTIME) VALUES ('se','${ops_run_dir}/${ops_enddir}','${rundir}');
INSERT INTO OPS_DIRECTORY_PATTERN (NAME,OPS,RUNTIME) VALUES ('raw','RAW/${nite}','${rundir}');
INSERT INTO OPS_DIRECTORY_PATTERN (NAME,OPS,RUNTIME) VALUES ('sne','${ops_run_dir}/${ops_enddir}','${rundir}');
INSERT INTO OPS_DIRECTORY_PATTERN (NAME,OPS,RUNTIME) VALUES ('sne_exp','${ops_run_dir}/${camera}${expnum:8}/${ops_enddir}','${camera}${expnum:8}/${rundir}');
INSERT INTO OPS_DIRECTORY_PATTERN (NAME,OPS,RUNTIME) VALUES ('sne_combined','${ops_run_dir}/combined/${ops_enddir}','combined/${rundir}');
INSERT INTO OPS_DIRECTORY_PATTERN (NAME,OPS,RUNTIME) VALUES ('sne_ccd','${ops_run_dir}/ccd${ccdnum:2}/${ops_enddir}','ccd${ccdnum:2}/${rundir}');
INSERT INTO OPS_DIRECTORY_PATTERN (NAME,OPS,RUNTIME) VALUES ('mepoch','${ops_run_dir}/${ops_enddir}','${rundir}');
INSERT INTO OPS_DIRECTORY_PATTERN (NAME,OPS,RUNTIME) VALUES ('snmanifest','DTS/snmanifest/${nite}','${rundir}');
