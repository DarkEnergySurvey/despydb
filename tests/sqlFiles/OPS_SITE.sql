CREATE TABLE "OPS_SITE" (
"NAME" TEXT NOT NULL,
"GRIDTYPE" TEXT NOT NULL,
"SETUPEUPS" TEXT NOT NULL,
"JOBROOT" TEXT NOT NULL,
PRIMARY KEY ("NAME"));
INSERT INTO OPS_SITE (NAME,GRIDTYPE,SETUPEUPS,JOBROOT) VALUES ('descampuscluster','condor','/scratch.local/desdm/stack/eups/desdm_eups_setup.sh','$_CONDOR_SCRATCH_DIR');
INSERT INTO OPS_SITE (NAME,GRIDTYPE,SETUPEUPS,JOBROOT) VALUES ('descmp1','condor','/work/apps/RHEL6/dist/eups/desdm_eups_setup.sh','/cluster_scratch/desdm_ops/${HOME_ARCHIVE}/jobroot');
INSERT INTO OPS_SITE (NAME,GRIDTYPE,SETUPEUPS,JOBROOT) VALUES ('fermigrid','condor-ce','/cvmfs/des.opensciencegrid.org/2015_Q2/eeups/SL6/eups/no_home_gridstart.sh','$_CONDOR_SCRATCH_DIR');
INSERT INTO OPS_SITE (NAME,GRIDTYPE,SETUPEUPS,JOBROOT) VALUES ('bluewaters','condor','/tmp/eeups/eups/desdm_eups_setup_local.sh','$_CONDOR_SCRATCH_DIR');
INSERT INTO OPS_SITE (NAME,GRIDTYPE,SETUPEUPS,JOBROOT) VALUES ('descmp2','condor','/local/stack/eups/desdm_eups_setup.sh','/cluster_scratch/desdm_ops/${HOME_ARCHIVE}/jobroot');
INSERT INTO OPS_SITE (NAME,GRIDTYPE,SETUPEUPS,JOBROOT) VALUES ('descmp3','condor','/work/apps/RHEL6/dist/eups/desdm_eups_setup.sh','/cluster_scratch/refact/descmp3/jobroot');
INSERT INTO OPS_SITE (NAME,GRIDTYPE,SETUPEUPS,JOBROOT) VALUES ('descmp4','condor','/work/apps/RHEL6/dist/eups/desdm_eups_setup.sh','/cluster_scratch/desdm_ops/${HOME_ARCHIVE}/jobroot');
INSERT INTO OPS_SITE (NAME,GRIDTYPE,SETUPEUPS,JOBROOT) VALUES ('decasub','condor','/des002/apps/RHEL7/vm/eups/1.2.30/desdm_eups_setup.sh','/decade/scratch/${HOME_ARCHIVE}/jobroot');
;
