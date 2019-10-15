CREATE TABLE "OPS_SITE_VAL" (
"NAME" TEXT NOT NULL,
"KEY" TEXT NOT NULL,
"VAL" TEXT
);
INSERT INTO OPS_SITE_VAL (NAME,"KEY",VAL) VALUES ('bluewaters','batchtype','glidein');
INSERT INTO OPS_SITE_VAL (NAME,"KEY",VAL) VALUES ('bluewaters','uiddomain','ncsa.illinois.edu');
INSERT INTO OPS_SITE_VAL (NAME,"KEY",VAL) VALUES ('descampuscluster','batchtype','nodeset');
INSERT INTO OPS_SITE_VAL (NAME,"KEY",VAL) VALUES ('descampuscluster','nodeset','CampusClusterDES');
INSERT INTO OPS_SITE_VAL (NAME,"KEY",VAL) VALUES ('descampuscluster','dynslots','true');
INSERT INTO OPS_SITE_VAL (NAME,"KEY",VAL) VALUES ('descmp1','batchtype','local');
INSERT INTO OPS_SITE_VAL (NAME,"KEY",VAL) VALUES ('descmp1','loginhost','descmp1.cosmology.illinois.edu');
INSERT INTO OPS_SITE_VAL (NAME,"KEY",VAL) VALUES ('descmp1','batchtype','local');
INSERT INTO OPS_SITE_VAL (NAME,"KEY",VAL) VALUES ('descmp1','loginhost','descmp1.cosmology.illinois.edu');
INSERT INTO OPS_SITE_VAL (NAME,"KEY",VAL) VALUES ('descmp2','loginhost','descmp2.cosmology.illinois.edu');
INSERT INTO OPS_SITE_VAL (NAME,"KEY",VAL) VALUES ('descmp2','batchtype','local');
INSERT INTO OPS_SITE_VAL (NAME,"KEY",VAL) VALUES ('descmp3','loginhost','descmp3.cosmology.illinois.edu');
INSERT INTO OPS_SITE_VAL (NAME,"KEY",VAL) VALUES ('descmp3','batchtype','local');
INSERT INTO OPS_SITE_VAL (NAME,"KEY",VAL) VALUES ('descmp4','batchtype','local');
INSERT INTO OPS_SITE_VAL (NAME,"KEY",VAL) VALUES ('descmp4','loginhost','descmp4.cosmology.illinois.edu');
INSERT INTO OPS_SITE_VAL (NAME,"KEY",VAL) VALUES ('fermigrid','condorjobreq','(Target.IsDESNode == True)');
INSERT INTO OPS_SITE_VAL (NAME,"KEY",VAL) VALUES ('fermigrid','condorjobclass','DES');
INSERT INTO OPS_SITE_VAL (NAME,"KEY",VAL) VALUES ('fermigrid','gridport','9619');
INSERT INTO OPS_SITE_VAL (NAME,"KEY",VAL) VALUES ('fermigrid','gridhost','gpce04.fnal.gov');
INSERT INTO OPS_SITE_VAL (NAME,"KEY",VAL) VALUES ('fermigrid','batchtype','condor');
INSERT INTO OPS_SITE_VAL (NAME,"KEY",VAL) VALUES ('fermigrid','dcache','true');
INSERT INTO OPS_SITE_VAL (NAME,"KEY",VAL) VALUES ('decasub','loginhost','decasub.ncsa.illinois.edu');
INSERT INTO OPS_SITE_VAL (NAME,"KEY",VAL) VALUES ('decasub','batchtype','local');
