 CREATE TABLE "SE_OBJECT_HPIX" (
    "FILENAME" TEXT NOT NULL,
    "OBJECT_NUMBER" INTEGER NOT NULL,
    "REQNUM" INTEGER,
    "HPIX_32" INTEGER NOT NULL,
    "HPIX_64" INTEGER NOT NULL,
    "HPIX_1024" INTEGER NOT NULL,
    "HPIX_4096" INTEGER NOT NULL,
    "HPIX_16384" INTEGER NOT NULL,
    "SE_CAT_FILENAME" TEXT NOT NULL,
    PRIMARY KEY ('FILENAME','OBJECT_NUMBER'));
