CREATE TABLE "COADD_OBJECT_EXTINCTION" (
    "COADD_OBJECT_ID" INTEGER NOT NULL,
    "FILENAME" TEXT NOT NULL,
    "EBV" FLOAT NOT NULL,
    "L" FLOAT NOT NULL,
    "B" FLOAT NOT NULL,
    PRIMARY KEY ("COADD_OBJECT_ID"));