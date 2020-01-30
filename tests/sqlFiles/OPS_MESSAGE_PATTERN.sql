CREATE TABLE "OPS_MESSAGE_PATTERN" (
"ID" INTEGER NOT NULL,
"LVL" INTEGER DEFAULT 3 NOT NULL,
"PRIORITY" INTEGER NOT NULL,
"EXECNAME" TEXT DEFAULT 'global' NOT NULL,
"USED" TEXT DEFAULT 'y' NOT NULL,
"NUMBER_OF_LINES" INTEGER DEFAULT 1 NOT NULL,
"PATTERN" TEXT NOT NULL,
"ONLY_MATCHED" TEXT DEFAULT 'N' NOT NULL,
"LAST_CHANGED_DATE" TIMESTAMP (6) DEFAULT 1234567890. NOT NULL,
"LAST_CHANGED_USER" TEXT DEFAULT 'USER' NOT NULL,
PRIMARY KEY ("ID"));
