CREATE TABLE "OPS_FILENAME_PATTERN" (
"NAME" TEXT NOT NULL,
"PATTERN" TEXT NOT NULL
);
INSERT INTO OPS_FILENAME_PATTERN (NAME,PATTERN) VALUES ('nite_band','${camsym}_n${nite}_${band}_r${reqnum}p${attnum:2}_${flabel}.${fsuffix}');
INSERT INTO OPS_FILENAME_PATTERN (NAME,PATTERN) VALUES ('nite_noband','${camsym}_n${nite}_r${reqnum}p${attnum:2}_${flabel}.${fsuffix}');
INSERT INTO OPS_FILENAME_PATTERN (NAME,PATTERN) VALUES ('exposure','${camsym}${expnum:8}_${band}_r${reqnum}p${attnum:2}_${flabel}.${fsuffix}');
INSERT INTO OPS_FILENAME_PATTERN (NAME,PATTERN) VALUES ('ccdnum_band','${camsym}${expnum:8}_${band}_c${ccdnum:2}_r${reqnum}p${attnum:2}_${flabel}.${fsuffix}');
INSERT INTO OPS_FILENAME_PATTERN (NAME,PATTERN) VALUES ('ccdnum_noband','${camsym}${expnum:8}_c${ccdnum:2}_r${reqnum}p${attnum:2}_${flabel}.${fsuffix}');
INSERT INTO OPS_FILENAME_PATTERN (NAME,PATTERN) VALUES ('nite_ccdnum_band','${camsym}_n${nite}_${band}_c${ccdnum:2}_r${reqnum}p${attnum:2}_${flabel}.${fsuffix}');
INSERT INTO OPS_FILENAME_PATTERN (NAME,PATTERN) VALUES ('nite_ccdnum_noband','${camsym}_n${nite}_c${ccdnum:2}_r${reqnum}p${attnum:2}_${flabel}.${fsuffix}');
INSERT INTO OPS_FILENAME_PATTERN (NAME,PATTERN) VALUES ('sn_ccdnum_band','${camsym}${expnum:8}_${field}_${band}_c${ccdnum:2}_r${reqnum}p${attnum:2}_${flabel}.${fsuffix}');
INSERT INTO OPS_FILENAME_PATTERN (NAME,PATTERN) VALUES ('sn_exposure','${camsym}${expnum:8}_${field}_${band}_r${reqnum}p${attnum:2}_${flabel}.${fsuffix}');
INSERT INTO OPS_FILENAME_PATTERN (NAME,PATTERN) VALUES ('sntmpl','SNTemplate_${field}_${tmpl_version}_${band}_${ccdnum:2}.${fsuffix}');
INSERT INTO OPS_FILENAME_PATTERN (NAME,PATTERN) VALUES ('sn_ccdnum_band_combined','${camsym}${field}_combined_${band}_c${ccdnum:2}_r${reqnum}p${attnum:2}_${flabel}.${fsuffix}');
INSERT INTO OPS_FILENAME_PATTERN (NAME,PATTERN) VALUES ('sn_field_ccdnum_band','${camsym}_${field}_${band}_c${ccdnum:2}_r${reqnum}p${attnum:2}_${flabel}.${fsuffix}');
INSERT INTO OPS_FILENAME_PATTERN (NAME,PATTERN) VALUES ('unitname_ccdnum_band','${camsym}_n${unitname}_${band}_c${ccdnum:2}_r${reqnum}p${attnum:2}_${flabel}.${fsuffix}');
INSERT INTO OPS_FILENAME_PATTERN (NAME,PATTERN) VALUES ('unitname_ccdnum_noband','${camsym}_n${unitname}_c${ccdnum:2}_r${reqnum}p${attnum:2}_${flabel}.${fsuffix}');
INSERT INTO OPS_FILENAME_PATTERN (NAME,PATTERN) VALUES ('unitname_generic','${unitname}_r${reqnum}p${attnum:2}_${flabel}.${fsuffix}');
INSERT INTO OPS_FILENAME_PATTERN (NAME,PATTERN) VALUES ('unitname_generic_nolabel','${unitname}_r${reqnum}p${attnum:2}.${fsuffix}');
INSERT INTO OPS_FILENAME_PATTERN (NAME,PATTERN) VALUES ('tile_generic','${tilename}_r${reqnum}p${attnum:2}_${flabel}.${fsuffix}');
INSERT INTO OPS_FILENAME_PATTERN (NAME,PATTERN) VALUES ('tile_generic_nolabel','${tilename}_r${reqnum}p${attnum:2}.${fsuffix}');
INSERT INTO OPS_FILENAME_PATTERN (NAME,PATTERN) VALUES ('tile_band','${tilename}_r${reqnum}p${attnum:2}_${band}_${flabel}.${fsuffix}');
INSERT INTO OPS_FILENAME_PATTERN (NAME,PATTERN) VALUES ('tile_band_nolabel','${tilename}_r${reqnum}p${attnum:2}_${band}.fits');
INSERT INTO OPS_FILENAME_PATTERN (NAME,PATTERN) VALUES ('tile_det','${tilename}_r${reqnum}p${attnum:2}_det_${flabel}.${fsuffix}');
INSERT INTO OPS_FILENAME_PATTERN (NAME,PATTERN) VALUES ('tile_det_nolabel','${tilename}_r${reqnum}p${attnum:2}_det.${fsuffix}');
INSERT INTO OPS_FILENAME_PATTERN (NAME,PATTERN) VALUES ('tile_expband','${tilename}_r${reqnum}p${attnum:2}_${camsym}${expnum:8}_${band}_${flabel}.${fsuffix}');
INSERT INTO OPS_FILENAME_PATTERN (NAME,PATTERN) VALUES ('tile_expbandccd','${tilename}_r${reqnum}p${attnum:2}_${camsym}${expnum:8}_${band}_c${ccdnum:2}_${flabel}.${fsuffix}');
INSERT INTO OPS_FILENAME_PATTERN (NAME,PATTERN) VALUES ('tile_ccd','${tilename}_r${reqnum}p${attnum:2}_c${ccdnum:2}_${modulename}');
INSERT INTO OPS_FILENAME_PATTERN (NAME,PATTERN) VALUES ('tile_bandccd','${tilename}_r${reqnum}p${attnum:2}_${band}_c${ccdnum:2}_${modulename}');
