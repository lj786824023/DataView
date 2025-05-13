with tabcol as
 (select distinct t1.table_name,
                  t1.DATA_TYPE,
                  t1.DATA_LENGTH,
                  t1.DATA_PRECISION,
                  t1.DATA_SCALE,
                  t1.COLUMN_ID,
                  t1.COLUMN_NAME,
                  replace(replace(t2.COMMENTS, ' ', ''), chr(13), '') COMMENTS,
                  t3.COMMENTS TAB_COMMENTS,
                  :SYS SYS_SRC,
                  t1.owner,
                  case
                    when t1.data_type = 'DATE' then
                     'varchar(50)'
                    when t1.data_type in ('NUMBER', 'NUMERIC') then
                     case
                       when data_precision is null then
                        'decimal(10)'
                       else
                        'decimal(' || to_char(data_precision) || ',' ||
                        to_char(data_scale) || ')'
                     end
                    when t1.data_type in ('VARCHAR2', 'CHAR', 'NVARCHAR2') then
                     'varchar(' || to_char(CASE
                                             WHEN data_length * 3 > 10000 THEN
                                              10000
                                             ELSE
                                              data_length * 3
                                           END) || ')'
                    when t1.data_type = 'CLOB' then
                     'varchar(4000)'
                    when t1.data_type like 'TIMESTAMP%' then
                     'DATETIME'
                  end col_type
    FROM SYS.ALL_TAB_COLUMNS T1
    LEFT JOIN SYS.ALL_COL_COMMENTS T2
      ON T1.TABLE_NAME = T2.TABLE_NAME
     AND T1.COLUMN_NAME = T2.COLUMN_NAME
     AND T1.OWNER = T2.OWNER
    LEFT JOIN SYS.ALL_TAB_COMMENTS T3
      ON T1.TABLE_NAME = T3.TABLE_NAME
     AND T1.OWNER = T3.OWNER
   WHERE T1.TABLE_NAME IN (:TABLE_NAME)
     and t1.owner = UPPER(:OWNER)
   ORDER BY T1.TABLE_NAME, T1.COLUMN_ID),
jg as
 (select table_name,
         chr(13) || 'drop table if exists STA.T_' || sys_src || '_' ||
         table_name || ';' dr_sta,
         chr(13) || 'drop table if exists ODS.' || sys_src || '_' ||
         table_name || ';' dr_ods,
         chr(13) || replace(('create table STA.T_' || sys_src || '_' ||
                            table_name || '(' || chr(13) || ' ' || xmlagg(xmlparse(content(column_name ||' ' || col_type ||' comment ''' || replace(comments, chr(13),'') ||'''' || chr(13)) ||',' wellformed) order by table_name, column_id asc)
                            .getclobval() || ') comment=''' ||
                            replace(tab_comments, chr(13), '') || ''';'),
                            ',)',
                            ')') sta_ddl,
         chr(13) ||
         replace(('create table ODS.' || sys_src || '_' || table_name || '(' ||
                 chr(13) || ' SDATE varchar(8) DEFAULT NULL COMMENT ''开始日期''' || chr(13) ||
                 ',EDATE varchar(8) DEFAULT NULL COMMENT ''结束日期''' || chr(13) || ',' || xmlagg(xmlparse(content(column_name ||' ' || col_type ||' comment ''' || replace(comments, chr(13),'') ||'''' || chr(13)) ||',' wellformed) order by table_name, column_id asc)
                 .getclobval() || 'LOAD_DATE VARCHAR(8) DEFAULT NULL COMMENT ''加载日期''' ||
                 chr(13) || ',SOURCE_DATA_TYPE VARCHAR(1) DEFAULT NULL COMMENT ''数据来源''' ||
                 chr(13) || ') comment=''' ||
                 replace(tab_comments, chr(13), '') || ''';'),
                 ',)',
                 '') ods_ddl
    from tabcol
   group by table_name, tab_comments, sys_src)
select -- table_name,
       -- dr_sta,
       -- dr_ods,
       STA_DDL,
       ODS_DDL
       -- to_char(sta_ddl),
       -- to_char(ods_ddl)
  from jg