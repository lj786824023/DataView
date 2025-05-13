select T.TABLE_NAME,
       T.COMMENTS,
       T1.COLUMN_ID,
       T1.COLUMN_NAME,
       case
         when T1.DATA_TYPE like '%TIMESTAMP%' then
          T1.DATA_TYPE
         when T1.DATA_TYPE in ('CHAR', 'VARCHAR', 'NVARCHAR', 'VARCHAR2', 'NVARCHAR2') then
          T1.DATA_TYPE || '(' || T1.DATA_LENGTH || ')'
         when T1.DATA_TYPE in ('DATE', 'LONG', 'BLOB', 'CLOB') then
          T1.DATA_TYPE
         when T1.DATA_TYPE = 'NUMBER' and T1.DATA_PRECISION is null then
          T1.DATA_TYPE
         when T1.DATA_TYPE = 'NUMBER' and T1.DATA_PRECISION is not null then
          T1.DATA_TYPE || '(' || T1.DATA_PRECISION || ',' || T1.DATA_SCALE || ')'
         else
          T1.DATA_TYPE
       end as DATA_TYPE,
       T2.COMMENTS
  from ALL_TAB_COMMENTS T
  left join ALL_TAB_COLUMNS T1
    on T1.OWNER = :DATABASE_NAME
   and T1.TABLE_NAME = :TABLE_NAME
  left join ALL_COL_COMMENTS T2
    on T2.OWNER = :DATABASE_NAME
   and T2.TABLE_NAME = :TABLE_NAME
   and T1.COLUMN_NAME = T2.COLUMN_NAME
 where T.OWNER = :DATABASE_NAME
   and T.TABLE_NAME = :TABLE_NAME
 order by T1.COLUMN_ID