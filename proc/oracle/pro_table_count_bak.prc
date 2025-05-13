create or replace procedure rwa_dev.pro_table_count_bak(p_data_dt in varchar2) is
  v_sqla varchar2(4000);
  v_sqlb varchar2(4000);
  v_sql  varchar2(4000);

  --表名
  v_tab varchar2(200);
  --列名
  v_col varchar2(200);
  --列名排序id
  v_id number;

  --定义游标
  cursor cur is
    SELECT A.table_name, A.COLUMN_NAME, A.COLUMN_ID
      FROM USER_TAB_COLS A
     WHERE A.table_name LIKE 'BRD_UN_BOND'
     ORDER BY A.table_name, A.COLUMN_ID;

begin

  delete from table_count_info where datano = p_data_dt;
  commit;

  open cur;
  loop
    fetch cur
      into v_tab, v_col, v_id;
    exit when cur%notfound;
    --select 'BRD_CUST_INFO','CUST_ID','1',COUNT(1) AS TABLE_COUNT FROM BRD_CUST_INFO WHERE DATA_NO='20190331'
    v_sqla := 'select ''' || v_tab || ''',''' || v_col || ''',''' || v_id ||
              ''',' || 'count(1) as TABLE_COUNT from ' || v_tab ||
              ' where data_no=''' || p_data_dt || '''';
    --select 'BRD_CUST_INFO','CUST_ID',COUNT(1) AS COL_NULL_COUN FROM BRD_CUST_INFO WHERE DATA_NO='20190331' AND (TRIM(CUST_ID)='' OR CUST_ID IS NULL)
    v_sqlb := 'select ''' || v_tab || ''',''' || v_col || ''',' ||
              'count(1) as COL_NULL_COUN from ' || v_tab ||
              ' where data_no=''' || p_data_dt || ''' and (trim(' || v_col ||
              ')='''' or ' || v_col || 'is null)';
    --select a.*,b.null_col_count,b.null_col_count/a.all_count*100,'20190331'
    v_sql := q'[insert into table_count_info select a.*,b.null_col_count,decode(a.all_count,0,0,b.null_col_count/a.all_count*100),']' ||
             p_data_dt || q'[' from (]' || v_sqla || q'[) a inner join (]' ||
             v_sqlb || q'[) b on a.v_tab=b.v_tab and a.v_col=b.v_col]';
    --DBMS_OUTPUT.ENABLE(buffer_size => null);  --表示输出buffer不受限制
    --dbms_output.put_line(v_sql);
    execute immediate v_sql;
    commit;
  end loop;

  close cur;

end pro_table_count_bak;
/

