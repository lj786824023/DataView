CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_TRUNCTAB_PARTITION(p_data_dt_str in VARCHAR2,
                                              p_po_rtncode  OUT VARCHAR2, --返回编号 1 成功,0 失败
                                             p_po_rtnmsg   OUT VARCHAR2 --返回描述
                                             ) IS
  /***************************************************************************
  create by chengang on 2020/6/15
  清理大表分区（保留最近两年每个季度的数据及最近两个月的数据）
  ***************************************************************************/
  --表名
  V_TAB VARCHAR2(100) := '';
  --分区名
  V_PAR VARCHAR2(100) := '';
  --所属用户
  V_OWER VARCHAR2(100) := '';
  --当前批次日期
  V_DATE VARCHAR2(100) := '';
  ---月数
  V_MON VARCHAR2(100) := '';
  ---月份
  V_MONTH VARCHAR2(20) := '';
  --真假判断
  V_TRUE VARCHAR2(20) := '';
  --分区名称
  V_PARN VARCHAR2(100) := '';
  --表名称
  V_RWATAB VARCHAR2(100) := '';
  --SQL代码
  V_SQL VARCHAR2(2000) := '';
  --定义游标
  CURSOR CUR IS
    SELECT T.TABLE_OWNER    TABLE_OWNER,
           T.TABLE_NAME     TABLE_NAME,
           T.PARTITION_NAME PARTITION_NAME
      FROM ALL_TAB_PARTITIONS T
     WHERE T.TABLE_NAME IN ( 'RWA_EI_CLIENT',
                             'RWA_EI_EXPOSURE',
                             'RWA_EI_CONTRACT')
     ORDER BY T.TABLE_OWNER, T.TABLE_NAME, T.PARTITION_NAME;

  ATP CUR%ROWTYPE;
BEGIN

  V_DATE := p_data_dt_str;
   ---数据库用户为RWA_DEV 
  OPEN CUR;

  LOOP
    FETCH CUR
      INTO ATP;
  
    V_OWER := ATP.TABLE_OWNER;
    V_TAB  := ATP.TABLE_NAME;
    V_PAR  := ATP.PARTITION_NAME;
  
    --表名
    V_RWATAB := V_OWER || '.' || V_TAB;
    V_PAR    := substr(V_PAR, -8);
    select ceil((to_date(V_DATE, 'yyyymmdd') - to_date(V_PAR, 'yyyymmdd')) / 30)
      into V_MON
      from dual;
  
    IF V_MON > 24 THEN
      V_TRUE := '1';
    ELSIF V_MON <= 3 THEN
      V_TRUE := '0';
    ELSIF V_MON > 3 and V_MON <= 24 THEN
      V_MONTH := substr(V_PAR, 5, 2);
      DBMS_OUTPUT.PUT_LINE(V_MONTH);
      IF V_MONTH in ('03', '06', '09', '12') THEN
        V_TRUE := '0';
      ELSE
        V_TRUE := '1';
      END IF;
    END IF;
  
    IF V_TRUE = '1' THEN
      V_PARN := ATP.PARTITION_NAME;
      V_SQL  := 'alter table ' || V_RWATAB || ' drop partition ' || V_PARN;
      /*      insert into trunc_sql (vsql) values (V_SQL);
      commit;*/
      EXECUTE IMMEDIATE V_SQL;
    END IF;
  
    EXIT WHEN CUR%NOTFOUND;
  
    --执行TRUNCATE TABLE
  
  END LOOP;
  CLOSE CUR;
  
  ---数据库用户为RWA
  RWA.TRUNCRWA_TAB_PARTITION(V_DATE);
      p_po_rtncode := '1';
    p_po_rtnmsg  := '成功';
    --定义异常
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '清理表分区ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_TRUNCTAB_PARTITION;
/

