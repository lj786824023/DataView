CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_YSP_OTCCOUNTERPARTY(p_data_dt_str IN VARCHAR2, --数据日期 yyyyMMdd
                                                        p_po_rtncode  OUT VARCHAR2, --返回编号 1 成功,0 失败
                                                        p_po_rtnmsg   OUT VARCHAR2 --返回描述
                                                        )
/*
  存储过程名称:RWA_DEV.PRO_RWA_YSP_OTCCOUNTERPARTY
  实现功能:财务系统-衍生品业务-场外衍生工具交易对手表
  数据口径:全量
  跑批频率:月初运行
  版  本  :V1.0.0
  编写人  :CHENGANG
  编写时间:2019-04-18
  单  位  :上海安硕信息技术股份有限公司
  源  表1 :RWA_DEV.BRD_SWAP|互换表
  源  表2 :RWA.ORG_INFO|机构信息表
  源  表3 :RWA_DEV.NCM_CUSTOMER_INFO|客户信息表
  变更记录(修改人|修改时间|修改内容):
  chengang 2019/04/23 更新RWA_DEV.BRD_SWAP|互换表的机构号
  */
 AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_YSP_OTCCOUNTERPARTY';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

BEGIN
  --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

  /*如果是全量数据加载需清空目标表*/
  --1.清除目标表中的原有记录
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_YSP_OTCCOUNTERPARTY';
  
  --插入有效场外衍生工具业务下的交易对手
  INSERT INTO RWA_DEV.RWA_YSP_OTCCOUNTERPARTY
    (DATADATE,                    --数据日期
     DATANO,                      --数据流水号
     NETTINGFLAG,                 --净额结算标识
     COUNTERPARTYID,              --交易对手ID
     COUNTERPARTYNAME,            --交易对手名称
     ORGSORTNO,                   --机构排序号
     ORGID,                       --所属机构ID
     ORGNAME,                     --所属机构名称
     CPERATING                    --交易对手外部评级
     )
    SELECT DISTINCT
         TO_DATE(p_data_dt_str, 'YYYYMMDD') AS DATADATE --数据日期
        ,p_data_dt_str
        ,'0'
        ,T1.CLIENTID AS CUSTOMERID --客户编号
        ,T2.CFN1 AS CUSTOMERNAME --客户名称
        ,'1290'
        ,'6001'
        ,'重庆银行股份有限公司国际业务部'
        ,NVL(T3.CREDITLEVEL, '0207') AS CREDITLEVEL --评级信息 默认：0207 未评级
    FROM RWA_DEV.RWA_YSP_EXPOSURE T1 --衍生品暴露表
    LEFT JOIN RWA_DEV.OPI_CUST T2 
           ON T1.CLIENTID = 'OPI' || T2.CNO
          AND T2.DATANO = p_data_dt_str
    LEFT JOIN RWA.RWA_WS_KHPJ_BL T3 --交易对手外部评级补录表
      ON TRIM(T3.CUSTOMERID) = T1.CLIENTID
     AND T3.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
    WHERE T1.DATANO = p_data_dt_str
    ;
 
   COMMIT;

 
  dbms_stats.gather_table_stats(ownname => 'RWA_DEV',
                                tabname => 'RWA_YSP_OTCCOUNTERPARTY',
                                cascade => true);

  /*目标表数据统计*/
  --统计插入的记录数
  SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_YSP_OTCCOUNTERPARTY;

  --Dbms_output.Put_line('RWA_DEV.RWA_TZ_CONTRACT表当前插入的财务系统-应收款投资数据记录为: ' || (v_count1 - v_count) || ' 条');
  --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

  p_po_rtncode := '1';
  p_po_rtnmsg  := '成功' || '-' || v_count;
  --定义异常
EXCEPTION
  WHEN OTHERS THEN
    --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
    ROLLBACK;
    p_po_rtncode := sqlcode;
    p_po_rtnmsg  := '合同信息(' || v_pro_name || ')ETL转换失败！' || sqlerrm ||
                    ';错误行数为:' || dbms_utility.format_error_backtrace;
    RETURN;
END PRO_RWA_YSP_OTCCOUNTERPARTY;
/

