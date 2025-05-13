CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_YSP_OTCNETTING(P_DATA_DT_STR IN VARCHAR2, --数据日期 yyyyMMdd
                                                   P_PO_RTNCODE  OUT VARCHAR2, --返回编号 1 成功,0 失败
                                                   P_PO_RTNMSG   OUT VARCHAR2 --返回描述
                                                   )
/*
  存储过程名称:RWA_DEV.PRO_RWA_YSP_OTCNETTING
  实现功能:财务系统-衍生品业务-场外衍生工具净额结算表
  数据口径:全量
  跑批频率:月初运行
  版  本  :V1.0.0
  编写人  :CHENGANG
  编写时间:2019-04-17
  单  位  :上海安硕信息技术股份有限公司
  源  表1 :RWA_DEV.BRD_SWAP|互换表
  源  表2 :RWA.ORG_INFO|机构信息表
  源  表3 :RWA_DEV.NCM_CUSTOMER_INFO|客户信息表
  源  表4 :RWA.CODE_LIBRARY|代码库表
  变更记录(修改人|修改时间|修改内容):
  
  */
 AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  V_PRO_NAME VARCHAR2(200) := 'RWA_DEV.RWA_YSP_OTCNETTING';
  --定义异常变量
  V_RAISE EXCEPTION;
  --定义当前插入的记录数
  V_COUNT INTEGER;

BEGIN
  --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

  /*如果是全量数据加载需清空目标表*/
  --1.清除目标表中的原有记录
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_YSP_OTCNETTING';
/*
  --2.将满足条件的数据从源表插入到目标表中
  INSERT INTO RWA_DEV.RWA_YSP_OTCNETTING
    (DATADATE, --数据日期
     DATANO, --数据流水号
     VALIDNETAGREEMENTID, --有效净额结算协议ID
     COUNTERPARTYID, --交易对手ID
     ORGSORTNO, --机构排序号
     ORGID, --所属机构ID
     ORGNAME, --所属机构名称
     INDUSTRYID, --所属行业代码
     INDUSTRYNAME, --所属行业名称
     BUSINESSLINE, --条线
     ASSETTYPE, --资产大类
     ASSETSUBTYPE, --资产小类
     BUSINESSTYPESTD, --权重法业务类型
     EXPOCLASSSTD, --权重法暴露大类
     EXPOSUBCLASSSTD, --权重法暴露小类
     EXPOCLASSIRB, --内评法暴露大类
     EXPOSUBCLASSIRB, --内评法暴露小类
     BOOKTYPE, --账户类别
     REPOTRANFLAG, --回购交易标识
     CLAIMSLEVEL, --债权级别
     ORIGINALMATURITY, --原始期限
     PRINCIPAL, --名义本金
     IRATING, --内部评级
     PD, --违约概率
     GROUPID --分组编号
     )
    SELECT TO_DATE(p_data_dt_str, 'YYYYMMDD'), --01数据日期
           p_data_dt_str, --02数据流水号
           'JEJS' || T1.DEALNO || T1.SEQ, --有效净额结算协议ID
           NVL(T3.TAXID, 'OPI' || T2.CNO), --交易对手ID
           '1290', --机构排序号
           '6001', --所属机构ID
           '重庆银行股份有限公司国际业务部', --所属机构名称
           'J6621', --所属行业代码
           '商业银行服务', --所属行业名称
           '0102', --业务条线
           '223', --资产大类
           '22301', --资产小类
           '01', --权重法业务类型
           '', --权重法暴露大类
           '', --权重法暴露小类
           '', --内评法暴露大类
           '', --内评法暴露小类
           CASE
             WHEN SUBSTR(T2.COST, 1, 4) = '3' THEN
              '01'
             ELSE
              '02'
           END, --账户类别
           '0', --回购交易标识
           '02', --债权级别
           CASE
             WHEN (TO_DATE(T2.MATDATE, 'YYYY-MM-DD') -
                  TO_DATE(T2.STARTDATE, 'YYYY-MM-DD')) / 365 < 0 THEN
              0
             ELSE
              (TO_DATE(T2.MATDATE, 'YYYY-MM-DD') -
              TO_DATE(T2.STARTDATE, 'YYYY-MM-DD')) / 365
           END, --原始期限
           T1.NOTCCYAMT, --名义本金
           '', --内部评级
           '', --违约概率
           '' --分组编号
      FROM RWA_DEV.OPI_SWDT T1 --互换交易
      LEFT JOIN RWA_DEV.OPI_SWDH T2 --互换报头 
        ON T1.DEALNO = T2.DEALNO
       AND T2.DATANO = p_data_dt_str
      LEFT JOIN RWA_DEV.OPI_CUST T3 --客户信息
        ON T2.CNO = T3.CNO
       AND T3.DATANO = p_data_dt_str
     WHERE T1.DATANO = p_data_dt_str;

  COMMIT;*/

  DBMS_STATS.GATHER_TABLE_STATS(OWNNAME => 'RWA_DEV',
                                TABNAME => 'RWA_YSP_OTCNETTING',
                                CASCADE => TRUE);

  /*目标表数据统计*/
  --统计插入的记录数
  SELECT COUNT(1) INTO V_COUNT FROM RWA_DEV.RWA_YSP_OTCNETTING;
  --Dbms_output.Put_line('RWA_DEV.RWA_TZ_CONTRACT表当前插入的财务系统-应收款投资数据记录为: ' || (v_count1 - v_count) || ' 条');
  --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

  P_PO_RTNCODE := '1';
  P_PO_RTNMSG  := '成功' || '-' || V_COUNT;
  --定义异常
EXCEPTION
  WHEN OTHERS THEN
    --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
    ROLLBACK;
    P_PO_RTNCODE := SQLCODE;
    P_PO_RTNMSG  := '合同信息(' || V_PRO_NAME || ')ETL转换失败！' || SQLERRM ||
                    ';错误行数为:' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
    RETURN;
END PRO_RWA_YSP_OTCNETTING;
/

