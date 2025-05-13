CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_OTCCOUNTERPARTY(
			 											P_DATA_DT_STR	IN	VARCHAR2,		--数据日期
       											P_PO_RTNCODE	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														P_PO_RTNMSG		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_EI_OTCCOUNTERPARTY
    实现功能:场外衍生工具交易对手表,插入场外衍生工具交易对手表
    数据口径:全量
    跑批频率:月初
    版  本  :V1.0.0
    编写人  :CHENGANG
    编写时间:2019-04-19
    单  位	:上海安硕信息技术股份有限公司
    源  表1	:RWA_DEV.RWA_ABS_INVEST_OTCCOUNTERPARTY|场外衍生工具交易对手表
    目标表  :RWA_DEV.RWA_EI_OTCCOUNTERPARTY|场外衍生工具交易对手表
    辅助表  :无
    变更记录(修改人|修改时间|修改内容)：
  	*/
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  V_PRO_NAME VARCHAR2(200) := 'RWA_DEV.PRO_RWA_EI_OTCCOUNTERPARTY';
  --定义判断值变量
  V_COUNT INTEGER;
  --定义异常变量
  V_RAISE EXCEPTION;

  BEGIN
    --DBMS_OUTPUT.PUT_LINE('【执行 ' || V_PRO_NAME || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    DELETE FROM RWA_DEV.RWA_EI_OTCCOUNTERPARTY WHERE DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYY/MM/DD');
    
    COMMIT;
    

    /*插入场外衍生工具交易对手表*/
    INSERT INTO RWA_DEV.RWA_EI_OTCCOUNTERPARTY(
              DATADATE,          --数据日期
              DATANO,          --数据流水号
              NETTINGFLAG,          --净额结算标识
              COUNTERPARTYID,          --交易对手ID
              COUNTERPARTYNAME,          --交易对手名称
              ORGSORTNO,          --机构排序号
              ORGID,          --所属机构ID
              ORGNAME,          --所属机构名称
              CPERATING          --交易对手外部评级
              )
          SELECT DATADATE         AS DATADATE, --数据日期
                 DATANO           AS DATANO, --数据流水号
                 NETTINGFLAG      AS NETTINGFLAG, --净额结算标识
                 COUNTERPARTYID   AS COUNTERPARTYID, --交易对手ID
                 COUNTERPARTYNAME AS COUNTERPARTYNAME, --交易对手名称
                 ORGSORTNO        AS ORGSORTNO, --机构排序号
                 ORGID            AS ORGID, --所属机构ID
                 ORGNAME          AS ORGNAME, --所属机构名称
                 CPERATING        AS CPERATING --交易对手外部评级
            FROM RWA_DEV.RWA_YSP_OTCCOUNTERPARTY
           WHERE DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD');
   
    COMMIT;


    /*目标表数据统计*/
    --统计插入的记录
    SELECT COUNT(1) INTO V_COUNT FROM RWA_DEV.RWA_EI_OTCCOUNTERPARTY WHERE DATANO = P_DATA_DT_STR;
    --DBMS_OUTPUT.PUT_LINE('RWA_DEV.RWA_EI_OTCCounterparty表当前插入的数据记录为:' || V_COUNT1 || '条');

    --DBMS_OUTPUT.PUT_LINE('【执行 ' || V_PRO_NAME || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    P_PO_RTNCODE := '1';
    P_PO_RTNMSG  := '成功'||'-'||V_COUNT;
    --定义异常
    EXCEPTION
    WHEN OTHERS THEN
         --DBMS_OUTPUT.PUT_LINE('出错了,错误代码为:'||SQLCODE||';错误信息为:'||SQLERRM||';错误行数为:'||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
          ROLLBACK;
          P_PO_RTNCODE := SQLCODE;
          P_PO_RTNMSG  := '资产证券化的合约与池信息表('|| v_pro_name ||')ETL转换失败！'|| SQLERRM||';错误行数为:'||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
         RETURN;
END PRO_RWA_EI_OTCCOUNTERPARTY;
/

