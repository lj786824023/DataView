CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ZJ_TRADBONDPOSITION(
                            p_data_dt_str IN  VARCHAR2,   --数据日期 yyyyMMdd
                            p_po_rtncode  OUT VARCHAR2,   --返回编号 1 成功,0 失败
                            p_po_rtnmsg   OUT VARCHAR2    --返回描述
        )
  /*
    存储过程名称:RWA_DEV.PRO_RWA_ZJ_TRADBONDPOSITION
    实现功能:市场风险-资金系统-交易债券头寸表
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :CHENGANG
    编写时间:2019-04-18
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.BRD_BOND|债劵信息表
    源  表2 :RWA_DEV.BRD_SECURITY_POSI|债券头寸信息表
     变更记录(修改人|修改时间|修改内容):
     pxl 2019/09/05  调整源逻辑
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ZJ_TRADBONDPOSITION';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;
  --v_count1 INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_ZJ_TRADBONDPOSITION';

    --2.将满足条件的数据从源表插入到目标表中
    INSERT INTO RWA_DEV.RWA_ZJ_TRADBONDPOSITION
      (DATADATE, --数据日期
       POSITIONID, --头寸ID
       BONDID, --债券ID
       TRANORGID, --交易机构ID
       ACCORGID, --账务机构ID
       INSTRUMENTSTYPE, --金融工具类型
       ACCSUBJECTS, --会计科目
       DENOMINATION, --面额
       MARKETVALUE, --市值
       DISCOUNTPREMIUM, --折溢价
       FAIRVALUECHANGE, --公允价值变动
       BOOKBALANCE, --账面余额
       CURRENCY --币种
       )
      SELECT TO_DATE(p_data_dt_str, 'YYYYMMDD'), --数据日期
             T1.ACCT_NO, --头寸ID                                                                                     
             T1.ACCT_NO, --债券ID
             T1.MAG_ORG_CD,  --交易机构ID
             T1.MAG_ORG_CD,  --账务机构ID
             '0101',     --金融工具类型  默认债券
             T1.SBJT_CD, --会计科目
             T1.PAR_VALUE, --面额
             NVL(T2.CLEAN_PRICE, 0), --市值  取的债券净价
             0,   --折溢价
             T1.FAIR_EXCH_VAL,  --公允价值变动
             NVL(T1.POSITION_INITIAL_VALUE, 0) +
             NVL(T1.FAIR_EXCH_VAL, 0), --NVL(T1.ACCRUAL, 0), --账面余额 = 成本科目记账金额  + 公允价值变动金额            
             DECODE(T2.CCY_CD,'156','CNY',T2.CCY_CD) --币种
        FROM BRD_SECURITY_POSI T1 --债券头寸信息
       INNER JOIN BRD_BOND T2 --债券
         ON T1.SECURITY_REFERENCE = T2.BOND_ID
        AND T2.DATANO = p_data_dt_str
        AND T2.BELONG_GROUP = '4' --资金系统                       
      WHERE T1.DATANO = p_data_dt_str
        AND T1.SBJT_CD = '11010101'  --以公允价值计量且其变动计入当期损益的金融资产         
        AND T2.BOND_TYPE NOT IN ('TTC')   --排除非国债  TTC 二级资本工具
    ;
    
    COMMIT;
    
    --- 衍生品交易头寸
     /* INSERT INTO RWA_DEV.RWA_ZJ_TRADBONDPOSITION
      (DATADATE, --数据日期
       POSITIONID, --头寸ID
       BONDID, --债券ID
       TRANORGID, --交易机构ID
       ACCORGID, --账务机构ID
       INSTRUMENTSTYPE, --金融工具类型
       ACCSUBJECTS, --会计科目
       DENOMINATION, --面额
       MARKETVALUE, --市值
       DISCOUNTPREMIUM, --折溢价
       FAIRVALUECHANGE, --公允价值变动
       BOOKBALANCE, --账面余额
       CURRENCY --币种
       )
      SELECT TO_DATE(p_data_dt_str, 'YYYYMMDD'), --数据日期
             T.DEALNO || T.SEQ, --头寸ID                                                                                     
             T.DEALNO || T.SEQ, --债券ID
             '6001', --交易机构ID
             '6001', --账务机构ID
             '0201', --金融工具类型  默认债券
             '70120000', --会计科目
             T.NOTCCYAMT, --面额
             ABS(T.NPVBAMT), --市值  取的债券净价
             0, --折溢价
             0, --公允价值变动
             ABS(T.NPVBAMT), --账面余额    
             T.INTCCY --币种
        FROM OPI_SWDT T --互换交易
        LEFT JOIN OPI_SWDH H --互换报头 
          ON T.DEALNO = H.DEALNO
         AND H.DATANO = p_data_dt_str
       WHERE T.DATANO = p_data_dt_str
    ;
    
    COMMIT;
    */

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_ZJ_TRADBONDPOSITION',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_ZJ_TRADBONDPOSITION;
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
         p_po_rtnmsg  := '交易债券头寸表('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_ZJ_TRADBONDPOSITION;
/

