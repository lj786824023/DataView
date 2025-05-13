CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_XF_CONTRACT(
                            p_data_dt_str IN  VARCHAR2, --数据日期
                            p_po_rtncode  OUT VARCHAR2, --返回编号
                            p_po_rtnmsg   OUT VARCHAR2  --返回描述
)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_XF_CONTRACT
    实现功能:消费金融系统-合同信息(从数据源理财系统将合同相关信息全量导入RWA消费金融接口表合同信息表中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :xlpang
    编写时间:2019-05-28
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.RMPS_CQ_CONTRACT|合同信息
    源  表2 :RWA_DEV.RWA_CD_PAYTODW_ORG|统一机构参数表
    源  表3 :
    源  表4 :
    源  表5 :

    目标表1 :RWA_DEV.RWA_XF_CONTRACT|消费金融合同信息表
    辅助表  :无
    变更记录(修改人|修改时间|修改内容):
    */

  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_XF_CONTRACT';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count1 INTEGER;
  --v_count2 INTEGER;

  BEGIN

    --DBMS_OUTPUT.PUT_LINE('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --清除目标表中的原有记录
    --DELETE FROM RWA_DEV.RWA_XF_CONTRACT WHERE DATADATE=TO_DATE(p_data_dt_str,'yyyyMMdd');
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_XF_CONTRACT';


    --2.将满足条件的数据从源表插入到目标表中-已使用额度、未使用额度
    INSERT INTO RWA_DEV.RWA_XF_CONTRACT(
                DataDate                               --数据日期
                ,DataNo                                --数据流水号
                ,ContractID                            --合同ID
                ,SContractID                           --源合同ID
                ,SSysID                                --源系统ID
                ,ClientID                              --参与主体ID
                ,SOrgID                                --源机构ID
                ,SOrgName                              --源机构名称
                ,ORGSORTNO                             --所属机构排序号
                ,OrgID                                 --所属机构ID
                ,OrgName                               --所属机构名称
                ,IndustryID                            --所属行业代码
                ,IndustryName                          --所属行业名称
                ,BusinessLine                          --条线
                ,AssetType                             --资产大类
                ,AssetSubType                          --资产小类
                ,BusinessTypeID                        --业务品种代码
                ,BusinessTypeName                      --业务品种名称
                ,CreditRiskDataType                    --信用风险数据类型
                ,StartDate                             --起始日期
                ,DueDate                               --到期日期
                ,OriginalMaturity                      --原始期限
                ,ResidualM                             --剩余期限
                ,SettlementCurrency                    --结算币种
                ,ContractAmount                        --合同总金额
                ,NotExtractPart                        --合同未提取部分
                ,UncondCancelFlag                      --是否可随时无条件撤销
                ,ABSUAFlag                             --资产证券化基础资产标识
                ,ABSPoolID                             --证券化资产池ID
                ,GroupID                               --分组编号
                ,GUARANTEETYPE                         --主要担保方式
                ,ABSPROPORTION                         --资产证券化比重
    )
    SELECT          DISTINCT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DataDate                 --数据日期
                ,p_data_dt_str                               AS DataNo                   --数据流水号
                ,T1.CONTRACT_NBR                                AS ContractID               --合同ID
                ,T1.CONTRACT_NBR                                    AS SContractID              --源合同ID
                ,'XJ'                                       AS SSysID                   --源系统ID                 默认：信用卡(XYK)
                ,T2.CORE_CUST_ID                                 AS ClientID                --参与主体ID
                ,T2.CORE_ACCT_ORG                                  AS SOrgID                   --源机构ID
                ,T3.ORGNAME                              AS SOrgName                --源机构名称
                ,T2.CORE_ACCT_ORG                                   AS ORGSORTNO                --所属机构排序号
                ,T3.SORTNO                                  AS OrgID                    --所属机构ID
                ,T3.ORGNAME                               AS OrgName                 --所属机构名称
                ,''                                          AS IndustryID               --所属行业代码             默认：空
                ,''                                          AS IndustryName             --所属行业名称             默认：空
                ,'0301'                                        AS BusinessLine             --条线                     零售业务
                ,NULL                                       AS AssetType                --资产大类                 默认
                ,NULL                                     AS AssetSubType             --资产小类                 默认
                ,'11103038'                                  AS BusinessTypeID           --业务品种代码             信用卡业务
                ,'捷e贷'                                 AS BusinessTypeName         --业务品种名称             固定值：信用卡
                ,'02'                                        AS CreditRiskDataType       --信用风险数据类型         固定值：一般零售
                ,T2.PAY_FINISH_DATE                                AS StartDate                --起始日期
                ,T2.DD_EXPIR_DAY                                  AS DueDate                  --到期日期
                ,CASE
                 WHEN (TO_DATE(T2.DD_EXPIR_DAY, 'YYYYMMDD') - TO_DATE(T2.PAY_FINISH_DATE, 'YYYYMMDD')) / 365 < 0 THEN
                  0
                 ELSE
                    (TO_DATE(T2.DD_EXPIR_DAY, 'YYYYMMDD') - TO_DATE(T2.PAY_FINISH_DATE, 'YYYYMMDD')) / 365
                 END AS ORIGINALMATURITY -- 原始期限
                ,CASE
                   WHEN (TO_DATE(T2.DD_EXPIR_DAY, 'YYYYMMDD') - TO_DATE(T2.DATANO, 'YYYYMMDD')) / 365 < 0 THEN
                    0
                   ELSE
                    (TO_DATE(T2.DD_EXPIR_DAY, 'YYYYMMDD') - TO_DATE(T2.DATANO, 'YYYYMMDD')) / 365
                 END AS RESIDUALM -- 剩余期限
                ,'CNY'                                       AS SettlementCurrency       --结算币种                 默认：人民币
                ,T1.CONTR_PRIN                               AS ContractAmount           --合同总金额
                ,0                                           AS NotExtractPart              --合同未提取部分
                ,'0'                                         AS UncondCancelFlag            --是否可随时无条件撤销     默认：否
                ,'0'                                         AS ABSUAFlag                   --资产证券化基础资产标识   默认：否
                ,''                                          AS ABSPoolID                   --证券化资产池ID           默认：空
                ,''                                          AS GroupID                     --分组编号                 RWA系统赋值
                ,'005'                                       AS GUARANTEETYPE               --主要担保方式
                ,NULL                                        AS ABSPROPORTION               --资产证券化比重
    FROM RWA_DEV.RMPS_CQ_CONTRACT T1
    INNER JOIN RWA_DEV.RMPS_CQ_LOAN T2 
           ON T1.CONTRACT_NBR = T2.CONTRACT_NBR
          AND T1.DATANO = T2.DATANO
    LEFT JOIN RWA.ORG_INFO T3
        ON T2.CORE_ACCT_ORG = T3.ORGID
    WHERE T1.DATANO = p_data_dt_str 
      AND T2.PRIN_BAL <> 0 --有效借据下的合同
      AND T2.TERMIN_DATE IS NOT NULL; --借据终结日期不为空
   
    
    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_XF_CONTRACT',cascade => true);


    /*目标表数据统计*/
    --统计插入的记录
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_XF_CONTRACT;


    --DBMS_OUTPUT.PUT_LINE('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功' || '-' || v_count1;

    --定义异常
    EXCEPTION WHEN OTHERS THEN
        --DBMS_OUTPUT.PUT_LINE('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
        ROLLBACK;
        p_po_rtncode := sqlcode;
        p_po_rtnmsg  := '消费金融系统合同信息('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;

    RETURN;
END PRO_RWA_XF_CONTRACT;
/

