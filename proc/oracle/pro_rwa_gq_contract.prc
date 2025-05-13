CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_GQ_CONTRACT(
                                                P_DATA_DT_STR IN    VARCHAR2,      --数据日期
                                                P_PO_RTNCODE  OUT   VARCHAR2,      --返回编号
                                                P_PO_RTNMSG   OUT   VARCHAR2       --返回描述
                                               )
  /*
    存储过程名称:RWA_DEV.PRO_RWA_GQ_CONTRACT
    实现功能:股权投资-合同,表结构为合同表
    数据口径:全量
    跑批频率:月末
    版  本  :V1.0.0
    编写人  :TANGLW
    编写时间:2016-01-07
    单  位  :上海安硕信息技术股份有限公司
    源 表1  :RWA.RWA_EI_UNCONSFIINVEST                       |股权投资-信用风险暴露表
    源 表2  :RWA.ORG_INFO                                    |机构表
    辅助表  :无
    变更记录(修改人|修改时间|修改内容)：
  */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  P_PRO_NAME VARCHAR2(200) := 'RWA_DEV.PRO_RWA_GQ_CONTRACT';
  --定义判断值变量
  P_COUNT INTEGER;
  --定义异常变量
  P_RAISE EXCEPTION;

  BEGIN
    --DBMS_OUTPUT.PUT_LINE('【执行 ' || P_PRO_NAME || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS'));

    --1.清除目标表中的原有记录
    /*如果是全量数据加载需清空目标表*/
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_GQ_CONTRACT';

    --2.将满足条件的数据从源表插入到目标表中
    /*插入目标表*/
    INSERT INTO RWA_GQ_CONTRACT(
               DataDate                             --数据日期
              ,DataNo                               --数据流水号
              ,ContractID                           --合同ID
              ,SContractID                          --源合同ID
              ,SSysID                               --源系统ID
              ,ClientID                             --参与主体ID
              ,SOrgID                               --源机构ID
              ,SOrgName                             --源机构名称
              ,OrgSortNo                            --所属机构排序号
              ,OrgID                                --所属机构ID
              ,OrgName                              --所属机构名称
              ,IndustryID                           --所属行业代码
              ,IndustryName                         --所属行业名称
              ,BusinessLine                         --业务条线
              ,AssetType                            --资产大类
              ,AssetSubType                         --资产小类
              ,BusinessTypeID                       --业务品种代码
              ,BusinessTypeName                     --业务品种名称
              ,CreditRiskDataType                   --信用风险数据类型
              ,StartDate                            --起始日期
              ,DueDate                              --到期日期
              ,OriginalMaturity                     --原始期限
              ,ResidualM                            --剩余期限
              ,SettlementCurrency                   --结算币种
              ,ContractAmount                       --合同总金额
              ,NotExtractPart                       --合同未提取部分
              ,UncondCancelFlag                     --是否可随时无条件撤销
              ,ABSUAFlag                            --资产证券化基础资产标识
              ,ABSPoolID                            --证券化资产池ID
              ,GroupID                              --分组编号
              ,GUARANTEETYPE                        --主要担保方式
              ,ABSPROPORTION                        --资产证券化比重
      )
      SELECT
              TO_DATE(P_DATA_DT_STR,'YYYYMMDD')                      AS DATADATE            --数据日期
              ,P_DATA_DT_STR                                         AS DATANO              --数据流水号
              ,'GQ'||T1.SERIALNO                                     AS CONTRACTID          --合同代号
              ,''                                                    AS SCONTRACTID         --源合同代号
              ,'GQ'                                                  AS SSYSID              --源系统代号
              ,T1.CUSTID1                                            AS CLIENTID            --参与主体代号
              ,T1.ORGID                                              AS SORGID              --源机构ID
              ,T2.ORGNAME                                            AS SORGNAME            --源机构名称
              ,T2.SORTNO                                             AS ORGSORTNO           --所属机构排序号
              ,T1.ORGID                                              AS ORGID               --所属机构ID
              ,T2.ORGNAME                                            AS ORGNAME             --所属机构名称
              ,''                                                    AS INDUSTRYID          --所属行业代码
              ,''                                                    AS INDUSTRYNAME        --所属行业名称
              ,T1.BUSINESSLINE                                       AS BUSINESSLINE        --条线
              ,''                                                    AS ASSETTYPE           --资产大类
              ,''                                                    AS ASSETSUBTYPE        --资产小类
              ,'109060'                                              AS BUSINESSTYPEID      --业务品种代码              (固定值'998')
              ,'股权投资'                                            AS BUSINESSTYPENAME    --业务品种名称              (固定值'股权投资')
              ,'01'                                                  AS CREDITRISKDATATYPE  --信用风险数据类型          (01 一般非零售,02 一般零售,03 交易对手)
              ,TO_CHAR(TO_DATE(P_DATA_DT_STR,'YYYY-MM-DD'),'YYYY-MM-DD')
                                                                     AS STARTDATE           --起始日期
              ,TO_CHAR(ADD_MONTHS(TO_DATE(P_DATA_DT_STR,'YYYY-MM-DD'),1),'YYYY-MM-DD')
                                                                     AS DUEDATE             --到期日期
              ,(ADD_MONTHS(TO_DATE(P_DATA_DT_STR,'YYYYMMDD'),1) - TO_DATE(P_DATA_DT_STR,'YYYYMMDD'))/365
                                                                     AS ORIGINALMATURITY    --原始期限
              ,(ADD_MONTHS(TO_DATE(P_DATA_DT_STR,'YYYYMMDD'),1) - TO_DATE(P_DATA_DT_STR,'YYYYMMDD'))/365
                                                                     AS RESIDUALM           --剩余期限
              ,T1.CURRENCY                                           AS SETTLEMENTCURRENCY  --结算币种
              ,CASE WHEN SUBSTR(T1.EQUITYINVESTTYPE,1,2)='02' THEN T1.EQUITYINVESTAMOUNT
              ELSE CTOCINVESTAMOUNT END                              AS CONTRACTAMOUNT      --合同总金额               (股权投资金额)
              ,0                                                     AS NOTEXTRACTPART      --合同未提取部分           (默认为0)
              ,'0'                                                   AS UNCONDCANCELFLAG    --是否可随时无条件撤销     (默认为否,1是0否)
              ,'0'                                                   AS ABSUAFLAG           --资产证券化基础资产标识   (默认为否,1是0否)
              ,''                                                    AS ABSPOOLID           --证券化资产池ID
              ,''                                                    AS GROUPID             --分组编号                 (默认为空)
              ,''                                                    AS GUARANTEETYPE       --主要担保方式
              ,NULL                                                  AS ABSPROPORTION       --资产证券化比重
    FROM      RWA.RWA_EI_UNCONSFIINVEST T1                  --长期股权投资补录表
    LEFT JOIN RWA.ORG_INFO T2
    ON        T2.ORGID = T1.ORGID
    WHERE     T1.DATADATE = TO_DATE(p_data_dt_str,'YYYY-MM-DD')
    AND       T1.CONSOLIDATEFLAG = '0'
    AND       T1.EQUITYINVESTTYPE LIKE '03%'
    ;
    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_GQ_CONTRACT',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录
    SELECT COUNT(1) INTO P_COUNT FROM RWA_DEV.RWA_GQ_CONTRACT;
    DBMS_OUTPUT.PUT_LINE('RWA_XN_CONTRACT表当前插入的数据记录为:' || P_COUNT || '条');
    DBMS_OUTPUT.PUT_LINE('【执行 ' || P_PRO_NAME || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS'));

    P_PO_RTNCODE := '1';
    P_PO_RTNMSG  := '成功-'||P_COUNT;
    --定义异常
    EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('出错了,错误代码为:'||SQLCODE||';错误信息为:'||SQLERRM||';错误行数为:'||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
        ROLLBACK;
        P_PO_RTNCODE := SQLCODE;
        P_PO_RTNMSG  := '股权投资-合同(PRO_RWA_XN_CONTRACT)ETL转换失败！'|| SQLERRM;
    RETURN;

END PRO_RWA_GQ_CONTRACT;
/

