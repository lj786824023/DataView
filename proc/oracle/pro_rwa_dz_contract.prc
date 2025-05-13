CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_DZ_CONTRACT(
                             p_data_dt_str  IN  VARCHAR2,    --数据日期
                             p_po_rtncode  OUT  VARCHAR2,    --返回编号 1 成功,0 失败
                             p_po_rtnmsg    OUT  VARCHAR2    --返回描述
        )
  /*
    存储过程名称:RWA_DEV.PRO_RWA_DZ_CONTRACT
    实现功能:抵债资产补录合同信息
    数据口径:全量
    跑批频率:月初
    版  本  :V1.0.0
    编写人  :YUSJ
    编写时间:2016-10-09
    单  位  :上海安硕信息技术股份有限公司
    源  表1  :NCM_ASSET_DEBT_INFO |抵债资产信息表
    目标表  :RWA_DEV.RWA_DZ_CONTRACT|信贷系统合同表
    辅助表  :无
    变更记录(修改人|修改时间|修改内容)：
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_DZ_CONTRACT';
  --定义判断值变量
  v_count1 INTEGER;
  --定义异常变量
  v_raise EXCEPTION;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_DZ_CONTRACT';
/*
    \*插入有效借据下的合同信息(信贷系统都以借据为准)*\
    INSERT INTO RWA_DEV.RWA_DZ_CONTRACT(
               DATADATE                              --数据日期
              ,DATANO                                --数据流水号
              ,CONTRACTID                            --合同ID
              ,SCONTRACTID                           --源合同ID
              ,SSYSID                                --源系统ID
              ,CLIENTID                              --参与主体ID
              ,SORGID                                --源机构ID
              ,SORGNAME                              --源机构名称
              ,ORGSORTNO                             --所属机构排序号
              ,ORGID                                 --所属机构ID
              ,ORGNAME                               --所属机构名称
              ,INDUSTRYID                            --所属行业代码
              ,INDUSTRYNAME                          --所属行业名称
              ,BUSINESSLINE                          --条线
              ,ASSETTYPE                             --资产大类
              ,ASSETSUBTYPE                          --资产小类
              ,BUSINESSTYPEID                        --业务品种代码
              ,BUSINESSTYPENAME                      --业务品种名称
              ,CREDITRISKDATATYPE                    --信用风险数据类型
              ,STARTDATE                             --起始日期
              ,DUEDATE                               --到期日期
              ,ORIGINALMATURITY                      --原始期限
              ,RESIDUALM                             --剩余期限
              ,SETTLEMENTCURRENCY                    --结算币种
              ,CONTRACTAMOUNT                        --合同总金额
              ,NOTEXTRACTPART                        --合同未提取部分
              ,UNCONDCANCELFLAG                      --是否可随时无条件撤销
							,ABSUAFLAG         										 --资产证券化基础资产标识
							,ABSPOOLID         										 --证券化资产池ID
							,GROUPID           										 --分组编号
							,GUARANTEETYPE												 --主要担保方式
							,ABSPROPORTION                         --资产证券化比重
    )
     SELECT
                 TO_DATE(T1.DATANO,'YYYYMMDD')                                     AS DATADATE            --数据日期
                ,T1.DATANO                                                         AS DATANO              --数据流水号
                ,'DZ-' || T1.GUARANTYID                                            AS CONTRACTID          --合同ID
                ,T1.GUARANTYID                                                     AS SCONTRACTID         --源合同ID
                ,'DZ'                                                              AS SSYSID              --源系统ID
                ,'DZ-' || T1.GUARANTYID                                            AS CLIENTID            --参与主体ID
                ,'01050000'	                                                       AS SORGID              --源机构ID
                ,'总行资产负债管理部'                                              AS SORGNAME            --源机构名称
                ,'1010050'                                                         AS ORGSORTNO           --所属机构排序号
                ,'01050000'	                                                   		 AS ORGID               --所属机构ID
                ,'总行资产负债管理部'                                              AS ORGNAME             --所属机构名称
                ,'999999'                                                          AS INDUSTRYID          --所属行业代码
                ,'未知'                                                            AS INDUSTRYNAME        --所属行业名称
                ,CASE WHEN T4.TYPEDIVISION='1' THEN '0501'
                			ELSE '0401'
                 END	                                                             AS BUSINESSLINE        --条线
                ,'129'                                                             AS ASSETTYPE           --资产大类
                ,'12901'                                                           AS ASSETSUBTYPE        --资产小类
                ,CASE WHEN T4.TYPEDIVISION='1' THEN '109040'
                      ELSE '109050'
                 END                                                               AS BUSINESSTYPEID      --业务品种代码
                ,CASE WHEN T4.TYPEDIVISION='1' THEN '抵债资产不动产类'
                      ELSE '抵债资产非不动产类'
                 END                                                               AS BUSINESSTYPENAME    --业务品种名称
                ,'01'                                                              AS CREDITRISKDATATYPE  --信用风险数据类型
                ,T1.ACQUIREDATE                                                    AS STARTDATE           --起始日期
                ,T1.DATANO                                 	                       AS DUEDATE             --到期日期
                ,CASE WHEN (TO_DATE(T1.DATANO,'YYYYMMDD')-TO_DATE(REPLACE(T1.ACQUIREDATE,'/',''),'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T1.DATANO,'YYYYMMDD')-TO_DATE(REPLACE(T1.ACQUIREDATE,'/',''),'YYYYMMDD'))/365
                 END                                                               AS OriginalMaturity    --原始期限
                ,CASE WHEN (TO_DATE(T1.DATANO,'YYYYMMDD')-TO_DATE(REPLACE(T1.ACQUIREDATE,'/',''),'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T1.DATANO,'YYYYMMDD')-TO_DATE(REPLACE(T1.ACQUIREDATE,'/',''),'YYYYMMDD'))/365
                 END                                                               AS ResidualM           --剩余期限
                ,'CNY'                                                             AS SETTLEMENTCURRENCY  --结算币种
                ,T1.ENTRYVALUE                                                     AS CONTRACTAMOUNT      --合同总金额
                ,0                                                                 AS NOTEXTRACTPART      --合同未提取部分
                ,'0'                                                               AS UNCONDCANCELFLAG    --是否可随时无条件撤销    0:否，1：是
                ,'0'                                                               AS ABSUAFLAG           --资产证券化基础资产标识
                ,NULL                                                              AS ABSPOOLID           --证券化资产池ID
                ,''                                                                AS GROUPID             --分组编号
                ,''                                                                AS GUARANTEETYPE       --主要担保方式
                ,NULL                                                              AS ABSPROPORTION       --资产证券化比重
    FROM 				RWA_DEV.NCM_ASSET_DEBT_INFO T1
    LEFT JOIN 	RWA.ORG_INFO T2
    ON 					T1.MANAGEORGID=T2.ORGID
    LEFT JOIN   RWA_DEV.NCM_COL_PARAM T4
    ON          T1.GUARANTYTYPEID=T4.GUARANTYTYPE
    AND         T4.DATANO=P_DATA_DT_STR
    WHERE  			T1.DATANO=P_DATA_DT_STR
    ;
    COMMIT;*/
    
  ---插入抵债资产合同信息  
 /*    INSERT INTO RWA_DEV.RWA_DZ_CONTRACT(
               DATADATE                              --数据日期
              ,DATANO                                --数据流水号
              ,CONTRACTID                            --合同ID
              ,SCONTRACTID                           --源合同ID
              ,SSYSID                                --源系统ID
              ,CLIENTID                              --参与主体ID
              ,SORGID                                --源机构ID
              ,SORGNAME                              --源机构名称
              ,ORGSORTNO                             --所属机构排序号
              ,ORGID                                 --所属机构ID
              ,ORGNAME                               --所属机构名称
              ,INDUSTRYID                            --所属行业代码
              ,INDUSTRYNAME                          --所属行业名称
              ,BUSINESSLINE                          --条线
              ,ASSETTYPE                             --资产大类
              ,ASSETSUBTYPE                          --资产小类
              ,BUSINESSTYPEID                        --业务品种代码
              ,BUSINESSTYPENAME                      --业务品种名称
              ,CREDITRISKDATATYPE                    --信用风险数据类型
              ,STARTDATE                             --起始日期
              ,DUEDATE                               --到期日期
              ,ORIGINALMATURITY                      --原始期限
              ,RESIDUALM                             --剩余期限
              ,SETTLEMENTCURRENCY                    --结算币种
              ,CONTRACTAMOUNT                        --合同总金额
              ,NOTEXTRACTPART                        --合同未提取部分
              ,UNCONDCANCELFLAG                      --是否可随时无条件撤销
              ,ABSUAFLAG                             --资产证券化基础资产标识
              ,ABSPOOLID                             --证券化资产池ID
              ,GROUPID                               --分组编号
              ,GUARANTEETYPE                         --主要担保方式
              ,ABSPROPORTION                         --资产证券化比重
    )
     SELECT
                 TO_DATE(T1.DATANO,'YYYYMMDD')                                     AS DATADATE            --数据日期
                ,T1.DATANO                                                         AS DATANO              --数据流水号
                ,T1.CONTRACTID                                         AS CONTRACTID          --合同ID
                ,T1.CONTRACTID                                                    AS SCONTRACTID         --源合同ID
                ,T1.SSYSID                                                             AS SSYSID              --源系统ID
                ,T1.CLIENTID                                            AS CLIENTID            --参与主体ID
                ,T1.SORGID                                                       AS SORGID              --源机构ID
                ,T1.SORGNAME                                              AS SORGNAME            --源机构名称
                ,T1.ORGSORTNO                                                         AS ORGSORTNO           --所属机构排序号
                ,T1.ORGID                                                        AS ORGID               --所属机构ID
                ,T1.ORGNAME                                             AS ORGNAME             --所属机构名称
                ,T1.INDUSTRYID                                                          AS INDUSTRYID          --所属行业代码
                ,T1.INDUSTRYNAME                                                         AS INDUSTRYNAME        --所属行业名称
                ,T1.BUSINESSLINE                                                               AS BUSINESSLINE        --条线
                ,T1.ASSETTYPE                                                             AS ASSETTYPE           --资产大类
                ,T1.ASSETSUBTYPE                                                         AS ASSETSUBTYPE        --资产小类
                ,T1.BUSINESSTYPEID                                                              AS BUSINESSTYPEID      --业务品种代码
                ,T1.BUSINESSTYPENAME                                                              AS BUSINESSTYPENAME    --业务品种名称
                ,'01'                                                              AS CREDITRISKDATATYPE  --信用风险数据类型
                ,T1.STARTDATE                                                    AS STARTDATE           --起始日期
                ,T1.DUEDATE                                                         AS DUEDATE             --到期日期
                ,T1.ORIGINALMATURITY                                                              AS OriginalMaturity    --原始期限
                ,T1.RESIDUALM                                                              AS ResidualM           --剩余期限
                ,'CNY'                                                             AS SETTLEMENTCURRENCY  --结算币种
                ,T1.NORMALPRINCIPAL                                                     AS CONTRACTAMOUNT      --合同总金额
                ,0                                                                 AS NOTEXTRACTPART      --合同未提取部分
                ,'0'                                                               AS UNCONDCANCELFLAG    --是否可随时无条件撤销    0:否，1：是
                ,'0'                                                               AS ABSUAFLAG           --资产证券化基础资产标识
                ,NULL                                                              AS ABSPOOLID           --证券化资产池ID
                ,''                                                                AS GROUPID             --分组编号
                ,''                                                                AS GUARANTEETYPE       --主要担保方式
                ,NULL                                                              AS ABSPROPORTION       --资产证券化比重
    FROM        RWA_DZ_EXPOSURE T1
    WHERE       T1.DATANO=P_DATA_DT_STR
    ;
    COMMIT;*/

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_DZ_CONTRACT',cascade => true);

     --统计插入的记录
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_DZ_CONTRACT;

    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功'||'-'||v_count1;
    --定义异常
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '抵债资产补录信息-合同(RWA_DEV.RWA_DZ_CONTRACT)ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace ;
         RETURN;
END PRO_RWA_DZ_CONTRACT;
/

