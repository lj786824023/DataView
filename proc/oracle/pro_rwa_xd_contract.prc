CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_XD_CONTRACT(
                             p_data_dt_str  IN  VARCHAR2,    --数据日期
                             p_po_rtncode  OUT  VARCHAR2,    --返回编号 1 成功,0 失败
                             p_po_rtnmsg    OUT  VARCHAR2    --返回描述
        )
  /*
    存储过程名称:PRO_RWA_XD_CONTRACT
    实现功能:信贷系统合同表,插入合同有关信息
    数据口径:全量
    跑批频率:月初
    版  本  :V1.0.0
    编写人  :YUSJ
    编写时间:2016-04-05
    单  位  :上海安硕信息技术股份有限公司
    源  表1  :NCM_BUSINESS_CONTRACT|授信业务合同表
    源  表2  :NCM_BUSINESS_TYPE|业务品种信息表
    源  表3  :RWA.ORG_INFO|机构信息表
    源  表5  :RWA.CODE_LIBIARY|代码库
    源  表6  :NCM_BUSINESS_DUEBILL|授信业务借据信息表
    目标表  :RWA_XD_CONTRACT|信贷系统合同表
    辅助表  :无
    变更记录(修改人|修改时间|修改内容)：
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_XD_CONTRACT';
  --定义判断值变量
  v_count1 INTEGER;
  --定义异常变量
  v_raise EXCEPTION;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_XD_CONTRACT';

    /*插入有效借据下的合同信息(插入有效借据下的合同信息)*/
    INSERT INTO RWA_DEV.RWA_XD_CONTRACT(
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
    )WITH TEMP_CONTRACT AS(SELECT DISTINCT T1.CONTRACTID AS CONTRACTNO
                           FROM RWA_DEV.RWA_XD_EXPOSURE T1
                           WHERE accsubject1 <> '70230101' ---20191014 by YSJ  排除资产证券化
                           AND ACCSUBJECT1 NOT LIKE '@%' --20191123 BY YSJ   排除
                           )
     SELECT
                TO_DATE(T1.DATANO,'YYYYMMDD')                                      AS DATADATE            --数据日期
                ,T1.DATANO                                                         AS DATANO              --数据流水号
                ,T1.SERIALNO                                                       AS CONTRACTID          --合同ID
                ,T1.SERIALNO                                                       AS SCONTRACTID         --源合同ID
                ,'XD'                                                              AS SSYSID              --源系统ID
                ,CASE WHEN REPLACE(T1.CUSTOMERID,'NCM_','') IS NULL THEN DECODE(T1.LINETYPE,'0030','XN-GRKH','XN-YBGS') 								--若客户ID为空，条线为个人就虚拟为个人客户，否则为一般公司
                			ELSE T1.CUSTOMERID
                 END                                                     AS CLIENTID            --参与主体ID
                ,NVL(T1.OPERATEORGID,'19999999')                                   AS SORGID              --源机构ID
                ,NVL(T3.ORGNAME,'未知')                                            AS SORGNAME            --源机构名称
                ,NVL(T3.SORTNO,'19999999')                                         AS ORGSORTNO           --所属机构排序
                ,decode(substr(T1.OPERATEORGID,1,1),'@','01000000',T1.OPERATEORGID)                                   AS ORGID               --所属机构ID
                ,NVL(T3.ORGNAME,'总行')                                            AS ORGNAME             --所属机构名称
                ,NVL(T1.DIRECTION,T8.DIRECTION)                                    AS INDUSTRYID          --所属行业代码
                ,NVL(T5.ITEMNAME,T9.ITEMNAME)                                      AS INDUSTRYNAME        --所属行业名称
                ,CASE WHEN T1.BUSINESSCURRENCY<>'CNY' THEN '0102'                                --外币的表内业务         大中-贸金部
                      WHEN T1.BUSINESSTYPE IN('10302010','10302015','10302020') THEN '0401'      --贴现业务               同业金融市场部
                      WHEN T1.BUSINESSTYPE IN('10201010','1035102010','1035102020') THEN '0102'  --进口信用证，跨境保函   归到 大中-贸金部
                      WHEN T1.LINETYPE='0010' THEN '0101'
                	    WHEN T1.LINETYPE='0020' THEN '0201'
                	    WHEN T1.LINETYPE='0030' THEN '0301'
                	    WHEN T1.LINETYPE='0040' THEN '0401'
                	    ELSE '0101'
                 END                                                               AS BUSINESSLINE        --条线
                ,''                                                                AS ASSETTYPE           --资产大类
                ,''                                                                AS ASSETSUBTYPE        --资产小类
                ,CASE WHEN T1.BUSINESSTYPE='11103019' AND T1.PURPOSE='010'                  --区分出个人住房抵押贷款
                      THEN '11103040'
                      ELSE T1.BUSINESSTYPE
                 END                                                               AS BUSINESSTYPEID          --业务品种代码
                ,CASE WHEN T1.BUSINESSTYPE='11103019' AND T1.PURPOSE='010'
                      THEN '个人综合消费贷款(个人住房贷款)'
                      ELSE T2.TYPENAME
                 END                                                               AS BUSINESSTYPENAME    --业务品种名称
                /*,CASE WHEN T2.ATTRIBUTE1='1'
                      THEN '01' --非零售
                      ELSE '02' --零售
                  END                                                              AS CREDITRISKDATATYPE  --信用风险数据类型
                */
                ,CASE WHEN T7.CUSTOMERTYPE = '0310' OR T7.CERTTYPE LIKE 'Ind%'
                      THEN '02' --零售
                      ELSE '01' --非零售
                  END                                                              AS CREDITRISKDATATYPE  --信用风险数据类型
                ,T1.PUTOUTDATE                                                     AS STARTDATE           --起始日期
                ,T1.MATURITY                                                       AS DUEDATE             --到期日期
                ,CASE WHEN (TO_DATE(T1.MATURITY,'YYYYMMDD')-TO_DATE(T1.PUTOUTDATE,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T1.MATURITY,'YYYYMMDD')-TO_DATE(T1.PUTOUTDATE,'YYYYMMDD'))/365
                END                                                                AS OriginalMaturity    --原始期限
                ,CASE WHEN (TO_DATE(T1.MATURITY,'YYYYMMDD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T1.MATURITY,'YYYYMMDD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD'))/365
                END                                                                AS ResidualM           --剩余期限
                ,decode(T1.BUSINESSCURRENCY,'@','CNY',T1.BUSINESSCURRENCY)                                               AS SETTLEMENTCURRENCY  --结算币种
                ,T1.BUSINESSSUM                                                    AS CONTRACTAMOUNT      --合同总金额
                ,0                                                                 AS NOTEXTRACTPART      --合同未提取部分
                ,'1'                                                               AS UNCONDCANCELFLAG    --是否可随时无条件撤销    0:否，1：是
                ,CASE WHEN T6.PROJECTNO IS NULL THEN '0'
                      ELSE '1'
                 END                                                               AS ABSUAFLAG           --资产证券化基础资产标识
                ,CASE WHEN T6.PROJECTNO IS NULL THEN ''
                      ELSE T6.PROJECTNO
                 END                                                               AS ABSPOOLID           --证券化资产池ID
                ,''                                                                AS GROUPID             --分组编号
                ,T1.VOUCHTYPE                                                      AS GUARANTEETYPE       --主要担保方式
                ,NULL                                                              AS ABSPROPORTION       --资产证券化比重
    FROM 				RWA_DEV.NCM_BUSINESS_CONTRACT T1
    INNER JOIN 	TEMP_CONTRACT T4
    ON 					T1.SERIALNO = T4.CONTRACTNO
    LEFT JOIN 	RWA_DEV.NCM_BUSINESS_TYPE T2
    ON 					T1.BUSINESSTYPE = T2.TYPENO
    AND 				T1.DATANO = T2.DATANO
    LEFT JOIN 	RWA.ORG_INFO T3
    ON 					T1.OPERATEORGID = T3.ORGID
    LEFT JOIN 	RWA.CODE_LIBRARY T5
    ON 					T1.DIRECTION = T5.ITEMNO
    AND 				T5.CODENO = 'IndustryType'
    LEFT JOIN 	(SELECT DISTINCT AA.PROJECTNO,AA.CONTRACTNO   --判断是否资产证券化
               		 FROM RWA_DEV.NCM_ABS_PROJECT_ASSET AA
             INNER JOIN RWA_DEV.NCM_ABS_PROJECT_INFO BB
               			 ON AA.PROJECTNO = BB.PROJECTNO
               			AND BB.DATANO = P_DATA_DT_STR
               			AND BB.PROJECTSTATUS='0401'            --交割成功
               		WHERE AA.DATANO = P_DATA_DT_STR
    						) T6
    ON 					T1.SERIALNO = T6.CONTRACTNO
    LEFT JOIN		RWA_DEV.NCM_CUSTOMER_INFO T7
    ON					T1.CUSTOMERID = T7.CUSTOMERID
    AND					T1.DATANO = T7.DATANO
    LEFT JOIN		(
    						select OBJECTNO, DIRECTION
								  from (select T.OBJECTNO,
								               T.DIRECTION,
								               ROW_NUMBER() OVER(PARTITION BY T.OBJECTNO order by T.SERIALNO DESC) AS RM
								          from RWA_DEV.NCM_PUTOUT_SCHEME T
								         where T.DATANO = P_DATA_DT_STR
								           and T.OBJECTTYPE = 'BusinessContract'
								           and T.DIRECTION IS NOT NULL)
								 where RM = 1
								) T8									--额度类业务的行业投向需从提用表取
    ON					T1.SERIALNO = T8.OBJECTNO
    LEFT JOIN 	RWA.CODE_LIBRARY T9
    ON 					T8.DIRECTION = T9.ITEMNO
    AND 				T9.CODENO = 'IndustryType'
    WHERE 			T1.DATANO = P_DATA_DT_STR
    ;
    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_XD_CONTRACT',cascade => true);

     --统计插入的记录
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_XD_CONTRACT;

    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功'||'-'||v_count1;
    --定义异常
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '信用风险合同表(PRO_RWA_EI_CONTRACT)ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace ;
         RETURN;
END PRO_RWA_XD_CONTRACT;
/

