CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ABS_ISSURE_COLLATERAL(
                             P_DATA_DT_STR  IN  VARCHAR2,    --数据日期
                             P_PO_RTNCODE  OUT  VARCHAR2,    --返回编号
                            P_PO_RTNMSG    OUT  VARCHAR2    --返回描述
)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_ABS_ISSURE_COLLATERAL
    实现功能:信息管理系统-抵质押,表结构为抵质押品表
    数据口径:全量
    跑批频率:月初
    版  本  :V1.0.0
    编写人  :YUSJ
    编写时间:2014-04-26
    单  位  :上海安硕信息技术股份有限公司
    源  表1  :NCM_GUARANTY_INFO|担保物信息表
    源  表2  :NCM_BUSINESS_DUEBILL|授信业务借据信息表
    源  表3  :NCM_BUSINESS_CONTRACT|授信业务合同表
    源  表4  :NCM_GUARANTY_CONTRACT|担保合同信息表
    源  表5  :NCM_CONTRACT_RELATIVE|合同关联表
    源  表6  :NCM_GUARANTY_RELATIVE|担保合同与担保物关联表
    源  表7  :NCM_CUSTOMER_INFO|客户基本信息表
    源  表8  :NCM_CODE_LIBRARY|代码库
    目标表  :RWA_XD_COLLATERAL|信贷系统-抵质押品表
    辅助表  :无
    变更记录(修改人|修改时间|修改内容)：
  */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ABS_ISSURE_COLLATERAL';
  --定义判断值变量
  v_count1 INTEGER;
  --定义异常变量
  v_raise EXCEPTION;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_ABS_ISSURE_COLLATERAL';

    /*有效借据下合同对应的抵质押品信息*/
    INSERT INTO RWA_DEV.RWA_ABS_ISSURE_COLLATERAL(
                 DATADATE                                --数据日期
                ,DATANO                                 --数据流水号
                ,COLLATERALID                           --抵质押品ID
                ,SSYSID                                 --源系统ID
                ,SGUARCONTRACTID                        --源担保合同ID
                ,SCOLLATERALID                          --源抵质押品ID
                ,COLLATERALNAME                         --抵质押品名称
                ,ISSUERID                               --发行人ID
                ,PROVIDERID                             --提供人ID
                ,CREDITRISKDATATYPE                     --信用风险数据类型
                ,GUARANTEEWAY                            --担保方式
                ,SOURCECOLTYPE                          --源抵质押品大类
                ,SOURCECOLSUBTYPE                       --源抵质押品小类
                ,SPECPURPBONDFLAG                       --是否为收购国有银行不良贷款而发行的债券
                ,QUALFLAGSTD                            --权重法合格标识
                ,QUALFLAGFIRB                           --内评初级法合格标识
                ,COLLATERALTYPESTD                      --权重法抵质押品类型
                ,COLLATERALSDVSSTD                      --权重法抵质押品细分
                ,COLLATERALTYPEIRB                      --内评法抵质押品类型
                ,COLLATERALAMOUNT                        --抵押总额
                ,CURRENCY                               --币种
                ,STARTDATE                              --起始日期
                ,DUEDATE                                --到期日期
                ,ORIGINALMATURITY                       --原始期限
                ,RESIDUALM                              --剩余期限
                ,INTEHAIRCUTSFLAG                       --自行估计折扣系数标识
                ,INTERNALHC                             --内部折扣系数
                ,FCTYPE                                 --金融质押品类型
                ,ABSFLAG                                --资产证券化标识
                ,RATINGDURATIONTYPE                     --评级期限类型
                ,FCISSUERATING                          --金融质押品发行等级
                ,FCISSUERTYPE                           --金融质押品发行人类别
                ,FCISSUERSTATE                          --金融质押品发行人注册国家
                ,FCRESIDUALM                            --金融质押品剩余期限
                ,REVAFREQUENCY                          --重估频率
                ,GROUPID                                --分组编号
                ,RCERating                              --发行人境外注册地外部评级
    )WITH TEMP_COLLATERAL1 AS(SELECT T3.SERIALNO AS CONTRACTNO,MIN(NVL(T1.PUTOUTDATE,CASE WHEN T1.BUSINESSTYPE IN ('10201010','10202010') THEN NVL(T3.OCCURDATE,T3.PUTOUTDATE) ELSE T3.PUTOUTDATE END)) AS PUTOUTDATE
                                    ,MAX(NVL(T1.ACTUALMATURITY,T3.MATURITY)) AS MATURITY, MIN(T2.ATTRIBUTE1) AS ATTRIBUTE1
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '3%'  --排除额度类业务
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --对账以借据为准，所以关联合同时，不应该加合同的有效条件
                               AND T1.DATANO=T3.DATANO
                               INNER JOIN RWA.RWA_WS_ABS_ISSUE_UNDERASSET RWAIU
                               ON T1.SERIALNO=RWAIU.HTBH
                               AND RWAIU.DATADATE = TO_DATE(P_DATA_DT_STR,'YYYYMMDD')
                               INNER JOIN  RWA.RWA_WP_DATASUPPLEMENT RWD
                               ON          RWAIU.SUPPORGID=RWD.ORGID
                               AND         RWD.DATADATE=TO_DATE(P_DATA_DT_STR,'YYYYMMDD')
                               AND         RWD.SUPPTMPLID='M-0133'
                               AND         RWD.SUBMITFLAG='1'
                               INNER JOIN 	RWA_DEV.RWA_ABS_ISSURE_CONTRACT T4
    													 ON					'ABS'||T3.SERIALNO = T4.CONTRACTID
                               WHERE T1.DATANO=P_DATA_DT_STR
                               GROUP BY T3.SERIALNO
                             )
    ,TEMP_RELATIVE AS (SELECT T5.GUARANTYID,MIN(T1.PUTOUTDATE) AS PUTOUTDATE,MAX(T1.MATURITY) AS MATURITY,MIN(ATTRIBUTE1) AS ATTRIBUTE1
                       FROM TEMP_COLLATERAL1 T1
                       INNER JOIN
                           (SELECT SERIALNO,OBJECTNO
                            FROM RWA_DEV.NCM_CONTRACT_RELATIVE
                            WHERE DATANO=P_DATA_DT_STR
                            AND OBJECTTYPE = 'GuarantyContract'
                            GROUP BY  SERIALNO,OBJECTNO
                            ) T2
                      ON T1.CONTRACTNO=T2.SERIALNO
                      INNER JOIN RWA_DEV.NCM_GUARANTY_CONTRACT T3
                      ON T2.OBJECTNO = T3.SERIALNO
                      AND T3.DATANO=P_DATA_DT_STR
                      INNER JOIN (SELECT CONTRACTNO, GUARANTYID
                                  FROM RWA_DEV.NCM_GUARANTY_RELATIVE
                                  WHERE DATANO=P_DATA_DT_STR
                                  GROUP BY CONTRACTNO, GUARANTYID
                                  ) T4
                      ON T3.SERIALNO=T4.CONTRACTNO
                      INNER JOIN RWA_DEV.NCM_GUARANTY_INFO T5
                      ON T4.GUARANTYID=T5.GUARANTYID
                      AND T5.DATANO=P_DATA_DT_STR
                      GROUP BY T5.GUARANTYID
                   )
    SELECT
                 TO_DATE(T1.DATANO,'YYYYMMDD')                                                AS DATADATE              --数据日期
                ,T1.DATANO                                                                    AS DATANO                --数据流水号
                ,'ABS'||T1.GUARANTYID                                                         AS COLLATERALID          --抵质押品ID
                ,'ABS'                                                                        AS SSYSID                --源系统ID
                ,''                                                                           AS SGUARCONTRACTID       --源担保合同ID
                ,'ABS'||T1.GUARANTYID                                                         AS SCOLLATERALID         --源抵质押品ID
                ,T6.ITEMNAME                                                                  AS COLLATERALNAME        --抵质押品名称
                ,CASE WHEN (T1.GUARANTYTYPEID LIKE '001003%' OR T1.GUARANTYTYPEID LIKE '001004%') --金融质押品需要发行人
                      THEN T3.OPENBANKNO
                      ELSE ''
                 END                                                                           AS ISSUERID              --发行人ID
                ,T1.CLRERID                                                                    AS PROVIDERID            --提供人ID
                ,CASE WHEN T2.ATTRIBUTE1='1' THEN '01'
                      ELSE '02'
                 END                                                                           AS CREDITRISKDATATYPE    --信用风险数据类型
                ,T1.GUARANTYTYPE                                                               AS GUARANTEEWAY          --担保方式
                ,SUBSTR(T1.GUARANTYTYPEID,1,6)                                                 AS SOURCECOLTYPE         --源抵质押品大类
                ,T1.GUARANTYTYPEID                                                             AS SOURCECOLSUBTYPE      --源抵质押品小类
                ,CASE WHEN T3.BONDPUBLISHPURPOSE='0010' THEN '1'
                      ELSE '0'
                 END                                                                           AS SPECPURPBONDFLAG      --是否为收购国有银行不良贷款而发行的债券
                ,''                                                                            AS QUALFLAGSTD           --权重法合格标识
                ,CASE WHEN T1.QUALIFYFLAG03='01' THEN '1'
                	    WHEN T1.QUALIFYFLAG03='02' THEN '0'
                      ELSE ''
                 END                                                                           AS QUALFLAGFIRB          --内评初级法合格标识
                ,''                                                                            AS COLLATERALTYPESTD     --权重法抵质押品类型
                ,''                                                                            AS COLLATERALSDVSSTD     --权重法抵质押品细分
                ,CASE WHEN T1.QUALIFYFLAG03 = '01' THEN T4.SLOWRELEASETYPE
                      ELSE ''
                 END                                                                           AS COLLATERALTYPEIRB     --内评法抵质押品类型
                ,T1.AFFIRMVALUE0                                                               AS COLLATERALAMOUNT     --抵押总额                              -
                ,T1.AFFIRMCURRENCY                                                             AS CURRENCY              --币种
                ,T2.PUTOUTDATE                                                                 AS STARTDATE             --起始日期
                ,T2.MATURITY                                                                   AS DUEDATE               --到期日期
                ,CASE WHEN (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(T2.PUTOUTDATE,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(T2.PUTOUTDATE,'YYYYMMDD'))/365
                END                                                                            AS ORIGINALMATURITY      --原始期限
                ,CASE WHEN (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD'))/365
                END                                                                            AS RESIDUALM             --剩余期限
                ,'0'                                                                           AS INTEHAIRCUTSFLAG      --自行估计折扣系数标识
                ,1                                                                             AS INTERNALHC            --内部折扣系数
                ,CASE WHEN T1.QUALIFYFLAG03 = '01' THEN T4.FINANCIALPLEDGETYPE
                      ELSE ''
                 END                                                                            AS FCTYPE                --金融质押品类型
                ,CASE WHEN T3.ABSFLAG='01'THEN '1'
                      ELSE '0'
                 END                                                                           AS ABSFLAG               --资产证券化标识
                ,''                                                                            AS RATINGDURATIONTYPE    --评级期限类型
                ,T3.BONDRATING                                                                 AS FCISSUERATING         --金融质押品发行等级
                ,CASE WHEN (T3.OPENBANKTYPE LIKE '10%' OR T3.OPENBANKTYPE LIKE '01%') THEN '01'
                	    ELSE '02'
                 END                                                                           AS FCISSUERTYPE          --金融质押品发行人类别
                ,CASE WHEN T3.BONDPUBLISHCOUNTRY<>'CHN' THEN '02'
                	    ELSE '01'
                 END                                                                           AS FCISSUERSTATE         --金融质押品发行人注册国家
                ,CASE WHEN (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD')) / 365 < 0
                      THEN 0
                      ELSE (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD')) / 365
                 END                                                                           AS FCRESIDUALM           --金融质押品剩余期限
                ,1                                                                             AS REVAFREQUENCY         --重估频率
                ,''                                                                            AS GROUPID               --分组编号
                ,T5.RATINGRESULT                                                               AS RCERating             --发行人境外注册地外部评级
    FROM RWA_DEV.NCM_GUARANTY_INFO T1
    INNER JOIN TEMP_RELATIVE T2
    ON T1.GUARANTYID = T2.GUARANTYID
    LEFT JOIN NCM_ASSET_FINANCE T3
    ON T1.GUARANTYID=T3.GUARANTYID
    AND T3.DATANO=P_DATA_DT_STR
    LEFT JOIN	RWA_DEV.NCM_COL_PARAM T4
    ON T1.GUARANTYTYPEID = T4.GUARANTYTYPE
    AND	T4.DATANO = p_data_dt_str
    LEFT JOIN RWA.RWA_WP_COUNTRYRATING T5
    ON T3.BONDPUBLISHCOUNTRY = T5.COUNTRYCODE
    AND	T5.ISINUSE = '1'
    LEFT JOIN RWA.CODE_LIBRARY T6
    ON T1.GUARANTYTYPE=T6.ITEMNO
    AND T6.CODENO='GuarantyList'
    WHERE T1.DATANO=P_DATA_DT_STR
    AND T1.GUARANTYTYPE NOT IN('004001004001','004001005001','004001006001','004001006002')     --信用证，备用信用证,融资性保函，非融资性保函 都归为保证
    ;
    COMMIT;

     /*插入保证金信息到抵质押品表*/
     INSERT INTO RWA_DEV.RWA_ABS_ISSURE_COLLATERAL(
                 DATADATE                                --数据日期
                ,DATANO                                 --数据流水号
                ,COLLATERALID                           --抵质押品ID
                ,SSYSID                                 --源系统ID
                ,SGUARCONTRACTID                        --源担保合同ID
                ,SCOLLATERALID                          --源抵质押品ID
                ,COLLATERALNAME                         --抵质押品名称
                ,ISSUERID                               --发行人ID
                ,PROVIDERID                             --提供人ID
                ,CREDITRISKDATATYPE                     --信用风险数据类型
                ,GUARANTEEWAY                            --担保方式
                ,SOURCECOLTYPE                          --源抵质押品大类
                ,SOURCECOLSUBTYPE                       --源抵质押品小类
                ,SPECPURPBONDFLAG                       --是否为收购国有银行不良贷款而发行的债券
                ,QUALFLAGSTD                            --权重法合格标识
                ,QUALFLAGFIRB                           --内评初级法合格标识
                ,COLLATERALTYPESTD                      --权重法抵质押品类型
                ,COLLATERALSDVSSTD                      --权重法抵质押品细分
                ,COLLATERALTYPEIRB                      --内评法抵质押品类型
                ,COLLATERALAMOUNT                        --抵押总额
                ,CURRENCY                               --币种
                ,STARTDATE                              --起始日期
                ,DUEDATE                                --到期日期
                ,ORIGINALMATURITY                       --原始期限
                ,RESIDUALM                              --剩余期限
                ,INTEHAIRCUTSFLAG                       --自行估计折扣系数标识
                ,INTERNALHC                             --内部折扣系数
                ,FCTYPE                                 --金融质押品类型
                ,ABSFLAG                                --资产证券化标识
                ,RATINGDURATIONTYPE                     --评级期限类型
                ,FCISSUERATING                          --金融质押品发行等级
                ,FCISSUERTYPE                           --金融质押品发行人类别
                ,FCISSUERSTATE                          --金融质押品发行人注册国家
                ,FCRESIDUALM                            --金融质押品剩余期限
                ,REVAFREQUENCY                          --重估频率
                ,GROUPID                                --分组编号
                ,RCERating                              --发行人境外注册地外部评级
    )
    SELECT
                T2.DATADATE									                                                   AS DATADATE              --数据日期
                ,T2.DATANO                                                                     AS DATANO                --数据流水号
                ,'ABS'||T1.CONTRACTNO                                                          AS COLLATERALID          --抵质押品ID
                ,'ABS'                                                                         AS SSYSID                --源系统ID
                ,'ABS'||T1.CONTRACTNO                                                          AS SGUARCONTRACTID       --源担保合同ID
                ,'ABS'||T1.CONTRACTNO                                                          AS SCOLLATERALID         --源抵质押品ID
                ,'保证金'                                                                      AS COLLATERALNAME        --抵质押品名称
                ,''                                                                            AS ISSUERID              --发行人ID
                ,T2.CLIENTID	                                                                 AS PROVIDERID            --提供人ID
                ,T2.CREDITRISKDATATYPE                                               					 AS CREDITRISKDATATYPE    --信用风险数据类型
                ,'060'                                                                         AS GUARANTEEWAY          --担保方式
                ,'001001'                                                                      AS SOURCECOLTYPE         --源抵质押品大类
                ,'001001003001'                                                                AS SOURCECOLSUBTYPE      --源抵质押品小类
                ,'0'                                                                           AS SPECPURPBONDFLAG      --是否为收购国有银行不良贷款而发行的债券
                ,'01'                                                                          AS QUALFLAGSTD           --权重法合格标识
                ,'01'                                                                          AS QUALFLAGFIRB          --内评初级法合格标识
                ,'030103'                                                                      AS COLLATERALTYPESTD     --权重法抵质押品类型
                ,'01'                                                                          AS COLLATERALSDVSSTD     --权重法抵质押品细分
                ,'030201'                                                                      AS COLLATERALTYPEIRB     --内评法抵质押品类型
                ,T1.BAILBALANCE                                                                AS COLLATERALAMOUNT     --抵押总额                              -
                ,T1.BAILCURRENCY					                                                     AS CURRENCY              --币种
                ,T2.STARTDATE	                                                                 AS STARTDATE             --起始日期
                ,T2.DUEDATE	                                                                   AS DUEDATE               --到期日期
                ,T2.ORIGINALMATURITY                                                           AS ORIGINALMATURITY      --原始期限
                ,T2.RESIDUALM                                                                  AS RESIDUALM             --剩余期限
                ,'0'                                                                           AS INTEHAIRCUTSFLAG      --自行估计折扣系数标识
                ,1                                                                             AS INTERNALHC            --内部折扣系数
                ,'01'                                                                          AS FCTYPE                --金融质押品类型
                ,'0'                                                                           AS ABSFLAG               --资产证券化标识
                ,''                                                                            AS RATINGDURATIONTYPE    --评级期限类型
                ,''                                                                            AS FCISSUERATING         --金融质押品发行等级
                ,NULL                                                                          AS FCISSUERTYPE          --金融质押品发行人类别
                ,''                                                                            AS FCISSUERSTATE         --金融质押品发行人注册国家
                ,''                                                                            AS FCRESIDUALM           --金融质押品剩余期限
                ,1                                                                             AS REVAFREQUENCY         --重估频率
                ,''                                                                            AS GROUPID               --分组编号
                ,NULL                                                                          AS RCERating             --发行人境外注册地外部评级
    FROM				RWA_DEV.RWA_TEMP_BAIL2 T1															--信贷合同表
    INNER JOIN	RWA_DEV.RWA_ABS_ISSURE_CONTRACT T2										--信贷借据表
    ON					'ABS' || T1.CONTRACTNO = T2.CONTRACTID
		WHERE 			T1.ISMAX = '1'																				--取相同合同下最大的一笔作为结果
    ;
    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_ABS_ISSURE_COLLATERAL',cascade => true);

    /*目标表数据统计*/
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_ABS_ISSURE_COLLATERAL;
    --Dbms_output.Put_line('rwa_xd_collateral表当前插入的数据记录为:' || (v_count3-v_count2) || '条');
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功'||'-'||v_count1;
    --定义异常
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '资产证券化-抵质押品('||v_pro_name||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;

END PRO_RWA_ABS_ISSURE_COLLATERAL;
/

