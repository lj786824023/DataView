CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_XD_COLLATERAL(
                             P_DATA_DT_STR  IN  VARCHAR2,    --数据日期
                             P_PO_RTNCODE  OUT  VARCHAR2,    --返回编号
                            P_PO_RTNMSG    OUT  VARCHAR2    --返回描述
)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_XD_COLLATERAL
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
  --v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_XD_COLLATERAL';
  --定义判断值变量
  v_count1 INTEGER;
  --定义异常变量
  v_raise EXCEPTION;
  --定义临时表名
  --v_tabname VARCHAR2(200);
  --定义创建语句
  --v_create VARCHAR2(1000) ;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_XD_COLLATERAL';

    /*1.1 有效借据下合同对应的抵质押品信息(普通)*/
    INSERT INTO RWA_DEV.RWA_XD_COLLATERAL(
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
                ,flag
    )WITH TEMP_COLLATERAL1 AS(SELECT T3.SERIALNO AS CONTRACTNO
                                    ,MIN(CASE WHEN T1.BUSINESSTYPE IN ('10201010','10202010','1035101010','1035101020','1035102010','1035102020','103550','1040101040') --信用证,保函
                                              THEN NVL(T3.PUTOUTDATE,T3.OCCURDATE)   --信用证保函，取合同起始日期
                                              ELSE T1.PUTOUTDATE 
                                         END) AS PUTOUTDATE
                                    ,MAX(CASE WHEN T1.BUSINESSTYPE IN ('10201010','10202010','1035101010','1035101020','1035102010','1035102020','103550','1040101040') --信用证,保函
                                              THEN T3.MATURITY
                                              ELSE T1.ACTUALMATURITY
                                         END) AS MATURITY
                                    ,MIN(T4.CREDITRISKDATATYPE) AS CREDITRISKDATATYPE
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --排除额度类业务
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --对账以借据为准，所以关联合同时，不应该加合同的有效条件
                               AND T1.DATANO=T3.DATANO
                               INNER JOIN RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND T31.BALANCE>0
                                          /*rwa_dev.brd_loan_nor t31
                               ON  t1.serialno = t31.crdt_acct_no
                               AND t31.datano = P_DATA_DT_STR
                               AND t31.cur_bal > 0*/
                               INNER JOIN RWA_DEV.RWA_XD_CONTRACT T4
    													 ON T1.RELATIVESERIALNO2 = T4.CONTRACTID
    													 AND T1.DATANO = T4.DATANO
                               WHERE T1.DATANO=P_DATA_DT_STR
                               --AND T1.BALANCE>0           --有效判断条件
                               AND T1.SERIALNO<>'BD2014110400000001'
                               AND T1.BUSINESSTYPE NOT IN ('1040101010','1040101020','1040101030','1040102010','1040102020','1040102040','10303010'
                                   ,'1040105060','1040201010','1040201020','1040202010','105010','11105010','11105020','10302010','10302015','10302020')  --排除同业，回购，投资，委托贷款业务,贴现和转帖也排除，用票据信息作为缓释
                               GROUP BY T3.SERIALNO
                             )
    ,TEMP_RELATIVE AS (SELECT T5.GUARANTYID,
                              MIN(T1.PUTOUTDATE) AS PUTOUTDATE,
                              MAX(T1.MATURITY) AS MATURITY,
                              MIN(T1.CREDITRISKDATATYPE) AS CREDITRISKDATATYPE
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
                ,'YP'||T1.GUARANTYID                                                          AS COLLATERALID          --抵质押品ID
                ,'XD'                                                                         AS SSYSID                --源系统ID
                ,''                                                                           AS SGUARCONTRACTID       --源担保合同ID
                ,T1.GUARANTYID                                                                AS SCOLLATERALID         --源抵质押品ID
                ,T6.ITEMNAME                                                                  AS COLLATERALNAME        --抵质押品名称
                ,CASE WHEN (T1.GUARANTYTYPEID LIKE '001003%' OR T1.GUARANTYTYPEID LIKE '001004%' OR T1.GUARANTYTYPEID LIKE '001001%') --金融质押品需要发行人
                      THEN NVL(T3.OPENBANKNO,'中国商业银行')
                      ELSE ''
                 END                                                                           AS ISSUERID              --发行人ID
                ,T1.CLRERID                                                                    AS PROVIDERID            --提供人ID
                ,T2.CREDITRISKDATATYPE                                                         AS CREDITRISKDATATYPE    --信用风险数据类型
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
                ,CASE WHEN T1.AFFIRMCURRENCY='...' or T1.AFFIRMCURRENCY like '@%' THEN 'CNY'
                	    ELSE NVL(T1.AFFIRMCURRENCY,'CNY')
                 END                                                                           AS CURRENCY              --币种
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
                ,'DZY|PT'
        FROM RWA_DEV.NCM_GUARANTY_INFO T1
  INNER JOIN TEMP_RELATIVE T2
        ON T1.GUARANTYID = T2.GUARANTYID
   LEFT JOIN RWA_DEV.NCM_ASSET_FINANCE T3
        ON T1.GUARANTYID=T3.GUARANTYID
        AND T3.DATANO=P_DATA_DT_STR
   LEFT JOIN	RWA_DEV.NCM_COL_PARAM T4
        ON T1.GUARANTYTYPEID = T4.GUARANTYTYPE
        AND	T4.DATANO = p_data_dt_str
   LEFT JOIN RWA.CODE_LIBRARY T6
        ON T1.GUARANTYTYPEID=T6.ITEMNO
        AND T6.CODENO='GuarantyList'
   LEFT JOIN RWA.RWA_WP_COUNTRYRATING T5
        ON T3.BONDPUBLISHCOUNTRY = T5.COUNTRYCODE
        AND	T5.ISINUSE = '1'
   WHERE T1.DATANO=P_DATA_DT_STR
     --AND T1.CLRSTATUS='01'               --modify by yushuangjiang
     --AND T1.CLRGNTSTATUS IN ('03','10')  --二期经过信贷陈康的确定，把这两个状态的条件去掉
     AND T1.GUARANTYTYPEID NOT IN('004001004001','004001005001','004001006001','004001006002','001001003001')  --信用证，备用信用证,融资性保函，非融资性保函 都归为保证
     ;
    COMMIT;
    
    /*1.2 有效借据下合同对应的抵质押品信息(逾期贷款-微粒贷)*/
   /* INSERT INTO RWA_DEV.RWA_XD_COLLATERAL(
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
                ,flag
    )WITH TEMP_COLLATERAL1 AS(SELECT T3.SERIALNO AS CONTRACTNO,
                                     MIN(NVL(T1.PUTOUTDATE,
                                         CASE WHEN T1.BUSINESSTYPE IN ('10201010','10202010') 
                                         THEN NVL(T3.OCCURDATE,T3.PUTOUTDATE) 
                                         ELSE T3.PUTOUTDATE END)) AS PUTOUTDATE
                                    ,MAX(NVL(T1.ACTUALMATURITY,T3.MATURITY)) AS MATURITY,
                                     MIN(T4.CREDITRISKDATATYPE) AS CREDITRISKDATATYPE
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --排除额度类业务
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --对账以借据为准，所以关联合同时，不应该加合同的有效条件
                               AND T1.DATANO=T3.DATANO
                               INNER JOIN RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND (T31.OVERDUEBALANCE+T31.DULLBALANCE+T31.BADBALANCE)>0   --取到逾期的记录
                                          \*rwa_dev.brd_loan_nor t31
                               ON t1.serialno = t31.crdt_acct_no
                               AND t31.datano = P_DATA_DT_STR
                               AND t31.sbjt_cd = '13100001' --逾期微粒贷*\
                               INNER JOIN RWA_DEV.RWA_XD_CONTRACT T4
    													 ON T1.RELATIVESERIALNO2 = T4.CONTRACTID
    													 AND T1.DATANO = T4.DATANO
                               WHERE  T1.DATANO=P_DATA_DT_STR
                               AND T1.BUSINESSTYPE='11103030'  --只取微粒贷业务
                               GROUP BY T3.SERIALNO
                             )
    ,TEMP_RELATIVE AS (SELECT T5.GUARANTYID,MIN(T1.PUTOUTDATE) AS PUTOUTDATE,MAX(T1.MATURITY) AS MATURITY,MIN(T1.CREDITRISKDATATYPE) AS CREDITRISKDATATYPE
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
                ,'YP'||T1.GUARANTYID                                                          AS COLLATERALID          --抵质押品ID
                ,'XD'                                                                         AS SSYSID                --源系统ID
                ,''                                                                           AS SGUARCONTRACTID       --源担保合同ID
                ,T1.GUARANTYID                                                                AS SCOLLATERALID         --源抵质押品ID
                ,T6.ITEMNAME                                                                  AS COLLATERALNAME        --抵质押品名称
                ,CASE WHEN (T1.GUARANTYTYPEID LIKE '001003%' OR T1.GUARANTYTYPEID LIKE '001004%' OR T1.GUARANTYTYPEID LIKE '001001%') --金融质押品需要发行人
                      THEN T3.OPENBANKNO
                      ELSE ''
                 END                                                                           AS ISSUERID              --发行人ID
                ,T1.CLRERID                                                                    AS PROVIDERID            --提供人ID
                ,T2.CREDITRISKDATATYPE                                                         AS CREDITRISKDATATYPE    --信用风险数据类型
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
                ,CASE WHEN T1.AFFIRMCURRENCY='...' THEN 'CNY'
                	    ELSE NVL(T1.AFFIRMCURRENCY,'CNY')
                 END                                                                           AS CURRENCY              --币种
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
                ,'DZY|YQWLD'
    FROM RWA_DEV.NCM_GUARANTY_INFO T1
    INNER JOIN TEMP_RELATIVE T2
    ON T1.GUARANTYID = T2.GUARANTYID
    LEFT JOIN RWA_DEV.NCM_ASSET_FINANCE T3
    ON T1.GUARANTYID=T3.GUARANTYID
    AND T3.DATANO=P_DATA_DT_STR
    LEFT JOIN	RWA_DEV.NCM_COL_PARAM T4
    ON T1.GUARANTYTYPEID = T4.GUARANTYTYPE
    AND	T4.DATANO = p_data_dt_str
    LEFT JOIN RWA.CODE_LIBRARY T6
    ON T1.GUARANTYTYPEID=T6.ITEMNO
    AND T6.CODENO='GuarantyList'
    LEFT JOIN RWA.RWA_WP_COUNTRYRATING T5
    ON T3.BONDPUBLISHCOUNTRY = T5.COUNTRYCODE
    AND	T5.ISINUSE = '1'
    WHERE T1.DATANO=P_DATA_DT_STR
    --AND T1.CLRSTATUS='01'               --modify by yushuangjiang
     --AND T1.CLRGNTSTATUS IN ('03','10')  --二期经过信贷陈康的确定，把这两个状态的条件去掉
    AND T1.GUARANTYTYPEID NOT IN('004001004001','004001005001','004001006001','004001006002','001001003001')  --信用证，备用信用证,融资性保函，非融资性保函 都归为保证
    AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_XD_COLLATERAL T6 WHERE 'YP'||T1.GUARANTYID=T6.COLLATERALID)
    ;
    COMMIT;*/
    
    /*1.3 有效借据下合同对应的抵质押品信息(逾期贷款-其余业务)*/
    INSERT INTO RWA_DEV.RWA_XD_COLLATERAL(
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
                ,flag
    )WITH TEMP_COLLATERAL1 AS(SELECT T3.SERIALNO AS CONTRACTNO
                                    ,MIN(CASE WHEN T1.BUSINESSTYPE IN ('10201010','10202010','1035101010','1035101020','1035102010','1035102020','103550','1040101040') --信用证,保函
                                              THEN NVL(T3.PUTOUTDATE,T3.OCCURDATE)   --信用证保函，取合同起始日期
                                              ELSE T1.PUTOUTDATE 
                                         END) AS PUTOUTDATE
                                    ,MAX(CASE WHEN T1.BUSINESSTYPE IN ('10201010','10202010','1035101010','1035101020','1035102010','1035102020','103550','1040101040') --信用证,保函
                                              THEN T3.MATURITY
                                              ELSE T1.ACTUALMATURITY
                                         END) AS MATURITY
                                    ,MIN(T4.CREDITRISKDATATYPE) AS CREDITRISKDATATYPE
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --排除额度类业务
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --对账以借据为准，所以关联合同时，不应该加合同的有效条件
                               AND T1.DATANO=T3.DATANO
                               INNER JOIN BRD_LOAN_NOR T4 --正常贷款
                               ON  T4.DATANO = p_data_dt_str
                               AND T4.CRDT_ACCT_NO = T1.SERIALNO 
                               AND substr(T4.SBJT_CD,1,4) = '1310' --科目编号
                               AND T4.SBJT_CD != '13100001' --所有不含微粒贷的逾期贷款                               
                               INNER JOIN RWA_DEV.RWA_XD_CONTRACT T4
                               ON T1.RELATIVESERIALNO2 = T4.CONTRACTID
                               AND T1.DATANO = T4.DATANO
                               WHERE  T1.DATANO=P_DATA_DT_STR
                               GROUP BY T3.SERIALNO                            
                             )
    ,TEMP_RELATIVE AS (SELECT T5.GUARANTYID,MIN(T1.PUTOUTDATE) AS PUTOUTDATE,MAX(T1.MATURITY) AS MATURITY,MIN(T1.CREDITRISKDATATYPE) AS CREDITRISKDATATYPE
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
                ,'YP'||T1.GUARANTYID                                                          AS COLLATERALID          --抵质押品ID
                ,'XD'                                                                         AS SSYSID                --源系统ID
                ,''                                                                           AS SGUARCONTRACTID       --源担保合同ID
                ,T1.GUARANTYID                                                                AS SCOLLATERALID         --源抵质押品ID
                ,T6.ITEMNAME                                                                  AS COLLATERALNAME        --抵质押品名称
                ,CASE WHEN (T1.GUARANTYTYPEID LIKE '001003%' OR T1.GUARANTYTYPEID LIKE '001004%' OR T1.GUARANTYTYPEID LIKE '001001%') --金融质押品需要发行人
                      THEN NVL(T3.OPENBANKNO,'中国商业银行')
                      ELSE ''
                 END                                                                           AS ISSUERID              --发行人ID
                ,T1.CLRERID                                                                    AS PROVIDERID            --提供人ID
                ,T2.CREDITRISKDATATYPE                                                         AS CREDITRISKDATATYPE    --信用风险数据类型
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
                ,CASE WHEN T1.AFFIRMCURRENCY='...' or T1.AFFIRMCURRENCY like '@%' THEN 'CNY'
                	    ELSE NVL(T1.AFFIRMCURRENCY,'CNY')
                 END                                                                           AS CURRENCY              --币种
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
                ,'DZY|YQ'
    FROM RWA_DEV.NCM_GUARANTY_INFO T1
    INNER JOIN TEMP_RELATIVE T2
    ON T1.GUARANTYID = T2.GUARANTYID
    LEFT JOIN RWA_DEV.NCM_ASSET_FINANCE T3
    ON T1.GUARANTYID=T3.GUARANTYID
    AND T3.DATANO=P_DATA_DT_STR
    LEFT JOIN	RWA_DEV.NCM_COL_PARAM T4
    ON T1.GUARANTYTYPEID = T4.GUARANTYTYPE
    AND	T4.DATANO = p_data_dt_str
    LEFT JOIN RWA.CODE_LIBRARY T6
    ON T1.GUARANTYTYPEID=T6.ITEMNO
    AND T6.CODENO='GuarantyList'
    LEFT JOIN RWA.RWA_WP_COUNTRYRATING T5
    ON T3.BONDPUBLISHCOUNTRY = T5.COUNTRYCODE
    AND	T5.ISINUSE = '1'
    WHERE T1.DATANO=P_DATA_DT_STR
    --AND T1.CLRSTATUS='01'               --modify by yushuangjiang
     --AND T1.CLRGNTSTATUS IN ('03','10')  --二期经过信贷陈康的确定，把这两个状态的条件去掉
    AND T1.GUARANTYTYPEID NOT IN('004001004001','004001005001','004001006001','004001006002','001001003001')  --信用证，备用信用证,融资性保函，非融资性保函 都归为保证
    AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_XD_COLLATERAL T6 WHERE 'YP'||T1.GUARANTYID=T6.COLLATERALID)
    ;
    COMMIT;
    
    /*1.4 有效借据下合同对应的抵质押品信息(追加到PUTOUT表 上的抵质押品信息)*/
    INSERT INTO RWA_DEV.RWA_XD_COLLATERAL(
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
                ,flag
    )WITH TEMP_COLLATERAL1 AS(SELECT T3.SERIALNO AS CONTRACTNO,MIN(NVL(T1.PUTOUTDATE,CASE WHEN T1.BUSINESSTYPE IN ('10201010','10202010') THEN NVL(T3.OCCURDATE,T3.PUTOUTDATE) ELSE T3.PUTOUTDATE END)) AS PUTOUTDATE
                                    ,MAX(NVL(T3.MATURITY,T1.ACTUALMATURITY)) AS MATURITY,MIN(T6.CREDITRISKDATATYPE) AS CREDITRISKDATATYPE,T4.SERIALNO AS BPSERIALNO
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --排除额度类业务
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --对账以借据为准，所以关联合同时，不应该加合同的有效条件
                               AND T1.DATANO=T3.DATANO
                               INNER JOIN RWA_DEV.NCM_BUSINESS_PUTOUT T4
                               ON T3.SERIALNO=T4.CONTRACTSERIALNO
                               AND T4.DATANO=P_DATA_DT_STR
                               INNER JOIN RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND T31.BALANCE>0
                                         /* rwa_dev.brd_loan_nor t31
                               ON t1.serialno = t31.crdt_acct_no
                               AND t31.datano = P_DATA_DT_STR
                               AND t31.cur_bal > 0*/
                               INNER JOIN RWA_DEV.RWA_XD_CONTRACT T6
    													 ON T1.RELATIVESERIALNO2 = T6.CONTRACTID
    													 AND T1.DATANO = T6.DATANO
                               WHERE T1.DATANO=P_DATA_DT_STR
                               --AND T1.BALANCE>0           --有效判断条件
                               AND T1.SERIALNO<>'BD2014110400000001'
                               AND T1.BUSINESSTYPE NOT IN ('1040101010','1040101020','1040101030','1040102010','1040102020','1040102040','10303010'
                                   ,'1040105060','1040201010','1040201020','1040202010','105010','11105010','11105020','10302010','10302015','10302020')  --排除同业，回购，投资，委托贷款业务,贴现和转帖也排除，用票据信息作为缓释
                               GROUP BY T3.SERIALNO,T4.SERIALNO
                             )
    ,TEMP_RELATIVE AS (SELECT T5.GUARANTYID,MIN(T1.PUTOUTDATE) AS PUTOUTDATE,MAX(T1.MATURITY) AS MATURITY,MIN(T1.CREDITRISKDATATYPE) AS CREDITRISKDATATYPE
                       FROM TEMP_COLLATERAL1 T1
                      INNER JOIN (SELECT OBJECTNO, GUARANTYID
                                  FROM RWA_DEV.NCM_GUARANTY_RELATIVE
                                  WHERE DATANO=P_DATA_DT_STR
                                  AND OBJECTTYPE='PutOutApply'
                                  GROUP BY OBJECTNO, GUARANTYID
                                  ) T4
                      ON T1.BPSERIALNO=T4.OBJECTNO
                      INNER JOIN RWA_DEV.NCM_GUARANTY_INFO T5
                      ON T4.GUARANTYID=T5.GUARANTYID
                      AND T5.DATANO=P_DATA_DT_STR
                      GROUP BY T5.GUARANTYID
                   )
    SELECT
                 TO_DATE(T1.DATANO,'YYYYMMDD')                                                AS DATADATE              --数据日期
                ,T1.DATANO                                                                    AS DATANO                --数据流水号
                ,'YP'||T1.GUARANTYID                                                          AS COLLATERALID          --抵质押品ID
                ,'XD'                                                                         AS SSYSID                --源系统ID
                ,''                                                                           AS SGUARCONTRACTID       --源担保合同ID
                ,T1.GUARANTYID                                                                AS SCOLLATERALID         --源抵质押品ID
                ,T6.ITEMNAME                                                                  AS COLLATERALNAME        --抵质押品名称
                ,CASE WHEN (T1.GUARANTYTYPEID LIKE '001003%' OR T1.GUARANTYTYPEID LIKE '001004%' OR T1.GUARANTYTYPEID LIKE '001001%') --金融质押品需要发行人
                      THEN T3.OPENBANKNO
                      ELSE ''
                 END                                                                           AS ISSUERID              --发行人ID
                ,T1.CLRERID                                                                    AS PROVIDERID            --提供人ID
                ,T2.CREDITRISKDATATYPE                                                         AS CREDITRISKDATATYPE    --信用风险数据类型
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
                ,CASE WHEN T1.AFFIRMCURRENCY='...' THEN 'CNY'
                	    ELSE NVL(T1.AFFIRMCURRENCY,'CNY')
                 END                                                                           AS CURRENCY              --币种
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
                ,CASE WHEN T1.QUALIFYFLAG03 = '01' THEN T4.FINANCIALPLEDGETYPE
                      ELSE ''
                 END                                                                           AS RATINGDURATIONTYPE    --评级期限类型
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
                ,'DZY|PUTOUT'
    FROM RWA_DEV.NCM_GUARANTY_INFO T1
    INNER JOIN TEMP_RELATIVE T2
    ON T1.GUARANTYID = T2.GUARANTYID
    LEFT JOIN NCM_ASSET_FINANCE T3
    ON T1.GUARANTYID=T3.GUARANTYID
    AND T3.DATANO=P_DATA_DT_STR
    LEFT JOIN	RWA_DEV.NCM_COL_PARAM T4
    ON T1.GUARANTYTYPEID = T4.GUARANTYTYPE
    AND	T4.DATANO = P_DATA_DT_STR
    LEFT JOIN RWA.CODE_LIBRARY T6
    ON T1.GUARANTYTYPEID=T6.ITEMNO
    AND T6.CODENO='GuarantyList'
    LEFT JOIN RWA.RWA_WP_COUNTRYRATING T5
    ON T3.BONDPUBLISHCOUNTRY = T5.COUNTRYCODE
    AND	T5.ISINUSE = '1'
    WHERE T1.DATANO=P_DATA_DT_STR
    --AND T1.CLRSTATUS='01'               --modify by yushuangjiang
     --AND T1.CLRGNTSTATUS IN ('03','10')  --二期经过信贷陈康的确定，把这两个状态的条件去掉
    AND T1.GUARANTYTYPEID NOT IN('004001004001','004001005001','004001006001','004001006002','001001003001')     --信用证，备用信用证,融资性保函，非融资性保函 都归为保证
    AND NOT EXISTS (SELECT 1 FROM RWA_DEV.RWA_XD_COLLATERAL T7 WHERE 'YP'||T1.GUARANTYID=T7.COLLATERALID )
    ;
    COMMIT;
    
    /*1.5 有效借据下合同对应的抵质押品信息（票据贴现，转帖现_外转）*/
    /*INSERT INTO RWA_DEV.RWA_XD_COLLATERAL(
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
                ,RCERating														  --发行人境外注册地外部评级
                ,flag
    )WITH TEMP_COLLATERAL1 AS(SELECT  T3.SERIALNO AS CONTRACTNO,MIN(NVL(T1.PUTOUTDATE,T3.PUTOUTDATE)) AS PUTOUTDATE,MAX(NVL(T1.ACTUALMATURITY,T3.MATURITY)) AS MATURITY,T3.BUSINESSTYPE
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --对账以借据为准，所以关联合同时，不应该加合同的有效条件
                               AND T1.DATANO=T3.DATANO
                               --AND T3.BUSINESSTYPE || T3.BUSINESSNATURE <> '1030301005'   --排除外部转帖现
                               INNER JOIN RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND T31.BALANCE>0
                                          \*rwa_dev.brd_loan_nor t31
                               ON t1.serialno = t31.crdt_acct_no
                               AND t31.datano = P_DATA_DT_STR
                               AND t31.cur_bal > 0*\
                               WHERE T1.DATANO=P_DATA_DT_STR
                               --AND T1.BALANCE>0           --有效判断条件
                               AND T1.BUSINESSTYPE IN ('10302010','10302015','10302020')  --贴现，用票据信息作为缓释
                               GROUP BY T3.SERIALNO,T3.BUSINESSTYPE
                             )
    SELECT
                 TO_DATE(T1.DATANO,'YYYYMMDD')                                                 AS DATADATE              --数据日期
                ,T1.DATANO                                                                     AS DATANO                --数据流水号
                ,'PJ'||T1.SERIALNO                                                             AS COLLATERALID          --抵质押品ID
                ,'XD'                                                                          AS SSYSID                --源系统ID
                ,''                                                                            AS SGUARCONTRACTID       --源担保合同ID
                ,T1.SERIALNO                                                                   AS SCOLLATERALID         --源抵质押品ID
                ,CASE WHEN T2.BUSINESSTYPE='10302020' THEN '中国商业银行承兑汇票'
                      ELSE '商业承兑汇票'
                 END                                                                           AS COLLATERALNAME        --抵质押品名称
                ,NVL(T1.ACCEPTORBANKID,'XN-ZGSYYH')                                            AS ISSUERID              --发行人ID
                ,T1.HOLDERID                                                                   AS PROVIDERID            --提供人ID
                ,'01'                                                                          AS CREDITRISKDATATYPE    --信用风险数据类型
                ,'060'                                                                         AS GUARANTEEWAY          --担保方式
                ,'001004'                                                                      AS SOURCECOLTYPE         --源抵质押品大类
                ,CASE WHEN T2.BUSINESSTYPE='10302020' THEN '001004002001'
                      ELSE '001004004001'
                 END                                                                           AS SOURCECOLSUBTYPE      --源抵质押品小类
                ,'0'                                                                           AS SPECPURPBONDFLAG      --是否为收购国有银行不良贷款而发行的债券
                ,''                                                                            AS QUALFLAGSTD           --权重法合格标识
                ,''                                                                            AS QUALFLAGFIRB          --内评初级法合格标识
                ,''                                                                            AS COLLATERALTYPESTD     --权重法抵质押品类型
                ,''                                                                            AS COLLATERALSDVSSTD     --权重法抵质押品细分
                ,''                                                                            AS COLLATERALTYPEIRB     --内评法抵质押品类型
                ,T1.BILLSUM                                                                    AS COLLATERALAMOUNT     --抵押总额                              -
                ,T1.LCCURRENCY                                                                 AS CURRENCY              --币种
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
                ,''                                                                            AS FCTYPE                --金融质押品类型
                ,'0'                                                                           AS ABSFLAG               --资产证券化标识
                ,''                                                                            AS RATINGDURATIONTYPE    --评级期限类型
                ,''                                                                            AS FCISSUERATING         --金融质押品发行等级
                ,'02'                                                                          AS FCISSUERTYPE          --金融质押品发行人类别
                ,CASE WHEN T3.COUNTRYCODE<>'CHN' THEN '02'
                	    ELSE '01'
                 END                                                                           AS FCISSUERSTATE         --金融质押品发行人注册国家
                ,CASE WHEN (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD')) / 365 < 0
                      THEN 0
                      ELSE (TO_DATE(T2.MATURITY,'YYYYMMDD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD')) / 365
                 END                                                                           AS FCRESIDUALM           --金融质押品剩余期限
                ,1                                                                             AS REVAFREQUENCY         --重估频率
                ,''                                                                            AS GROUPID               --分组编号
                ,T5.RATINGRESULT                                                               AS RCERating             --发行人境外注册地外部评级
                ,'DZY|TXZT'
    FROM RWA_DEV.NCM_BILL_INFO T1
    INNER JOIN TEMP_COLLATERAL1 T2
    ON T1.OBJECTNO = T2.CONTRACTNO
    LEFT JOIN NCM_CUSTOMER_INFO T3
    ON T1.ACCEPTORBANKID=T3.CUSTOMERID
    AND T3.DATANO=P_DATA_DT_STR
    LEFT JOIN RWA.RWA_WP_COUNTRYRATING T5
    ON T3.COUNTRYCODE = T5.COUNTRYCODE
    AND	T5.ISINUSE = '1'
    WHERE T1.DATANO=P_DATA_DT_STR
    AND T1.OBJECTTYPE='BusinessContract'
    ;
    COMMIT;*/
    
    /*2.1 插入保证金信息到抵质押品表-正常业务*/
    INSERT INTO RWA_DEV.RWA_XD_COLLATERAL(
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
                ,flag
    )WITH TEMP_COLLATERAL3 AS(SELECT T3.SERIALNO AS CONTRACTNO
                                    ,MIN(CASE WHEN T1.BUSINESSTYPE IN ('10201010','10202010','1035101010','1035101020','1035102010','1035102020','103550','1040101040') --信用证,保函
                                              THEN NVL(T3.PUTOUTDATE,T3.OCCURDATE)   --信用证保函，取合同起始日期
                                              ELSE T1.PUTOUTDATE 
                                         END) AS PUTOUTDATE
                                    ,MAX(CASE WHEN T1.BUSINESSTYPE IN ('10201010','10202010','1035101010','1035101020','1035102010','1035102020','103550','1040101040') --信用证,保函
                                              THEN T3.MATURITY
                                              ELSE T1.ACTUALMATURITY
                                         END) AS MATURITY
                                    ,MIN(T6.CREDITRISKDATATYPE) AS CREDITRISKDATATYPE
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --排除额度类业务
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --对账以借据为准，所以关联合同时，不应该加合同的有效条件
                               AND T1.DATANO=T3.DATANO
                               --AND T3.BUSINESSTYPE || T3.BUSINESSNATURE <> '1030301005'   --排除外部转帖现
                               INNER JOIN RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND T31.BALANCE>0
                                          /*rwa_dev.brd_loan_nor t31
                               ON t1.serialno = t31.crdt_acct_no
                               AND t31.datano = P_DATA_DT_STR
                               AND t31.cur_bal > 0*/
                               INNER JOIN RWA_DEV.RWA_XD_CONTRACT T6
    													 ON T1.RELATIVESERIALNO2 = T6.CONTRACTID
    													 AND T1.DATANO = T6.DATANO
                               WHERE T1.DATANO=P_DATA_DT_STR
                               --AND T1.BALANCE>0           --有效判断条件
                               AND T1.SERIALNO<>'BD2014110400000001'
                               AND T1.BUSINESSTYPE NOT IN ('1040101010','1040101020','1040101030','1040102010','1040102020','1040102040','10303010'
                                   ,'1040105060','1040201010','1040201020','1040202010','105010','11105010','11105020','10302010','10302015','10302020')  --排除同业，回购，投资，委托贷款业务,贴现和转帖也排除，用票据信息作为缓释
                               GROUP BY T3.SERIALNO
                             )
    SELECT
                TO_DATE(T1.DATANO,'YYYYMMDD')                                                  AS DATADATE              --数据日期
                ,T1.DATANO                                                                     AS DATANO                --数据流水号
                ,'HT'||T1.SERIALNO||T3.BAILCURRENCY                                            AS COLLATERALID          --抵质押品ID
                ,'XD'                                                                          AS SSYSID                --源系统ID
                ,T1.SERIALNO                                                                   AS SGUARCONTRACTID       --源担保合同ID
                ,T1.SERIALNO                                                                   AS SCOLLATERALID         --源抵质押品ID
                ,'保证金'                                                                      AS COLLATERALNAME        --抵质押品名称
                ,''                                                                            AS ISSUERID              --发行人ID
                ,T1.CUSTOMERID                                                                 AS PROVIDERID            --提供人ID
                ,T2.CREDITRISKDATATYPE                                                         AS CREDITRISKDATATYPE    --信用风险数据类型
                ,'060'                                                                         AS GUARANTEEWAY          --担保方式
                ,'001001'                                                                      AS SOURCECOLTYPE         --源抵质押品大类
                ,'001001003001'                                                                AS SOURCECOLSUBTYPE      --源抵质押品小类
                ,'0'                                                                           AS SPECPURPBONDFLAG      --是否为收购国有银行不良贷款而发行的债券
                ,'1'                                                                           AS QUALFLAGSTD           --权重法合格标识
                ,'1'                                                                           AS QUALFLAGFIRB          --内评初级法合格标识
                ,'030103'                                                                      AS COLLATERALTYPESTD     --权重法抵质押品类型
                ,'01'                                                                          AS COLLATERALSDVSSTD     --权重法抵质押品细分
                ,'030201'                                                                      AS COLLATERALTYPEIRB     --内评法抵质押品类型
                ,T3.BAILBALANCE                                                                AS COLLATERALAMOUNT     --抵押总额                              -
                ,NVL(T3.BAILCURRENCY,'CNY')                                                    AS CURRENCY              --币种
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
                ,'01'                                                                          AS FCTYPE                --金融质押品类型
                ,'0'                                                                           AS ABSFLAG               --资产证券化标识
                ,''                                                                            AS RATINGDURATIONTYPE    --评级期限类型
                ,''                                                                            AS FCISSUERATING         --金融质押品发行等级
                ,NULL                                                                          AS FCISSUERTYPE          --金融质押品发行人类别
                ,'01'                                                                            AS FCISSUERSTATE         --金融质押品发行人注册国家
                ,''                                                                            AS FCRESIDUALM           --金融质押品剩余期限
                ,1                                                                             AS REVAFREQUENCY         --重估频率
                ,''                                                                            AS GROUPID               --分组编号
                ,NULL                                                                          AS RCERating             --发行人境外注册地外部评级
                ,'BZJ|PT'
    FROM  RWA_DEV.NCM_BUSINESS_CONTRACT T1
    INNER JOIN TEMP_COLLATERAL3 T2
    ON T1.SERIALNO=T2.CONTRACTNO
    INNER JOIN RWA_DEV.RWA_TEMP_BAIL1 T3
    ON T1.SERIALNO=T3.CONTRACTNO
    AND T3.BAILBALANCE<9999999999999     --排除脏数据
    --AND T3.ISMAX='1' --这个是一期的逻辑，这里关联的BAIL2表，改为关联BAIL1表不需要加这个标志  modify by yushuangjiang
    WHERE T1.DATANO=P_DATA_DT_STR
    ;
    COMMIT;
    
    /*2.2 插入保证金信息到抵质押品表(逾期贷款-微粒贷)*/  
    /*INSERT INTO RWA_DEV.RWA_XD_COLLATERAL(
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
                ,flag
    )WITH TEMP_COLLATERAL3 AS(SELECT T1.RELATIVESERIALNO2 AS CONTRACTNO,MIN(NVL(T1.PUTOUTDATE,CASE WHEN T1.BUSINESSTYPE IN ('10201010','10202010') THEN NVL(T3.OCCURDATE,T3.PUTOUTDATE) ELSE T3.PUTOUTDATE END)) AS PUTOUTDATE
                                     ,MAX(NVL(T1.ACTUALMATURITY,T3.MATURITY)) AS MATURITY,MIN(T6.CREDITRISKDATATYPE) AS CREDITRISKDATATYPE
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --排除额度类业务
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --对账以借据为准，所以关联合同时，不应该加合同的有效条件
                               AND T1.DATANO=T3.DATANO
                               --AND T3.BUSINESSTYPE || T3.BUSINESSNATURE <> '1030301005'   --排除外部转帖现
                               INNER JOIN RWA_DEV.NCM_BUSINESS_HISTORY T31
                               ON T1.SERIALNO=T31.SERIALNO
                               AND T31.DATANO=P_DATA_DT_STR
                               AND (T31.OVERDUEBALANCE+T31.DULLBALANCE+T31.BADBALANCE)>0   --取到逾期的记录
                                         \* rwa_dev.brd_loan_nor t31
                               ON t1.serialno = t31.crdt_acct_no
                               AND t31.datano = P_DATA_DT_STR
                               AND t31.sbjt_cd = '13100001' --微粒贷的逾期贷款*\
                               INNER JOIN RWA_DEV.RWA_XD_CONTRACT T6
    													 ON T1.RELATIVESERIALNO2 = T6.CONTRACTID
    													 AND T1.DATANO = T6.DATANO
                               WHERE  T1.DATANO=P_DATA_DT_STR
                               AND T1.BUSINESSTYPE='11103030'  --只取微粒贷业务
                               GROUP BY T1.RELATIVESERIALNO2
                             )
    SELECT
                TO_DATE(T1.DATANO,'YYYYMMDD')                                                  AS DATADATE              --数据日期
                ,T1.DATANO                                                                     AS DATANO                --数据流水号
                ,'HT'||T1.SERIALNO||T3.BAILCURRENCY                                            AS COLLATERALID          --抵质押品ID
                ,'XD'                                                                          AS SSYSID                --源系统ID
                ,T1.SERIALNO                                                                   AS SGUARCONTRACTID       --源担保合同ID
                ,T1.SERIALNO                                                                   AS SCOLLATERALID         --源抵质押品ID
                ,'保证金'                                                                      AS COLLATERALNAME        --抵质押品名称
                ,''                                                                            AS ISSUERID              --发行人ID
                ,T1.CUSTOMERID                                                                 AS PROVIDERID            --提供人ID
                ,T2.CREDITRISKDATATYPE                                                         AS CREDITRISKDATATYPE    --信用风险数据类型
                ,'060'                                                                         AS GUARANTEEWAY          --担保方式
                ,'001001'                                                                      AS SOURCECOLTYPE         --源抵质押品大类
                ,'001001003001'                                                                AS SOURCECOLSUBTYPE      --源抵质押品小类
                ,'0'                                                                           AS SPECPURPBONDFLAG      --是否为收购国有银行不良贷款而发行的债券
                ,'1'                                                                           AS QUALFLAGSTD           --权重法合格标识
                ,'1'                                                                           AS QUALFLAGFIRB          --内评初级法合格标识
                ,'030103'                                                                      AS COLLATERALTYPESTD     --权重法抵质押品类型
                ,'01'                                                                          AS COLLATERALSDVSSTD     --权重法抵质押品细分
                ,'030201'                                                                      AS COLLATERALTYPEIRB     --内评法抵质押品类型
                ,T3.BAILBALANCE                                                                AS COLLATERALAMOUNT     --抵押总额                              -
                ,NVL(T3.BAILCURRENCY,'CNY')                                                    AS CURRENCY              --币种
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
                ,'01'                                                                          AS FCTYPE                --金融质押品类型
                ,'0'                                                                           AS ABSFLAG               --资产证券化标识
                ,''                                                                            AS RATINGDURATIONTYPE    --评级期限类型
                ,''                                                                            AS FCISSUERATING         --金融质押品发行等级
                ,NULL                                                                          AS FCISSUERTYPE          --金融质押品发行人类别
                ,'01'                                                                            AS FCISSUERSTATE         --金融质押品发行人注册国家
                ,''                                                                            AS FCRESIDUALM           --金融质押品剩余期限
                ,1                                                                             AS REVAFREQUENCY         --重估频率
                ,''                                                                            AS GROUPID               --分组编号
                ,NULL                                                                          AS RCERating             --发行人境外注册地外部评级
                ,'BZJ|YQWLD'
    FROM  RWA_DEV.NCM_BUSINESS_CONTRACT T1
    INNER JOIN TEMP_COLLATERAL3 T2
    ON T1.SERIALNO=T2.CONTRACTNO
    INNER JOIN RWA_DEV.RWA_TEMP_BAIL1 T3
    ON T1.SERIALNO=T3.CONTRACTNO
    --AND T3.ISMAX='1'    --这个是一期的逻辑，这里关联的BAIL2表，改为关联BAIL1表不需要加这个标志  modify by yushuangjiang
    WHERE T1.DATANO=P_DATA_DT_STR
    AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_XD_COLLATERAL T6 WHERE 'HT'||T1.SERIALNO||T3.BAILCURRENCY=T6.COLLATERALID)
    ;
    COMMIT;*/
    
    /*2.3 插入保证金信息到抵质押品表-逾期其他业务*/
    INSERT INTO RWA_DEV.RWA_XD_COLLATERAL(
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
                ,flag
    )WITH TEMP_COLLATERAL3 AS(SELECT T3.SERIALNO AS CONTRACTNO
                                    ,MIN(CASE WHEN T1.BUSINESSTYPE IN ('10201010','10202010','1035101010','1035101020','1035102010','1035102020','103550','1040101040') --信用证,保函
                                              THEN NVL(T3.PUTOUTDATE,T3.OCCURDATE)   --信用证保函，取合同起始日期
                                              ELSE T1.PUTOUTDATE 
                                         END) AS PUTOUTDATE
                                    ,MAX(CASE WHEN T1.BUSINESSTYPE IN ('10201010','10202010','1035101010','1035101020','1035102010','1035102020','103550','1040101040') --信用证,保函
                                              THEN T3.MATURITY
                                              ELSE T1.ACTUALMATURITY
                                         END) AS MATURITY
                                    ,MIN(T6.CREDITRISKDATATYPE) AS CREDITRISKDATATYPE
                               FROM RWA_DEV.NCM_BUSINESS_DUEBILL T1
                               INNER JOIN RWA_DEV.NCM_BUSINESS_TYPE T2
                               ON T1.BUSINESSTYPE=T2.TYPENO
                               AND T1.DATANO=T2.DATANO
                               AND T2.SORTNO NOT LIKE '30%' --排除额度类业务
                               INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T3
                               ON T1.RELATIVESERIALNO2=T3.SERIALNO        --对账以借据为准，所以关联合同时，不应该加合同的有效条件
                               AND T1.DATANO=T3.DATANO
                               INNER JOIN BRD_LOAN_NOR T4 --正常贷款
                               ON  T4.DATANO = p_data_dt_str
                               AND T4.CRDT_ACCT_NO = T1.SERIALNO
                               AND substr(t4.sbjt_cd,1,4) = '1310' --科目编号
                               AND T4.SBJT_CD != '13100001' --所有不含微粒贷的逾期贷款  
                               INNER JOIN RWA_DEV.RWA_XD_CONTRACT T6
                               ON T1.RELATIVESERIALNO2 = T6.CONTRACTID
                               AND T1.DATANO = T6.DATANO
                               WHERE  T1.DATANO=P_DATA_DT_STR
                               GROUP BY T3.SERIALNO
                             )
    SELECT
                TO_DATE(T1.DATANO,'YYYYMMDD')                                                  AS DATADATE              --数据日期
                ,T1.DATANO                                                                     AS DATANO                --数据流水号
                ,'HT'||T1.SERIALNO||T3.BAILCURRENCY                                            AS COLLATERALID          --抵质押品ID
                ,'XD'                                                                          AS SSYSID                --源系统ID
                ,T1.SERIALNO                                                                   AS SGUARCONTRACTID       --源担保合同ID
                ,T1.SERIALNO                                                                   AS SCOLLATERALID         --源抵质押品ID
                ,'保证金'                                                                      AS COLLATERALNAME        --抵质押品名称
                ,''                                                                            AS ISSUERID              --发行人ID
                ,T1.CUSTOMERID                                                                 AS PROVIDERID            --提供人ID
                ,T2.CREDITRISKDATATYPE                                                         AS CREDITRISKDATATYPE    --信用风险数据类型
                ,'060'                                                                         AS GUARANTEEWAY          --担保方式
                ,'001001'                                                                      AS SOURCECOLTYPE         --源抵质押品大类
                ,'001001003001'                                                                AS SOURCECOLSUBTYPE      --源抵质押品小类
                ,'0'                                                                           AS SPECPURPBONDFLAG      --是否为收购国有银行不良贷款而发行的债券
                ,'1'                                                                           AS QUALFLAGSTD           --权重法合格标识
                ,'1'                                                                           AS QUALFLAGFIRB          --内评初级法合格标识
                ,'030103'                                                                      AS COLLATERALTYPESTD     --权重法抵质押品类型
                ,'01'                                                                          AS COLLATERALSDVSSTD     --权重法抵质押品细分
                ,'030201'                                                                      AS COLLATERALTYPEIRB     --内评法抵质押品类型
                ,T3.BAILBALANCE                                                                AS COLLATERALAMOUNT     --抵押总额                              -
                ,NVL(T3.BAILCURRENCY,'CNY')                                                    AS CURRENCY              --币种
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
                ,'01'                                                                          AS FCTYPE                --金融质押品类型
                ,'0'                                                                           AS ABSFLAG               --资产证券化标识
                ,''                                                                            AS RATINGDURATIONTYPE    --评级期限类型
                ,''                                                                            AS FCISSUERATING         --金融质押品发行等级
                ,NULL                                                                          AS FCISSUERTYPE          --金融质押品发行人类别
                ,'01'                                                                            AS FCISSUERSTATE         --金融质押品发行人注册国家
                ,''                                                                            AS FCRESIDUALM           --金融质押品剩余期限
                ,1                                                                             AS REVAFREQUENCY         --重估频率
                ,''                                                                            AS GROUPID               --分组编号
                ,NULL                                                                          AS RCERating             --发行人境外注册地外部评级
                ,'BZJ|YQ'
    FROM  RWA_DEV.NCM_BUSINESS_CONTRACT T1
    INNER JOIN TEMP_COLLATERAL3 T2
    ON T1.SERIALNO=T2.CONTRACTNO
    INNER JOIN RWA_DEV.RWA_TEMP_BAIL1 T3
    ON T1.SERIALNO=T3.CONTRACTNO
    AND T3.BAILBALANCE<9999999999999
    --AND T3.ISMAX='1'  --这个是一期的逻辑，这里关联的BAIL2表，改为关联BAIL1表不需要加这个标志  modify by yushuangjiang
    WHERE T1.DATANO=P_DATA_DT_STR
    AND NOT EXISTS(SELECT 1 FROM RWA_DEV.RWA_XD_COLLATERAL T6 WHERE 'HT'||T1.SERIALNO||T3.BAILCURRENCY=T6.COLLATERALID)
    ;
    COMMIT;
    
    --更新暴露表的贷款价值比,要用到抵质押品总额，所以放在这里更新
    UPDATE RWA_DEV.RWA_XD_EXPOSURE T1
       SET LTV=(SELECT T1.ASSETBALANCE/T4.CollateralAmount
                FROM ( SELECT T2.CONTRACTID AS ,SUM(T3.CollateralAmount) AS CollateralAmount
                       FROM RWA_DEV.RWA_XD_CMRELEVENCE T2
                       INNER JOIN RWA_DEV.RWA_XD_COLLATERAL T3
                       ON T2.MITIGATIONID=T3.COLLATERALID
                       AND T2.DATANO=p_data_dt_str
                       AND T2.DATANO=T3.DATANO
                       GROUP BY T2.CONTRACTID
                     )T4
                WHERE T1.CONTRACTID=T4.CONTRACTID
                AND T4.CollateralAmount<>0)
    WHERE T1.BUSINESSTYPEID='11103040'
    AND T1.DATANO=p_data_dt_str;
    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_XD_COLLATERAL',cascade => true);

    /*目标表数据统计*/
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_XD_COLLATERAL;

    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功'||'-'||v_count1;
    --定义异常
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '信贷系统-抵质押品(pro_rwa_xd_collateral)ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;

END PRO_RWA_XD_COLLATERAL;
/

