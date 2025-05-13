CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_CLIENT(
                             p_data_dt_str  IN  VARCHAR2,    --数据日期
                             p_po_rtncode  OUT  VARCHAR2,    --返回编号 1 成功,0 失败
                            p_po_rtnmsg    OUT  VARCHAR2    --返回描述
        )
  /*
    存储过程名称:RWA_DEV.PRO_RWA_EI_CLIENT
    实现功能:汇总主体表,插入所有主体表信息
    数据口径:全量
    跑批频率:月初
    版  本  :V1.0.0
    编写人  :SHUXD
    编写时间:2016-07-05
    单  位   :上海安硕信息技术股份有限公司
    源  表1 :RWA.RWA_WP_COUNTRYRATING|国家评级表
    源  表2 :RWA_DEV.NCM_CUSTOMER_INFO|客户表
    源  表3 :RWA_DEV.RWA_EI_EXPOSURE|信用风险暴露表
    源  表4 :RWA_DEV.RWA_EI_CONTRACT|合同表
    源  表5 :RWA_DEV.RWA_EI_GUARANTEE|保证表
    源  表6 :RWA_DEV.RWA_EI_COLLATERAL|抵质押品表
    源  表7 :RWA_DEV.BL_CUSTOMER_INFO|补录客户汇总表
    源  表8 :RWA.ORG_INFO|机构表
    源  表9 :RWA.CODE_LIBRARY|代码表
    源  表10:RWA.RWA_WP_SUBCOMPANY|子公司配置表
    源  表11:RWA_DEV.CCS_ACCT|人民币贷记帐户表
    源  表12:RWA.RWA_WS_ASSET|抵债资产补录表
    源  表13:RAW_DEV.RWA_XF_CLINET|消费金融参与主体信息表
    
    目标表  :RWA_DEV.RWA_EI_CLIENT|参与主体汇总表
    辅助表  :无
    变更记录(修改人|修改时间|修改内容)
    xlpang  2019/05/29  新增消费金融参与主体客户信息到EI表  
    
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_EI_CLIENT';
  --定义判断值变量
  v_count INTEGER;
  --定义异常变量
  v_raise EXCEPTION;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    BEGIN
    --删除不存在的分区，如果此存储过程是第一次跑抛出异常，可以忽略
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_CLIENT DROP PARTITION CLIENT' || p_data_dt_str;

    COMMIT;

    EXCEPTION
      WHEN OTHERS THEN
        IF (SQLCODE <> '-2149') THEN
          --首次分区truncate会出现2149异常
         p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '汇总参与主体表('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
        END IF;
    END;

    --新增一个当前日期下的分区
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_CLIENT ADD PARTITION CLIENT' || p_data_dt_str || ' VALUES(TO_DATE('|| p_data_dt_str || ',''YYYYMMDD''))';

    COMMIT;
    --2.将满足条件的数据从源表插入到目标表中
    --2.1 把补录客户汇总表插入参与主体信息
    INSERT INTO RWA_DEV.RWA_EI_CLIENT(
                 DATADATE                                 --数据日期
                ,DATANO                                  --数据流水号
                ,CLIENTID                                --参与主体ID
                ,SOURCECLIENTID                          --源参与主体ID
                ,SSYSID                                  --源系统ID
                ,CLIENTNAME                              --参与主体名称
                ,SORGID                                  --源机构ID
                ,SORGNAME                                --源机构名称
                ,ORGSORTNO                               --所属机构排序号
                ,ORGID                                   --所属机构ID
                ,ORGNAME                                 --所属机构名称
                ,INDUSTRYID                              --所属行业代码
                ,INDUSTRYNAME                            --所属行业名称
                ,CLIENTTYPE                              --参与主体大类
                ,CLIENTSUBTYPE                           --参与主体小类
                ,COUNTRYCODE                             --国家/地区代码
                ,REGISTSTATE                             --注册国家或地区
                ,RCERATING                               --境外注册地外部评级
                ,RCERAGENCY                              --境外注册地外部评级机构
                ,ORGANIZATIONCODE                        --组织机构代码
                ,CONSOLIDATEDSCFLAG                      --是否并表子公司
                ,SLCLIENTFLAG                            --专业贷款客户标识
                ,SLCLIENTTYPE                            --专业贷款客户类型
                ,EXPOCATEGORYIRB                         --内评法暴露类别
                ,DEFAULTFLAG                             --违约标识
                ,MODELIRATING                            --模型内部评级
                ,MODELPD                                 --模型违约概率
                ,IRATING                                 --内部评级
                ,PD                                      --违约概率
                ,CLIENTERATING                           --参与主体外部评级
                ,CCPFLAG                                 --中央交易对手标识
                ,QUALCCPFLAG                             --是否合格中央交易对手
                ,CLEARMEMBERFLAG                         --清算会员标识
                ,MSMBFLAG                                --工信部微小企业标识
                ,SSMBFLAG                                --标准小微企业标识
                ,SSMBFLAGSTD         										 --权重法标准小微企业标识
                ,ANNUALSALE                              --公司客户年销售额
                ,MODELID                                 --模型ID
                ,NEWDEFAULTFLAG                          --新增违约标识
                ,DEFAULTDATE                             --违约时点
                ,COMPANYSIZE                             --企业规模
    )
    WITH TMP_CUST_ERATING AS (
							    SELECT CUSTOMERID
							    			,CLIENTERATING
							  		FROM (SELECT T1.CUSTOMERID 	AS CUSTOMERID,
							                    T1.EVALUTEORG 	AS EVALUTEORG,
							                    T2.DESCRATING		AS CLIENTERATING,
							                    T1.EVALUTEDATE	AS EVALUTEDATE,
							                    RANK() OVER(PARTITION BY T1.CUSTOMERID ORDER BY T1.EVALUTEDATE DESC) AS RK,
							                    ROW_NUMBER() OVER(PARTITION BY T1.CUSTOMERID, T1.EVALUTEDATE ORDER BY T2.DESCRATING) AS RM,
							                    COUNT(1) OVER(PARTITION BY T1.CUSTOMERID, T1.EVALUTEDATE) AS RN
							               FROM RWA_DEV.NCM_CUSTOMER_RATING T1
							          LEFT JOIN RWA_DEV.RWA_CD_RATING_MAPPING T2
							                 ON T1.EVALUTEORG = T2.SRCRATINGORG
							                AND T1.EVALUTELEVEL = T2.SRCRATINGNAME
							                AND T2.MAPPINGTYPE = '01' --全量类型
							                AND T2.SRCRATINGTYPE = '01' --长期评级
							              WHERE T1.EVALUTEDATE <= P_DATA_DT_STR
							              	AND T1.DATANO = P_DATA_DT_STR)
								 WHERE RK = 1
								   AND RM = (CASE WHEN RN = 1 THEN 1 ELSE 2 END)							--取最新的一笔评级，如果最近一天存在多笔评级，取第二好的评级
   	)
   	, TMP_CUST_IRATING AS (
   								SELECT CUSTID,
									       CUSTNAME,
									       ORGCERTCODE,
									       MODELID,
									       PDCODE,
									       PDLEVEL,
									       PDADJCODE,
									       PDADJLEVEL,
									       PD,
									       PDVAVLIDDATE
									  FROM RWA_DEV.RWA_TEMP_PDLEVEL
									 WHERE ROWID IN
									       (SELECT MAX(ROWID) FROM RWA_DEV.RWA_TEMP_PDLEVEL GROUP BY ORGCERTCODE)
   	)
     SELECT
                TO_DATE(T1.DATANO,'YYYYMMDD')                           AS DATADATE            --数据日期
                ,T1.DATANO                                              AS DATANO              --数据流水号
                ,T1.CUSTOMERID                                          AS CLIENTID            --参与主体ID
                ,T1.CUSTOMERID                                          AS SOURCECLIENTID      --源参与主体ID
                ,'BL'                                                   AS SSYSID              --源系统ID
                ,T1.CUSTOMERNAME                                        AS CLIENTNAME          --参与主体名称
                ,T1.ORGID                                               AS SORGID              --源机构ID
                ,T4.ORGNAME                                             AS SORGNAME            --源机构名称
                ,T4.SORTNO                                              AS ORGSORTNO           --所属机构排序号
                ,T1.ORGID                                               AS ORGID               --所属机构ID
                ,T4.ORGNAME                                             AS ORGNAME             --所属机构名称
                ,T1.INDUSTRYTYPE                                        AS INDUSTRYID          --所属行业代码
                ,T5.ITEMNAME                                            AS INDUSTRYNAME        --所属行业名称
                ,CASE WHEN T1.CUSTOMERCATEGORY IS NOT NULL THEN SUBSTR(T1.CUSTOMERCATEGORY,1,2)
                      ELSE ''
                 END                                                    AS CLIENTTYPE          --参与主体大类
                ,T1.CUSTOMERCATEGORY                                    AS CLIENTSUBTYPE       --参与主体小类
                ,CASE WHEN  (T1.COUNTRYCODE IS NULL OR T1.COUNTRYCODE='OTH')
                      THEN  'CHN'
                      ELSE  T1.COUNTRYCODE
                 END                                                    AS COUNTRYCODE         --国家/地区代码
                ,CASE WHEN (T1.COUNTRYCODE IS NULL OR T1.COUNTRYCODE='OTH' OR T1.COUNTRYCODE='CHN') THEN '01'
                      ELSE '02'
                 END                                                    AS REGISTSTATE         --注册国家或地区
                ,CASE WHEN (T1.COUNTRYCODE IS NULL OR T1.COUNTRYCODE='OTH' OR T1.COUNTRYCODE='CHN') THEN ''
                      ELSE NVL(T3.RATINGRESULT,'0124')
                 END                                                    AS RCERATING           --境外注册地外部评级
                ,CASE WHEN (T1.COUNTRYCODE IS NULL OR T1.COUNTRYCODE='OTH' OR T1.COUNTRYCODE='CHN') THEN ''
                      ELSE NVL(T3.RATINGORG,'01')
                 END                                                    AS RCERAGENCY          --境外注册地外部评级机构
                ,T1.CERTID                                              AS ORGANIZATIONCODE    --组织机构代码
                ,CASE WHEN T6.ORGANIZATIONCODE IS NOT NULL THEN '1'
                      ELSE '0'
                 END                                                    AS CONSOLIDATEDSCFLAG  --是否并表子公司
                ,''                                                     AS SLCLIENTFLAG        --专业贷款客户标识
                ,''                                                     AS SLCLIENTTYPE        --专业贷款客户类型
                ,''                                                     AS EXPOCATEGORYIRB     --内评法暴露类别
                ,CASE WHEN NVL(T7.PDADJCODE,T14.PDADJCODE) = 'D' THEN '1'
                      ELSE '0'
                 END                                                    AS DEFAULTFLAG         --违约标识
                ,NVL(T7.PDLEVEL,T14.PDLEVEL)	                          AS MODELIRATING        --模型内部评级
                ,NVL(T7.PD,T14.PD)		                                  AS MODELPD             --模型违约概率
                ,NVL(T7.PDADJLEVEL,T14.PDADJLEVEL)                      AS IRATING             --内部评级
                ,NVL(T7.PD,T14.PD)		                                  AS PD                  --违约概率
                ,CASE WHEN T1.ERATING IS NULL THEN T12.CLIENTERATING
                 ELSE RWA_DEV.GETSTANDARDRATING1(T1.ERATING)
                 END																	                  AS CLIENTERATING       --参与主体外部评级
                ,'0'                                                    AS CCPFLAG             --中央交易对手标识
                ,'0'                                                    AS QUALCCPFLAG         --是否合格中央交易对手
                ,'0'                                                    AS CLEARMEMBERFLAG     --清算会员标识
                ,CASE WHEN T1.SCOPE IN ('4','5','02','03')
                      THEN '1'
                      ELSE '0'
                 END                                                    AS MSMBFLAG            --工信部微小企业标识
                ,'0'                                                    AS SSMBFLAG            --标准小微企业标识
                ,'0'                                                    AS SSMBFLAGSTD         --权重法标准小微企业标识
                ,T9.AVEFINANCESUM	                                      AS ANNUALSALE          --公司客户年销售额
                ,NVL(T7.MODELID,T14.MODELID)                      			AS MODELID             --模型ID
                ,DECODE(NVL(T9.NEWDEFAULTFLAG,'1'),'0','1','0')         AS NEWDEFAULTFLAG      --新增违约标识
                ,CASE WHEN NVL(T7.PDADJCODE,T14.PDADJCODE) = 'D' THEN TO_DATE(NVL(T7.PDVAVLIDDATE,T14.PDVAVLIDDATE),'YYYYMMDD')
                 ELSE NULL
                 END					                                        	AS DEFAULTDATE         --违约时点
                ,CASE WHEN T1.SCOPE = '2' THEN '00'																						 --大型企业
                			WHEN T1.SCOPE = '3' THEN '01'																						 --中型企业
                			WHEN T1.SCOPE = '4' THEN '02'																						 --小型企业
                			WHEN T1.SCOPE = '5' THEN '03'																						 --微型企业
                			ELSE NVL(T1.SCOPE,'01')																									 --默认中型企业
                 END		                                                AS COMPANYSIZE         --企业规模
    FROM        RWA_DEV.BL_CUSTOMER_INFO T1
    LEFT JOIN   RWA.RWA_WP_COUNTRYRATING T3
    ON          T1.COUNTRYCODE = T3.COUNTRYCODE
    LEFT JOIN   RWA.ORG_INFO T4
    ON          T1.ORGID = T4.ORGID
    LEFT JOIN   RWA.CODE_LIBRARY T5
    ON          T1.INDUSTRYTYPE = T5.ITEMNO
    AND         T5.CODENO = 'IndustryType'
    LEFT JOIN   RWA.RWA_WP_SUBCOMPANY T6
    ON          REPLACE(T1.CERTID,'-') = REPLACE(T6.ORGANIZATIONCODE,'-')
    AND					T1.CERTTYPE IN ('Ent01','Ent02')
    LEFT JOIN		RWA_DEV.RWA_TEMP_PDLEVEL T7
	  ON					T1.CUSTOMERID = T7.CUSTID
	  LEFT JOIN		RWA_DEV.NCM_CUSTOMER_INFO T9
	  ON					T1.CUSTOMERID = T9.CUSTOMERID
	  AND					T9.DATANO = p_data_dt_str
	  LEFT JOIN		TMP_CUST_ERATING T12
	  ON					T1.CUSTOMERID = T12.CUSTOMERID
	  LEFT JOIN		TMP_CUST_IRATING T14
	  ON					REPLACE(T1.CERTID,'-','') = REPLACE(T14.ORGCERTCODE,'-','')
	  AND					T1.CERTTYPE IN ('Ent01','Ent02')
    WHERE       T1.DATANO = P_DATA_DT_STR
    ;

    COMMIT;

    --插入投资客户到EI客户表
    INSERT INTO RWA_DEV.RWA_EI_CLIENT(
                 DATADATE                                 --数据日期
                ,DATANO                                  --数据流水号
                ,CLIENTID                                --参与主体ID
                ,SOURCECLIENTID                          --源参与主体ID
                ,SSYSID                                  --源系统ID
                ,CLIENTNAME                              --参与主体名称
                ,SORGID                                  --源机构ID
                ,SORGNAME                                --源机构名称
                ,ORGSORTNO                               --所属机构排序号
                ,ORGID                                   --所属机构ID
                ,ORGNAME                                 --所属机构名称
                ,INDUSTRYID                              --所属行业代码
                ,INDUSTRYNAME                            --所属行业名称
                ,CLIENTTYPE                              --参与主体大类
                ,CLIENTSUBTYPE                           --参与主体小类
                ,COUNTRYCODE                             --国家/地区代码
                ,REGISTSTATE                             --注册国家或地区
                ,RCERATING                               --境外注册地外部评级
                ,RCERAGENCY                              --境外注册地外部评级机构
                ,ORGANIZATIONCODE                        --组织机构代码
                ,CONSOLIDATEDSCFLAG                      --是否并表子公司
                ,SLCLIENTFLAG                            --专业贷款客户标识
                ,SLCLIENTTYPE                            --专业贷款客户类型
                ,EXPOCATEGORYIRB                         --内评法暴露类别
                ,DEFAULTFLAG                             --违约标识
                ,MODELIRATING                            --模型内部评级
                ,MODELPD                                 --模型违约概率
                ,IRATING                                 --内部评级
                ,PD                                      --违约概率
                ,CLIENTERATING                           --参与主体外部评级
                ,CCPFLAG                                 --中央交易对手标识
                ,QUALCCPFLAG                             --是否合格中央交易对手
                ,CLEARMEMBERFLAG                         --清算会员标识
                ,MSMBFLAG                                --工信部微小企业标识
                ,SSMBFLAG                                --标准小微企业标识
                ,SSMBFLAGSTD         										 --权重法标准小微企业标识
                ,ANNUALSALE                              --公司客户年销售额
                ,MODELID                                 --模型ID
                ,NEWDEFAULTFLAG                          --新增违约标识
                ,DEFAULTDATE                             --违约时点
                ,COMPANYSIZE                             --企业规模
     )
     SELECT
                 T1.DATADATE                                 --数据日期
                ,T1.DATANO                                  --数据流水号
                ,T1.CLIENTID                                --参与主体ID
                ,T1.SOURCECLIENTID                          --源参与主体ID
                ,T1.SSYSID                                  --源系统ID
                ,T1.CLIENTNAME                              --参与主体名称
                ,T1.SORGID                                  --源机构ID
                ,T1.SORGNAME                                --源机构名称
                ,T1.ORGSORTNO                               --所属机构排序号
                ,T1.ORGID                                   --所属机构ID
                ,T1.ORGNAME                                 --所属机构名称
                ,T1.INDUSTRYID                              --所属行业代码
                ,T1.INDUSTRYNAME                            --所属行业名称
                ,T1.CLIENTTYPE                              --参与主体大类
                ,T1.CLIENTSUBTYPE                           --参与主体小类
                ,T1.COUNTRYCODE                             --国家/地区代码
                ,T1.REGISTSTATE                             --注册国家或地区
                ,T1.RCERATING                               --境外注册地外部评级
                ,T1.RCERAGENCY                              --境外注册地外部评级机构
                ,T1.ORGANIZATIONCODE                        --组织机构代码
                ,T1.CONSOLIDATEDSCFLAG                      --是否并表子公司
                ,T1.SLCLIENTFLAG                            --专业贷款客户标识
                ,T1.SLCLIENTTYPE                            --专业贷款客户类型
                ,T1.EXPOCATEGORYIRB                         --内评法暴露类别
                ,T1.DEFAULTFLAG                             --违约标识
                ,T1.MODELIRATING                            --模型内部评级
                ,T1.MODELPD                                 --模型违约概率
                ,T1.IRATING                                 --内部评级
                ,T1.PD                                      --违约概率
                ,T1.CLIENTERATING                           --参与主体外部评级
                ,T1.CCPFLAG                                 --中央交易对手标识
                ,T1.QUALCCPFLAG                             --是否合格中央交易对手
                ,T1.CLEARMEMBERFLAG                         --清算会员标识
                ,T1.MSMBFLAG                                --工信部微小企业标识
                ,T1.SSMBFLAG                                --标准小微企业标识
                ,T1.SSMBFLAGSTD                             --权重法标准小微企业标识
                ,T1.ANNUALSALE                              --公司客户年销售额
                ,T1.MODELID                                 --模型ID
                ,T1.NEWDEFAULTFLAG                          --新增违约标识
                ,T1.DEFAULTDATE                             --违约时点
                ,NVL(T1.COMPANYSIZE,'01')                   --企业规模 默认中型企业
    FROM RWA_DEV.RWA_TZ_CLIENT T1
    WHERE       T1.DATANO = P_DATA_DT_STR
    AND NOT EXISTS (SELECT 1 FROM RWA_EI_CLIENT T6 WHERE T1.CLIENTID = T6.CLIENTID AND T6.DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD'))
    ;

    COMMIT;

    --2.2 依据集市客户表插入信用风险暴露(排除信用卡系统的，信用卡客户后面单独处理)的参与主体信息
    INSERT INTO RWA_DEV.RWA_EI_CLIENT(
                 DATADATE                                 --数据日期
                ,DATANO                                  --数据流水号
                ,CLIENTID                                --参与主体ID
                ,SOURCECLIENTID                          --源参与主体ID
                ,SSYSID                                  --源系统ID
                ,CLIENTNAME                              --参与主体名称
                ,SORGID                                  --源机构ID
                ,SORGNAME                                --源机构名称
                ,ORGSORTNO                               --所属机构排序号
                ,ORGID                                   --所属机构ID
                ,ORGNAME                                 --所属机构名称
                ,INDUSTRYID                              --所属行业代码
                ,INDUSTRYNAME                            --所属行业名称
                ,CLIENTTYPE                              --参与主体大类
                ,CLIENTSUBTYPE                           --参与主体小类
                ,COUNTRYCODE                             --国家/地区代码
                ,REGISTSTATE                             --注册国家或地区
                ,RCERATING                               --境外注册地外部评级
                ,RCERAGENCY                              --境外注册地外部评级机构
                ,ORGANIZATIONCODE                        --组织机构代码
                ,CONSOLIDATEDSCFLAG                      --是否并表子公司
                ,SLCLIENTFLAG                            --专业贷款客户标识
                ,SLCLIENTTYPE                            --专业贷款客户类型
                ,EXPOCATEGORYIRB                         --内评法暴露类别
                ,DEFAULTFLAG                             --违约标识
                ,MODELIRATING                            --模型内部评级
                ,MODELPD                                 --模型违约概率
                ,IRATING                                 --内部评级
                ,PD                                      --违约概率
                ,CLIENTERATING                           --参与主体外部评级
                ,CCPFLAG                                 --中央交易对手标识
                ,QUALCCPFLAG                             --是否合格中央交易对手
                ,CLEARMEMBERFLAG                         --清算会员标识
                ,MSMBFLAG                                --工信部微小企业标识
                ,SSMBFLAG                                --标准小微企业标识
                ,SSMBFLAGSTD         										 --权重法标准小微企业标识
                ,ANNUALSALE                              --公司客户年销售额
                ,MODELID                                 --模型ID
                ,NEWDEFAULTFLAG                          --新增违约标识
                ,DEFAULTDATE                             --违约时点
                ,COMPANYSIZE                             --企业规模
    )
    WITH TMP_CUST_ERATING AS (
							    SELECT CUSTOMERID
							    			,CLIENTERATING
							  		FROM (SELECT T1.CUSTOMERID 	AS CUSTOMERID,
							                    T1.EVALUTEORG 	AS EVALUTEORG,
							                    T2.DESCRATING		AS CLIENTERATING,
							                    T1.EVALUTEDATE	AS EVALUTEDATE,
							                    RANK() OVER(PARTITION BY T1.CUSTOMERID ORDER BY T1.EVALUTEDATE DESC) AS RK,
							                    ROW_NUMBER() OVER(PARTITION BY T1.CUSTOMERID, T1.EVALUTEDATE ORDER BY T2.DESCRATING) AS RM,
							                    COUNT(1) OVER(PARTITION BY T1.CUSTOMERID, T1.EVALUTEDATE) AS RN
							               FROM RWA_DEV.NCM_CUSTOMER_RATING T1
							          LEFT JOIN RWA_DEV.RWA_CD_RATING_MAPPING T2
							                 ON T1.EVALUTEORG = T2.SRCRATINGORG
							                AND T1.EVALUTELEVEL = T2.SRCRATINGNAME
							                AND T2.MAPPINGTYPE = '01' --全量类型
							                AND T2.SRCRATINGTYPE = '01' --长期评级
							              WHERE T1.EVALUTEDATE <= P_DATA_DT_STR
							              	AND T1.DATANO = P_DATA_DT_STR)
								 WHERE RK = 1
								   AND RM = (CASE WHEN RN = 1 THEN 1 ELSE 2 END)							--取最新的一笔评级，如果最近一天存在多笔评级，取第二好的评级
   	)
     SELECT
                TO_DATE(T1.DATANO,'YYYYMMDD')                           AS DATADATE            --数据日期
                ,T1.DATANO                                              AS DATANO              --数据流水号
                ,T1.CUSTOMERID                                          AS CLIENTID            --参与主体ID
                ,T1.CUSTOMERID                                          AS SOURCECLIENTID      --源参与主体ID
                ,'HX'                                                   AS SSYSID              --源系统ID
                ,T1.CUSTOMERNAME                                        AS CLIENTNAME          --参与主体名称
                ,T1.ORGID                                               AS SORGID              --源机构ID
                ,T4.ORGNAME                                             AS SORGNAME            --源机构名称
                ,T4.SORTNO                                              AS ORGSORTNO           --所属机构排序号
                --,T1.ORGID                                               AS ORGID               --所属机构ID
                ,DECODE(SUBSTR(T1.ORGID,1,1),'@','01000000',T1.ORGID)
                --,T4.ORGNAME                                             AS ORGNAME             --所属机构名称
                ,NVL(T4.ORGNAME,'总行')
                ,T1.INDUSTRYTYPE                                        AS INDUSTRYID          --所属行业代码
                ,T5.ITEMNAME                                            AS INDUSTRYNAME        --所属行业名称
                ,''/*SUBSTR(T1.RWACUSTOMERTYPE,1,2)*/                         AS CLIENTTYPE          --参与主体大类
                ,''/*T1.RWACUSTOMERTYPE */                                    AS CLIENTSUBTYPE       --参与主体小类
                ,CASE WHEN  (T1.COUNTRYCODE IS NULL OR T1.COUNTRYCODE='OTH')
                      THEN  'CHN'
                      ELSE  T1.COUNTRYCODE
                 END                                                    AS COUNTRYCODE         --国家/地区代码
                ,CASE WHEN (T1.COUNTRYCODE IS NULL OR T1.COUNTRYCODE='OTH' OR T1.COUNTRYCODE='CHN') THEN '01'
                      ELSE '02'
                 END                                                    AS REGISTSTATE         --注册国家或地区
                ,CASE WHEN (T1.COUNTRYCODE IS NULL OR T1.COUNTRYCODE='OTH' OR T1.COUNTRYCODE='CHN') THEN ''
                      ELSE NVL(T3.RATINGRESULT,'0124')
                 END                                                    AS RCERATING           --境外注册地外部评级
                ,CASE WHEN (T1.COUNTRYCODE IS NULL OR T1.COUNTRYCODE='OTH' OR T1.COUNTRYCODE='CHN') THEN ''
                      ELSE NVL(T3.RATINGORG,'01')
                 END                                                    AS RCERAGENCY          --境外注册地外部评级机构
                ,CASE WHEN T1.CUSTOMERTYPE='0321000001'
                      THEN ''
                      ELSE T1.CERTID
                 END                                                    AS ORGANIZATIONCODE    --组织机构代码
                ,CASE WHEN T6.ORGANIZATIONCODE IS NOT NULL THEN '1'
                      ELSE '0'
                 END                                                    AS CONSOLIDATEDSCFLAG  --是否并表子公司
                ,'0'                                                    AS SLCLIENTFLAG        --专业贷款客户标识
                ,''                                                     AS SLCLIENTTYPE        --专业贷款客户类型
                ,''                                                     AS EXPOCATEGORYIRB     --内评法暴露类别
                ,CASE WHEN T7.PDADJCODE = 'D' THEN '1'
                      ELSE '0'
                 END                                                    AS DEFAULTFLAG         --违约标识
                ,T7.PDLEVEL	                          									AS MODELIRATING        --模型内部评级
                ,T7.PD		                        						          AS MODELPD             --模型违约概率
                ,T7.PDADJLEVEL										                      AS IRATING             --内部评级
                ,T7.PD								                                  AS PD                  --违约概率
                ,T12.CLIENTERATING							                        AS CLIENTERATING       --参与主体外部评级
                ,'0'                                                    AS CCPFLAG             --中央交易对手标识
                ,'0'                                                    AS QUALCCPFLAG         --是否合格中央交易对手
                ,'0'                                                    AS CLEARMEMBERFLAG     --清算会员标识
                ,CASE WHEN T1.SCOPE IN ('02','03')
                      THEN '1'
                      ELSE '0'
                 END                                                    AS MSMBFLAG            --工信部微小企业标识
                ,NVL(T1.ISSUPERVISESTANDARSMENT,'0')                    AS SSMBFLAG            --标准小微企业标识
                ,'0'                                                    AS SSMBFLAGSTD         --权重法标准小微企业标识
                ,T1.AVEFINANCESUM                                       AS ANNUALSALE          --公司客户年销售额
                ,T7.MODELID									                      			AS MODELID             --模型ID
                ,DECODE(NVL(T1.NEWDEFAULTFLAG,'1'),'0','1','0')         AS NEWDEFAULTFLAG      --新增违约标识
                ,CASE WHEN T7.PDADJCODE = 'D' THEN TO_DATE(T7.PDVAVLIDDATE,'YYYYMMDD')
                 ELSE NULL
                 END					                                        	AS DEFAULTDATE         --违约时点
                ,CASE WHEN T1.SCOPE = '2' THEN '00'                                             --大型企业
                      WHEN T1.SCOPE = '3' THEN '01'                                            --中型企业
                      WHEN T1.SCOPE = '4' THEN '02'                                            --小型企业
                      WHEN T1.SCOPE = '5' THEN '03'                                            --微型企业
                      ELSE NVL(T1.SCOPE,'01')                                                  --默认中型企业
                 END                                                    AS COMPANYSIZE         --企业规模
    FROM        RWA_DEV.NCM_CUSTOMER_INFO T1
    INNER JOIN   (SELECT DISTINCT CLIENTID
                        -- ,SUM(NORMALPRINCIPAL) AS BALANCE
                  FROM RWA_DEV.RWA_EI_EXPOSURE
                  WHERE DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')
                  AND   SSYSID NOT IN('XYK','LC','DZ')
                  AND ACCSUBJECT1<>'13010511'      --排除内部转帖现，内部转帖现后面单独处理
                  ) T2
    ON           T1.CUSTOMERID = T2.CLIENTID
    LEFT JOIN   RWA.RWA_WP_COUNTRYRATING T3
    ON          T1.COUNTRYCODE = T3.COUNTRYCODE
    LEFT JOIN   RWA.ORG_INFO T4
    ON          T1.ORGID = T4.ORGID
    LEFT JOIN   RWA.CODE_LIBRARY T5
    ON          T1.INDUSTRYTYPE = T5.ITEMNO
    AND         T5.CODENO = 'IndustryType'
    LEFT JOIN   RWA.RWA_WP_SUBCOMPANY T6
    ON          T1.CERTID=T6.ORGANIZATIONCODE
    LEFT JOIN   RWA_DEV.RWA_TEMP_PDLEVEL T7
    ON          T1.CUSTOMERID=T7.CUSTID
    LEFT JOIN		TMP_CUST_ERATING T12
	  ON					T1.CUSTOMERID = T12.CUSTOMERID
    WHERE       T1.DATANO = P_DATA_DT_STR
    AND NOT EXISTS (SELECT 1 FROM RWA_EI_CLIENT T6 WHERE T1.CUSTOMERID = T6.CLIENTID AND T6.DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD'))
    ;

    COMMIT;
    
     --2.2 依据集市客户表插入信用风险暴露(内部转帖现)的参与主体信息
    INSERT INTO RWA_DEV.RWA_EI_CLIENT(
                 DATADATE                                 --数据日期
                ,DATANO                                  --数据流水号
                ,CLIENTID                                --参与主体ID
                ,SOURCECLIENTID                          --源参与主体ID
                ,SSYSID                                  --源系统ID
                ,CLIENTNAME                              --参与主体名称
                ,SORGID                                  --源机构ID
                ,SORGNAME                                --源机构名称
                ,ORGSORTNO                               --所属机构排序号
                ,ORGID                                   --所属机构ID
                ,ORGNAME                                 --所属机构名称
                ,INDUSTRYID                              --所属行业代码
                ,INDUSTRYNAME                            --所属行业名称
                ,CLIENTTYPE                              --参与主体大类
                ,CLIENTSUBTYPE                           --参与主体小类
                ,COUNTRYCODE                             --国家/地区代码
                ,REGISTSTATE                             --注册国家或地区
                ,RCERATING                               --境外注册地外部评级
                ,RCERAGENCY                              --境外注册地外部评级机构
                ,ORGANIZATIONCODE                        --组织机构代码
                ,CONSOLIDATEDSCFLAG                      --是否并表子公司
                ,SLCLIENTFLAG                            --专业贷款客户标识
                ,SLCLIENTTYPE                            --专业贷款客户类型
                ,EXPOCATEGORYIRB                         --内评法暴露类别
                ,DEFAULTFLAG                             --违约标识
                ,MODELIRATING                            --模型内部评级
                ,MODELPD                                 --模型违约概率
                ,IRATING                                 --内部评级
                ,PD                                      --违约概率
                ,CLIENTERATING                           --参与主体外部评级
                ,CCPFLAG                                 --中央交易对手标识
                ,QUALCCPFLAG                             --是否合格中央交易对手
                ,CLEARMEMBERFLAG                         --清算会员标识
                ,MSMBFLAG                                --工信部微小企业标识
                ,SSMBFLAG                                --标准小微企业标识
                ,SSMBFLAGSTD                             --权重法标准小微企业标识
                ,ANNUALSALE                              --公司客户年销售额
                ,MODELID                                 --模型ID
                ,NEWDEFAULTFLAG                          --新增违约标识
                ,DEFAULTDATE                             --违约时点
                ,COMPANYSIZE                             --企业规模
    )
     SELECT
                TO_DATE(TEMP.DATANO,'YYYYMMDD')                           AS DATADATE            --数据日期
                ,TEMP.DATANO                                              AS DATANO              --数据流水号
                ,TEMP.CUSTNO                                              AS CLIENTID            --参与主体ID
                ,TEMP.CUSTNO                                              AS SOURCECLIENTID      --源参与主体ID
                ,'PJ'                                                     AS SSYSID              --源系统ID
                ,TEMP.CUSTNAME                                          AS CLIENTNAME          --参与主体名称
                ,'9998'                                             AS SORGID              --源机构ID
                ,'未知'                                                 AS SORGNAME            --源机构名称
                ,'1'                                        AS ORGSORTNO           --所属机构排序号
                ,'9998'                                             AS ORGID               --所属机构ID
                ,'未知'                                                 AS ORGNAME             --所属机构名称
                ,'999999'                                               AS INDUSTRYID          --所属行业代码
                ,'未知'                                                 AS INDUSTRYNAME        --所属行业名称
                ,''                                                     AS CLIENTTYPE          --参与主体大类
                ,''                                                     AS CLIENTSUBTYPE       --参与主体小类
                ,'CHN'                                                  AS COUNTRYCODE         --国家/地区代码
                ,'01'                                                   AS REGISTSTATE         --注册国家或地区
                ,''                                                     AS RCERATING           --境外注册地外部评级
                ,''                                                     AS RCERAGENCY          --境外注册地外部评级机构
                ,''                                                     AS ORGANIZATIONCODE    --组织机构代码
                ,'0'                                                    AS CONSOLIDATEDSCFLAG  --是否并表子公司
                ,''                                                     AS SLCLIENTFLAG        --专业贷款客户标识
                ,''                                                     AS SLCLIENTTYPE        --专业贷款客户类型
                ,''                                                     AS EXPOCATEGORYIRB     --内评法暴露类别
                ,'0'                                                    AS DEFAULTFLAG         --违约标识
                ,''                                                     AS MODELIRATING        --模型内部评级
                ,''                                                     AS MODELPD             --模型违约概率
                ,''                                                     AS IRATING             --内部评级
                ,''                                                     AS PD                  --违约概率
                ,''                                                     AS CLIENTERATING       --参与主体外部评级
                ,'0'                                                    AS CCPFLAG             --中央交易对手标识
                ,'0'                                                    AS QUALCCPFLAG         --是否合格中央交易对手
                ,'0'                                                    AS CLEARMEMBERFLAG     --清算会员标识
                ,'0'                                                    AS MSMBFLAG            --工信部微小企业标识
                ,'0'                                                    AS SSMBFLAG            --标准小微企业标识
                ,'0'                                                    AS SSMBFLAGSTD         --权重法标准小微企业标识
                ,NULL                                                   AS ANNUALSALE          --公司客户年销售额
                ,''                                                     AS MODELID             --模型ID
                ,'0'                                                    AS NEWDEFAULTFLAG      --新增违约标识
                ,''                                                     AS DEFAULTDATE         --违约时点
                ,'01'                                                   AS COMPANYSIZE         --企业规模 默认中型企业
    FROM
    (SELECT DISTINCT T1.DATANO, substr(T1.BILL_NO,2,12) AS CUSTNO,T2.UBANK_NAME AS CUSTNAME
    FROM BRD_BILL T1
    INNER JOIN ebs_union_bank T2
    ON substr(T1.BILL_NO,2,12)=T2.ubank_no
    AND T1.DATANO=T2.DATANO
    WHERE SUBSTR(T1.SBJT_CD, 1, 6)='130105'   --限制为内部转帖现
    AND ATL_PAY_AMT <> 0
    AND T1.DATANO=p_data_dt_str) TEMP 
    ;

    COMMIT;

    --2.2 依据集市客户表插入信用风险暴露(信用卡系统的，信用卡客户这里单独处理)的参与主体信息
    INSERT INTO RWA_DEV.RWA_EI_CLIENT(
                 DATADATE                                 --数据日期
                ,DATANO                                  --数据流水号
                ,CLIENTID                                --参与主体ID
                ,SOURCECLIENTID                          --源参与主体ID
                ,SSYSID                                  --源系统ID
                ,CLIENTNAME                              --参与主体名称
                ,SORGID                                  --源机构ID
                ,SORGNAME                                --源机构名称
                ,ORGSORTNO                               --所属机构排序号
                ,ORGID                                   --所属机构ID
                ,ORGNAME                                 --所属机构名称
                ,INDUSTRYID                              --所属行业代码
                ,INDUSTRYNAME                            --所属行业名称
                ,CLIENTTYPE                              --参与主体大类
                ,CLIENTSUBTYPE                           --参与主体小类
                ,COUNTRYCODE                             --国家/地区代码
                ,REGISTSTATE                             --注册国家或地区
                ,RCERATING                               --境外注册地外部评级
                ,RCERAGENCY                              --境外注册地外部评级机构
                ,ORGANIZATIONCODE                        --组织机构代码
                ,CONSOLIDATEDSCFLAG                      --是否并表子公司
                ,SLCLIENTFLAG                            --专业贷款客户标识
                ,SLCLIENTTYPE                            --专业贷款客户类型
                ,EXPOCATEGORYIRB                         --内评法暴露类别
                ,DEFAULTFLAG                             --违约标识
                ,MODELIRATING                            --模型内部评级
                ,MODELPD                                 --模型违约概率
                ,IRATING                                 --内部评级
                ,PD                                      --违约概率
                ,CLIENTERATING                           --参与主体外部评级
                ,CCPFLAG                                 --中央交易对手标识
                ,QUALCCPFLAG                             --是否合格中央交易对手
                ,CLEARMEMBERFLAG                         --清算会员标识
                ,MSMBFLAG                                --工信部微小企业标识
                ,SSMBFLAG                                --标准小微企业标识
                ,SSMBFLAGSTD         										 --权重法标准小微企业标识
                ,ANNUALSALE                              --公司客户年销售额
                ,MODELID                                 --模型ID
                ,NEWDEFAULTFLAG                          --新增违约标识
                ,DEFAULTDATE                             --违约时点
                ,COMPANYSIZE                             --企业规模
    ) 
    SELECT DISTINCT 
                TO_DATE(p_data_dt_str,'YYYYMMDD')                       AS DATADATE            --数据日期
                ,p_data_dt_str                                          AS DATANO              --数据流水号
                ,T1.CLIENTID                                            AS CLIENTID            --参与主体ID
                ,T1.CLIENTID                                            AS SOURCECLIENTID      --源参与主体ID
                ,'XYK'                                                  AS SSYSID              --源系统ID
                ,T1.CLIENTNAME                                        AS CLIENTNAME          --参与主体名称
                ,'9998'                                             AS SORGID              --源机构ID
                ,'重庆银行'                                         AS SORGNAME            --源机构名称
                ,'1'                                              AS ORGSORTNO           --所属机构排序号
                ,'9998'                                             AS ORGID               --所属机构ID
                ,'重庆银行'                                         AS ORGNAME             --所属机构名称
                ,''                                                     AS INDUSTRYID          --所属行业代码
                ,''                                                     AS INDUSTRYNAME        --所属行业名称
                ,'04'                                                   AS CLIENTTYPE          --参与主体大类
                ,'0401'                                                 AS CLIENTSUBTYPE       --参与主体小类   --默认 自然人
                ,'CHN'                                                  AS COUNTRYCODE         --国家/地区代码
                ,'01'                                                   AS REGISTSTATE         --注册国家或地区
                ,''                                                     AS RCERATING           --境外注册地外部评级
                ,''                                                     AS RCERAGENCY          --境外注册地外部评级机构
                ,''                                                     AS ORGANIZATIONCODE    --组织机构代码
                ,'0'                                                    AS CONSOLIDATEDSCFLAG  --是否并表子公司
                ,''                                                     AS SLCLIENTFLAG        --专业贷款客户标识
                ,''                                                     AS SLCLIENTTYPE        --专业贷款客户类型
                ,''                                                     AS EXPOCATEGORYIRB     --内评法暴露类别
                ,'0'                                                    AS DEFAULTFLAG         --违约标识
                ,''                                                     AS MODELIRATING        --模型内部评级
                ,''                                                     AS MODELPD             --模型违约概率
                ,''                                                     AS IRATING             --内部评级
                ,''                                                     AS PD                  --违约概率
                ,''                                                     AS CLIENTERATING       --参与主体外部评级
                ,'0'                                                    AS CCPFLAG             --中央交易对手标识
                ,'0'                                                    AS QUALCCPFLAG         --是否合格中央交易对手
                ,'0'                                                    AS CLEARMEMBERFLAG     --清算会员标识
                ,'0'                                                    AS MSMBFLAG            --工信部微小企业标识
                ,'0'                                                    AS SSMBFLAG            --标准小微企业标识
                ,'0'                                                    AS SSMBFLAGSTD         --权重法标准小微企业标识
                ,NULL                                                   AS ANNUALSALE          --公司客户年销售额
                ,''                                                     AS MODELID             --模型ID
                ,'0'                                                    AS NEWDEFAULTFLAG      --新增违约标识
                ,''                                                     AS DEFAULTDATE         --违约时点
                ,''                                                     AS COMPANYSIZE         --企业规模
    FROM RWA_XYK_EXPOSURE T1
    WHERE NOT EXISTS (SELECT 1 FROM RWA_EI_CLIENT T6 WHERE T1.CLIENTID = T6.CLIENTID AND T6.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD'))
    ;

    COMMIT;


     --2.2 依据集市客户表插入信用风险暴露(抵债资产补录信息，抵债资产客户这里单独处理)的参与主体信息
    INSERT INTO RWA_DEV.RWA_EI_CLIENT(
                 DATADATE                                 --数据日期
                ,DATANO                                  --数据流水号
                ,CLIENTID                                --参与主体ID
                ,SOURCECLIENTID                          --源参与主体ID
                ,SSYSID                                  --源系统ID
                ,CLIENTNAME                              --参与主体名称
                ,SORGID                                  --源机构ID
                ,SORGNAME                                --源机构名称
                ,ORGSORTNO                               --所属机构排序号
                ,ORGID                                   --所属机构ID
                ,ORGNAME                                 --所属机构名称
                ,INDUSTRYID                              --所属行业代码
                ,INDUSTRYNAME                            --所属行业名称
                ,CLIENTTYPE                              --参与主体大类
                ,CLIENTSUBTYPE                           --参与主体小类
                ,COUNTRYCODE                             --国家/地区代码
                ,REGISTSTATE                             --注册国家或地区
                ,RCERATING                               --境外注册地外部评级
                ,RCERAGENCY                              --境外注册地外部评级机构
                ,ORGANIZATIONCODE                        --组织机构代码
                ,CONSOLIDATEDSCFLAG                      --是否并表子公司
                ,SLCLIENTFLAG                            --专业贷款客户标识
                ,SLCLIENTTYPE                            --专业贷款客户类型
                ,EXPOCATEGORYIRB                         --内评法暴露类别
                ,DEFAULTFLAG                             --违约标识
                ,MODELIRATING                            --模型内部评级
                ,MODELPD                                 --模型违约概率
                ,IRATING                                 --内部评级
                ,PD                                      --违约概率
                ,CLIENTERATING                           --参与主体外部评级
                ,CCPFLAG                                 --中央交易对手标识
                ,QUALCCPFLAG                             --是否合格中央交易对手
                ,CLEARMEMBERFLAG                         --清算会员标识
                ,MSMBFLAG                                --工信部微小企业标识
                ,SSMBFLAG                                --标准小微企业标识
                ,SSMBFLAGSTD         										 --权重法标准小微企业标识
                ,ANNUALSALE                              --公司客户年销售额
                ,MODELID                                 --模型ID
                ,NEWDEFAULTFLAG                          --新增违约标识
                ,DEFAULTDATE                             --违约时点
                ,COMPANYSIZE                             --企业规模
    )
     SELECT
                TO_DATE(T1.DATANO,'YYYYMMDD')                           AS DATADATE            --数据日期
                ,T1.DATANO                                              AS DATANO              --数据流水号
                ,'DZ-' || T1.GUARANTYID                                 AS CLIENTID            --参与主体ID
                ,T1.GUARANTYID                                          AS SOURCECLIENTID      --源参与主体ID
                ,'DZ'                                                   AS SSYSID              --源系统ID
                ,T1.GUARANTYNAME                                        AS CLIENTNAME          --参与主体名称
                ,'9998'                                         		AS SORGID              --源机构ID
                ,'重庆银行'                                   AS SORGNAME            --源机构名称
                ,'1'                                              AS ORGSORTNO           --所属机构排序号
                ,'9998'                                         		AS ORGID               --所属机构ID
                ,'重庆银行'                                   AS ORGNAME             --所属机构名称
                ,'J1622'                                               AS INDUSTRYID          --所属行业代码
                ,'未知'                                                 AS INDUSTRYNAME        --所属行业名称
                ,'03'                                                   AS CLIENTTYPE          --参与主体大类
                ,'0301'                                                 AS CLIENTSUBTYPE       --参与主体小类
                ,'CHN'                                                  AS COUNTRYCODE         --国家/地区代码
                ,'01'                                                   AS REGISTSTATE         --注册国家或地区
                ,''                                                     AS RCERATING           --境外注册地外部评级
                ,''                                                     AS RCERAGENCY          --境外注册地外部评级机构
                ,''                                                     AS ORGANIZATIONCODE    --组织机构代码
                ,'0'                                                    AS CONSOLIDATEDSCFLAG  --是否并表子公司
                ,''                                                     AS SLCLIENTFLAG        --专业贷款客户标识
                ,''                                                     AS SLCLIENTTYPE        --专业贷款客户类型
                ,''                                                     AS EXPOCATEGORYIRB     --内评法暴露类别
                ,'0'                                                    AS DEFAULTFLAG         --违约标识
                ,''                                                     AS MODELIRATING        --模型内部评级
                ,''                                                     AS MODELPD             --模型违约概率
                ,''                                                     AS IRATING             --内部评级
                ,''                                                     AS PD                  --违约概率
                ,''                                                     AS CLIENTERATING       --参与主体外部评级
                ,'0'                                                    AS CCPFLAG             --中央交易对手标识
                ,'0'                                                    AS QUALCCPFLAG         --是否合格中央交易对手
                ,'0'                                                    AS CLEARMEMBERFLAG     --清算会员标识
                ,'0'                                                    AS MSMBFLAG            --工信部微小企业标识
                ,'0'                                                    AS SSMBFLAG            --标准小微企业标识
                ,'0'                                                    AS SSMBFLAGSTD         --权重法标准小微企业标识
                ,NULL                                                   AS ANNUALSALE          --公司客户年销售额
                ,''                                                     AS MODELID             --模型ID
                ,'0'                                                    AS NEWDEFAULTFLAG      --新增违约标识
                ,''                                                     AS DEFAULTDATE         --违约时点
                ,'01'                                                   AS COMPANYSIZE         --企业规模 默认中型企业
    FROM 				RWA_DEV.NCM_ASSET_DEBT_INFO T1
    LEFT JOIN 	RWA.ORG_INFO T2
    ON 					T1.MANAGEORGID=T2.ORGID
    WHERE  			T1.DATANO=P_DATA_DT_STR
    ;

    COMMIT;

     --2.4 依据集市客户表插入信用风险保证的参与主体信息
    INSERT INTO RWA_DEV.RWA_EI_CLIENT(
                 DATADATE                                 --数据日期
                ,DATANO                                  --数据流水号
                ,CLIENTID                                --参与主体ID
                ,SOURCECLIENTID                          --源参与主体ID
                ,SSYSID                                  --源系统ID
                ,CLIENTNAME                              --参与主体名称
                ,SORGID                                  --源机构ID
                ,SORGNAME                                --源机构名称
                ,ORGSORTNO                               --所属机构排序号
                ,ORGID                                   --所属机构ID
                ,ORGNAME                                 --所属机构名称
                ,INDUSTRYID                              --所属行业代码
                ,INDUSTRYNAME                            --所属行业名称
                ,CLIENTTYPE                              --参与主体大类
                ,CLIENTSUBTYPE                           --参与主体小类
                ,COUNTRYCODE                             --国家/地区代码
                ,REGISTSTATE                             --注册国家或地区
                ,RCERATING                               --境外注册地外部评级
                ,RCERAGENCY                              --境外注册地外部评级机构
                ,ORGANIZATIONCODE                        --组织机构代码
                ,CONSOLIDATEDSCFLAG                      --是否并表子公司
                ,SLCLIENTFLAG                            --专业贷款客户标识
                ,SLCLIENTTYPE                            --专业贷款客户类型
                ,EXPOCATEGORYIRB                         --内评法暴露类别
                ,DEFAULTFLAG                             --违约标识
                ,MODELIRATING                            --模型内部评级
                ,MODELPD                                 --模型违约概率
                ,IRATING                                 --内部评级
                ,PD                                      --违约概率
                ,CLIENTERATING                           --参与主体外部评级
                ,CCPFLAG                                 --中央交易对手标识
                ,QUALCCPFLAG                             --是否合格中央交易对手
                ,CLEARMEMBERFLAG                         --清算会员标识
                ,MSMBFLAG                                --工信部微小企业标识
                ,SSMBFLAG                                --标准小微企业标识
                ,SSMBFLAGSTD         										 --权重法标准小微企业标识
                ,ANNUALSALE                              --公司客户年销售额
                ,MODELID                                 --模型ID
                ,NEWDEFAULTFLAG                          --新增违约标识
                ,DEFAULTDATE                             --违约时点
                ,COMPANYSIZE                             --企业规模
    )
    WITH TMP_CUST_ERATING AS (
							    SELECT CUSTOMERID
							    			,CLIENTERATING
							  		FROM (SELECT T1.CUSTOMERID 	AS CUSTOMERID,
							                    T1.EVALUTEORG 	AS EVALUTEORG,
							                    T2.DESCRATING		AS CLIENTERATING,
							                    T1.EVALUTEDATE	AS EVALUTEDATE,
							                    RANK() OVER(PARTITION BY T1.CUSTOMERID ORDER BY T1.EVALUTEDATE DESC) AS RK,
							                    ROW_NUMBER() OVER(PARTITION BY T1.CUSTOMERID, T1.EVALUTEDATE ORDER BY T2.DESCRATING) AS RM,
							                    COUNT(1) OVER(PARTITION BY T1.CUSTOMERID, T1.EVALUTEDATE) AS RN
							               FROM RWA_DEV.NCM_CUSTOMER_RATING T1
							          LEFT JOIN RWA_DEV.RWA_CD_RATING_MAPPING T2
							                 ON T1.EVALUTEORG = T2.SRCRATINGORG
							                AND T1.EVALUTELEVEL = T2.SRCRATINGNAME
							                AND T2.MAPPINGTYPE = '01' --全量类型
							                AND T2.SRCRATINGTYPE = '01' --长期评级
							              WHERE T1.EVALUTEDATE <= P_DATA_DT_STR
							              	AND T1.DATANO = P_DATA_DT_STR)
								 WHERE RK = 1
								   AND RM = (CASE WHEN RN = 1 THEN 1 ELSE 2 END)							--取最新的一笔评级，如果最近一天存在多笔评级，取第二好的评级
   	)
     SELECT
                TO_DATE(T1.DATANO,'YYYYMMDD')                           AS DATADATE            --数据日期
                ,T1.DATANO                                              AS DATANO              --数据流水号
                ,T1.CUSTOMERID                                          AS CLIENTID            --参与主体ID
                ,T1.CUSTOMERID                                          AS SOURCECLIENTID      --源参与主体ID
                ,'BZ'                                                   AS SSYSID              --源系统ID
                ,T1.CUSTOMERNAME                                        AS CLIENTNAME          --参与主体名称
                ,T1.ORGID                                               AS SORGID              --源机构ID
                ,T4.ORGNAME                                             AS SORGNAME            --源机构名称
                ,T4.SORTNO                                              AS ORGSORTNO           --所属机构排序号
                --,T1.ORGID                                               AS ORGID               --所属机构ID
                ,DECODE(SUBSTR(T1.ORGID,1,1),'@','01000000',T1.ORGID)
                --,T4.ORGNAME                                             AS ORGNAME             --所属机构名称
                ,NVL(T4.ORGNAME,'总行')
                ,'999999'                                               AS INDUSTRYID          --所属行业代码  经全鹏确认保证人行业是没有用的，直接归到未知行业
                ,'未知'                                                 AS INDUSTRYNAME        --所属行业名称
                ,''/*SUBSTR(T1.RWACUSTOMERTYPE,1,2)*/                         AS CLIENTTYPE          --参与主体大类
                ,''/*T1.RWACUSTOMERTYPE */                                    AS CLIENTSUBTYPE       --参与主体小类
                ,CASE WHEN  (T1.COUNTRYCODE IS NULL OR T1.COUNTRYCODE='OTH')
                      THEN  'CHN'
                      ELSE  T1.COUNTRYCODE
                 END                                                    AS COUNTRYCODE         --国家/地区代码
                ,CASE WHEN (T1.COUNTRYCODE IS NULL OR T1.COUNTRYCODE='OTH' OR T1.COUNTRYCODE='CHN') THEN '01'
                      ELSE '02'
                 END                                                    AS REGISTSTATE         --注册国家或地区
                ,CASE WHEN (T1.COUNTRYCODE IS NULL OR T1.COUNTRYCODE='OTH' OR T1.COUNTRYCODE='CHN') THEN ''
                      ELSE NVL(T3.RATINGRESULT,'0124')
                 END                                                    AS RCERATING           --境外注册地外部评级
                ,CASE WHEN (T1.COUNTRYCODE IS NULL OR T1.COUNTRYCODE='OTH' OR T1.COUNTRYCODE='CHN') THEN ''
                      ELSE NVL(T3.RATINGORG,'01')
                 END                                                    AS RCERAGENCY          --境外注册地外部评级机构
                ,CASE WHEN T1.CUSTOMERTYPE='0321000001'
                      THEN ''
                      ELSE T1.CERTID
                 END                                                    AS ORGANIZATIONCODE    --组织机构代码
                ,CASE WHEN T6.ORGANIZATIONCODE IS NOT NULL THEN '1'
                      ELSE '0'
                 END                                                    AS CONSOLIDATEDSCFLAG  --是否并表子公司
                ,'0'                                                    AS SLCLIENTFLAG        --专业贷款客户标识
                ,''                                                     AS SLCLIENTTYPE        --专业贷款客户类型
                ,''                                                     AS EXPOCATEGORYIRB     --内评法暴露类别
                ,CASE WHEN T8.PDADJCODE='D' THEN '1'
                      ELSE '0'
                 END                                                    AS DEFAULTFLAG         --违约标识
                ,T8.PDLEVEL										                          AS MODELIRATING        --模型内部评级
                ,T8.PD									                                AS MODELPD             --模型违约概率
                ,T8.PDADJLEVEL			 							                      AS IRATING             --内部评级
                ,T8.PD								                                  AS PD                  --违约概率
                ,T12.CLIENTERATING                                      AS CLIENTERATING       --参与主体外部评级
                ,'0'                                                    AS CCPFLAG             --中央交易对手标识
                ,'0'                                                    AS QUALCCPFLAG         --是否合格中央交易对手
                ,'0'                                                    AS CLEARMEMBERFLAG     --清算会员标识
                ,CASE WHEN T1.SCOPE IN ('02','03')
                      THEN '1'
                      ELSE '0'
                 END                                                    AS MSMBFLAG            --工信部微小企业标识
                ,NVL(T1.ISSUPERVISESTANDARSMENT,'0')                    AS SSMBFLAG            --标准小微企业标识
                ,'0'                                                    AS SSMBFLAGSTD         --权重法标准小微企业标识
                ,T1.AVEFINANCESUM                                       AS ANNUALSALE          --公司客户年销售额
                ,T8.MODELID                                             AS MODELID             --模型ID
                ,DECODE(NVL(T1.NEWDEFAULTFLAG,'1'),'0','1','0')         AS NEWDEFAULTFLAG      --新增违约标识
                ,CASE WHEN T8.PDADJCODE = 'D' THEN TO_DATE(T8.PDVAVLIDDATE,'YYYYMMDD')
                      ELSE NULL
                 END                                         						AS DEFAULTDATE         --违约时点
                ,CASE WHEN T1.SCOPE = '2' THEN '00'                                             --大型企业
                      WHEN T1.SCOPE = '3' THEN '01'                                            --中型企业
                      WHEN T1.SCOPE = '4' THEN '02'                                            --小型企业
                      WHEN T1.SCOPE = '5' THEN '03'                                            --微型企业
                      ELSE NVL(T1.SCOPE,'01')                                                  --默认中型企业
                 END                                                    AS COMPANYSIZE         --企业规模
    FROM        RWA_DEV.NCM_CUSTOMER_INFO T1
    INNER JOIN   (SELECT DISTINCT GUARANTORID FROM RWA_DEV.RWA_EI_GUARANTEE WHERE DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')) T2
    ON           T1.CUSTOMERID = T2.GUARANTORID
    LEFT JOIN   RWA.RWA_WP_COUNTRYRATING T3
    ON           T1.COUNTRYCODE = T3.COUNTRYCODE
    LEFT JOIN   RWA.ORG_INFO T4
    ON           T1.ORGID = T4.ORGID
    LEFT JOIN   RWA.CODE_LIBRARY T5
    ON           T1.INDUSTRYTYPE = T5.ITEMNO
    AND         T5.CODENO = 'IndustryType'
    LEFT JOIN   RWA.RWA_WP_SUBCOMPANY T6
    ON          T1.CERTID=T6.ORGANIZATIONCODE
    LEFT JOIN   RWA_DEV.RWA_TEMP_PDLEVEL T8
    ON          T1.CUSTOMERID=T8.CUSTID
    LEFT JOIN		TMP_CUST_ERATING T12
    ON					T1.CUSTOMERID = T12.CUSTOMERID
    WHERE       T1.DATANO = P_DATA_DT_STR
    AND NOT EXISTS (SELECT 1 FROM RWA_EI_CLIENT T6 WHERE T1.CUSTOMERID = T6.CLIENTID AND T6.DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD'))
    ;

    COMMIT;

    --2.5 依据集市客户表插入信用风险抵质押品的参与主体信息
    INSERT INTO RWA_DEV.RWA_EI_CLIENT(
                 DATADATE                                 --数据日期
                ,DATANO                                  --数据流水号
                ,CLIENTID                                --参与主体ID
                ,SOURCECLIENTID                          --源参与主体ID
                ,SSYSID                                  --源系统ID
                ,CLIENTNAME                              --参与主体名称
                ,SORGID                                  --源机构ID
                ,SORGNAME                                --源机构名称
                ,ORGSORTNO                               --所属机构排序号
                ,ORGID                                   --所属机构ID
                ,ORGNAME                                 --所属机构名称
                ,INDUSTRYID                              --所属行业代码
                ,INDUSTRYNAME                            --所属行业名称
                ,CLIENTTYPE                              --参与主体大类
                ,CLIENTSUBTYPE                           --参与主体小类
                ,COUNTRYCODE                             --国家/地区代码
                ,REGISTSTATE                             --注册国家或地区
                ,RCERATING                               --境外注册地外部评级
                ,RCERAGENCY                              --境外注册地外部评级机构
                ,ORGANIZATIONCODE                        --组织机构代码
                ,CONSOLIDATEDSCFLAG                      --是否并表子公司
                ,SLCLIENTFLAG                            --专业贷款客户标识
                ,SLCLIENTTYPE                            --专业贷款客户类型
                ,EXPOCATEGORYIRB                         --内评法暴露类别
                ,DEFAULTFLAG                             --违约标识
                ,MODELIRATING                            --模型内部评级
                ,MODELPD                                 --模型违约概率
                ,IRATING                                 --内部评级
                ,PD                                      --违约概率
                ,CLIENTERATING                           --参与主体外部评级
                ,CCPFLAG                                 --中央交易对手标识
                ,QUALCCPFLAG                             --是否合格中央交易对手
                ,CLEARMEMBERFLAG                         --清算会员标识
                ,MSMBFLAG                                --工信部微小企业标识
                ,SSMBFLAG                                --标准小微企业标识
                ,SSMBFLAGSTD         										 --权重法标准小微企业标识
                ,ANNUALSALE                              --公司客户年销售额
                ,MODELID                                 --模型ID
                ,NEWDEFAULTFLAG                          --新增违约标识
                ,DEFAULTDATE                             --违约时点
                ,COMPANYSIZE                             --企业规模
    )
    WITH TMP_CUST_ERATING AS (
							    SELECT CUSTOMERID
							    			,CLIENTERATING
							  		FROM (SELECT T1.CUSTOMERID 	AS CUSTOMERID,
							                    T1.EVALUTEORG 	AS EVALUTEORG,
							                    T2.DESCRATING		AS CLIENTERATING,
							                    T1.EVALUTEDATE	AS EVALUTEDATE,
							                    RANK() OVER(PARTITION BY T1.CUSTOMERID ORDER BY T1.EVALUTEDATE DESC) AS RK,
							                    ROW_NUMBER() OVER(PARTITION BY T1.CUSTOMERID, T1.EVALUTEDATE ORDER BY T2.DESCRATING) AS RM,
							                    COUNT(1) OVER(PARTITION BY T1.CUSTOMERID, T1.EVALUTEDATE) AS RN
							               FROM RWA_DEV.NCM_CUSTOMER_RATING T1
							          LEFT JOIN RWA_DEV.RWA_CD_RATING_MAPPING T2
							                 ON T1.EVALUTEORG = T2.SRCRATINGORG
							                AND T1.EVALUTELEVEL = T2.SRCRATINGNAME
							                AND T2.MAPPINGTYPE = '01' --全量类型
							                AND T2.SRCRATINGTYPE = '01' --长期评级
							              WHERE T1.EVALUTEDATE <= P_DATA_DT_STR
							              	AND T1.DATANO = P_DATA_DT_STR)
								 WHERE RK = 1
								   AND RM = (CASE WHEN RN = 1 THEN 1 ELSE 2 END)							--取最新的一笔评级，如果最近一天存在多笔评级，取第二好的评级
   	)
     SELECT
                TO_DATE(T1.DATANO,'YYYYMMDD')                           AS DATADATE            --数据日期
                ,T1.DATANO                                              AS DATANO              --数据流水号
                ,T1.CUSTOMERID                                          AS CLIENTID            --参与主体ID
                ,T1.CUSTOMERID                                          AS SOURCECLIENTID      --源参与主体ID
                ,'FX'                                                   AS SSYSID              --源系统ID
                ,T1.CUSTOMERNAME                                        AS CLIENTNAME          --参与主体名称
                ,T1.ORGID                                               AS SORGID              --源机构ID
                ,T4.ORGNAME                                             AS SORGNAME            --源机构名称
                ,T4.SORTNO                                              AS ORGSORTNO           --所属机构排序号
                --,T1.ORGID                                               AS ORGID               --所属机构ID
                ,decode(substr(T1.ORGID,1,1),'@','01000000',T1.ORGID)
                --,T4.ORGNAME                                             AS ORGNAME             --所属机构名称
                ,NVL(T4.ORGNAME,'总行')
                ,T1.INDUSTRYTYPE                                        AS INDUSTRYID          --所属行业代码
                ,T5.ITEMNAME                                            AS INDUSTRYNAME        --所属行业名称
                ,''/*SUBSTR(T1.RWACUSTOMERTYPE,1,2)*/                         AS CLIENTTYPE          --参与主体大类
                ,''/*T1.RWACUSTOMERTYPE */                                    AS CLIENTSUBTYPE       --参与主体小类
                ,CASE WHEN  (T1.COUNTRYCODE IS NULL OR T1.COUNTRYCODE='OTH')
                      THEN  'CHN'
                      ELSE  T1.COUNTRYCODE
                 END                                                    AS COUNTRYCODE         --国家/地区代码
                ,CASE WHEN (T1.COUNTRYCODE IS NULL OR T1.COUNTRYCODE='OTH' OR T1.COUNTRYCODE='CHN') THEN '01'
                      ELSE '02'
                 END                                                    AS REGISTSTATE         --注册国家或地区
                ,CASE WHEN (T1.COUNTRYCODE IS NULL OR T1.COUNTRYCODE='OTH' OR T1.COUNTRYCODE='CHN') THEN ''
                      ELSE NVL(T3.RATINGRESULT,'0124')
                 END                                                    AS RCERATING           --境外注册地外部评级
                ,CASE WHEN (T1.COUNTRYCODE IS NULL OR T1.COUNTRYCODE='OTH' OR T1.COUNTRYCODE='CHN') THEN ''
                      ELSE NVL(T3.RATINGORG,'01')
                 END                                                    AS RCERAGENCY          --境外注册地外部评级机构
                ,CASE WHEN T1.CUSTOMERTYPE='0321000001'
                      THEN ''
                      ELSE T1.CERTID
                 END                                                    AS ORGANIZATIONCODE    --组织机构代码
                ,CASE WHEN T6.ORGANIZATIONCODE IS NOT NULL THEN '1'
                      ELSE '0'
                 END                                                    AS CONSOLIDATEDSCFLAG  --是否并表子公司
                ,''                                                     AS SLCLIENTFLAG        --专业贷款客户标识
                ,''                                                     AS SLCLIENTTYPE        --专业贷款客户类型
                ,''                                                     AS EXPOCATEGORYIRB     --内评法暴露类别
                ,CASE WHEN T8.PDADJCODE='D' THEN '1'
                      ELSE '0'
                 END                                                    AS DEFAULTFLAG         --违约标识
                ,T8.PDLEVEL										                          AS MODELIRATING        --模型内部评级
                ,T8.PD								                                  AS MODELPD             --模型违约概率
                ,T8.PDADJLEVEL										                      AS IRATING             --内部评级
                ,T8.PD								                                  AS PD                  --违约概率
                ,T12.CLIENTERATING                                      AS CLIENTERATING       --参与主体外部评级
                ,'0'                                                    AS CCPFLAG             --中央交易对手标识
                ,'0'                                                    AS QUALCCPFLAG         --是否合格中央交易对手
                ,'0'                                                    AS CLEARMEMBERFLAG     --清算会员标识
                ,CASE WHEN T1.SCOPE IN ('02','03')
                      THEN '1'
                      ELSE '0'
                 END                                                    AS MSMBFLAG            --工信部微小企业标识
                ,NVL(T1.ISSUPERVISESTANDARSMENT,'0')                    AS SSMBFLAG            --标准小微企业标识
                ,'0'                                                    AS SSMBFLAGSTD         --权重法标准小微企业标识
                ,NULL                                                   AS ANNUALSALE          --公司客户年销售额
                ,T8.MODELID                                      				AS MODELID             --模型ID
                ,DECODE(NVL(T1.NEWDEFAULTFLAG,'1'),'0','1','0')         AS NEWDEFAULTFLAG      --新增违约标识
                ,CASE WHEN T8.PDADJCODE = 'D' THEN TO_DATE(T8.PDVAVLIDDATE,'YYYYMMDD')
                      ELSE NULL
                 END                                         						AS DEFAULTDATE         --违约时点
                ,CASE WHEN T1.SCOPE = '2' THEN '00'                                             --大型企业
                      WHEN T1.SCOPE = '3' THEN '01'                                            --中型企业
                      WHEN T1.SCOPE = '4' THEN '02'                                            --小型企业
                      WHEN T1.SCOPE = '5' THEN '03'                                            --微型企业
                      ELSE NVL(T1.SCOPE,'01')                                                  --默认中型企业
                 END                                                    AS COMPANYSIZE         --企业规模
    FROM        RWA_DEV.NCM_CUSTOMER_INFO T1
    INNER JOIN   (SELECT DISTINCT ISSUERID FROM RWA_DEV.RWA_EI_COLLATERAL WHERE DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD')) T2
    ON           T1.CUSTOMERID = T2.ISSUERID
    LEFT JOIN   RWA.RWA_WP_COUNTRYRATING T3
    ON           T1.COUNTRYCODE = T3.COUNTRYCODE
    LEFT JOIN   RWA.ORG_INFO T4
    ON           T1.ORGID = T4.ORGID
    LEFT JOIN   RWA.CODE_LIBRARY T5
    ON           T1.INDUSTRYTYPE = T5.ITEMNO
    AND         T5.CODENO = 'IndustryType'
    LEFT JOIN   RWA.RWA_WP_SUBCOMPANY T6
    ON          T1.CERTID=T6.ORGANIZATIONCODE
    LEFT JOIN   RWA_DEV.RWA_TEMP_PDLEVEL T8
    ON          T1.CUSTOMERID=T8.CUSTID
    LEFT JOIN		TMP_CUST_ERATING T12
    ON					T1.CUSTOMERID = T12.CUSTOMERID
    WHERE       T1.DATANO = P_DATA_DT_STR
    AND NOT EXISTS (SELECT 1 FROM RWA_EI_CLIENT T6 WHERE T1.CUSTOMERID = T6.CLIENTID AND T6.DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD'))
    ;

    COMMIT;

     --插入理财客户到EI客户表
    INSERT INTO RWA_DEV.RWA_EI_CLIENT(
                 DATADATE                                 --数据日期
                ,DATANO                                  --数据流水号
                ,CLIENTID                                --参与主体ID
                ,SOURCECLIENTID                          --源参与主体ID
                ,SSYSID                                  --源系统ID
                ,CLIENTNAME                              --参与主体名称
                ,SORGID                                  --源机构ID
                ,SORGNAME                                --源机构名称
                ,ORGSORTNO                               --所属机构排序号
                ,ORGID                                   --所属机构ID
                ,ORGNAME                                 --所属机构名称
                ,INDUSTRYID                              --所属行业代码
                ,INDUSTRYNAME                            --所属行业名称
                ,CLIENTTYPE                              --参与主体大类
                ,CLIENTSUBTYPE                           --参与主体小类
                ,COUNTRYCODE                             --国家/地区代码
                ,REGISTSTATE                             --注册国家或地区
                ,RCERATING                               --境外注册地外部评级
                ,RCERAGENCY                              --境外注册地外部评级机构
                ,ORGANIZATIONCODE                        --组织机构代码
                ,CONSOLIDATEDSCFLAG                      --是否并表子公司
                ,SLCLIENTFLAG                            --专业贷款客户标识
                ,SLCLIENTTYPE                            --专业贷款客户类型
                ,EXPOCATEGORYIRB                         --内评法暴露类别
                ,DEFAULTFLAG                             --违约标识
                ,MODELIRATING                            --模型内部评级
                ,MODELPD                                 --模型违约概率
                ,IRATING                                 --内部评级
                ,PD                                      --违约概率
                ,CLIENTERATING                           --参与主体外部评级
                ,CCPFLAG                                 --中央交易对手标识
                ,QUALCCPFLAG                             --是否合格中央交易对手
                ,CLEARMEMBERFLAG                         --清算会员标识
                ,MSMBFLAG                                --工信部微小企业标识
                ,SSMBFLAG                                --标准小微企业标识
                ,SSMBFLAGSTD         										 --权重法标准小微企业标识
                ,ANNUALSALE                              --公司客户年销售额
                ,MODELID                                 --模型ID
                ,NEWDEFAULTFLAG                          --新增违约标识
                ,DEFAULTDATE                             --违约时点
                ,COMPANYSIZE                             --企业规模
    )
    SELECT
                 T1.DATADATE                                 --数据日期
                ,T1.DATANO                                  --数据流水号
                ,T1.CLIENTID                                --参与主体ID
                ,T1.SOURCECLIENTID                          --源参与主体ID
                ,T1.SSYSID                                  --源系统ID
                ,T1.CLIENTNAME                              --参与主体名称
                ,T1.SORGID                                  --源机构ID
                ,T1.SORGNAME                                --源机构名称
                ,T1.ORGSORTNO                               --所属机构排序号
                ,T1.ORGID                                   --所属机构ID
                ,T1.ORGNAME                                 --所属机构名称
                ,T1.INDUSTRYID                              --所属行业代码
                ,T1.INDUSTRYNAME                            --所属行业名称
                ,T1.CLIENTTYPE                              --参与主体大类
                ,T1.CLIENTSUBTYPE                           --参与主体小类
                ,T1.COUNTRYCODE                             --国家/地区代码
                ,T1.REGISTSTATE                             --注册国家或地区
                ,T1.RCERATING                               --境外注册地外部评级
                ,T1.RCERAGENCY                              --境外注册地外部评级机构
                ,T1.ORGANIZATIONCODE                        --组织机构代码
                ,T1.CONSOLIDATEDSCFLAG                      --是否并表子公司
                ,T1.SLCLIENTFLAG                            --专业贷款客户标识
                ,T1.SLCLIENTTYPE                            --专业贷款客户类型
                ,T1.EXPOCATEGORYIRB                         --内评法暴露类别
                ,T1.DEFAULTFLAG                             --违约标识
                ,T1.MODELIRATING                            --模型内部评级
                ,T1.MODELPD                                 --模型违约概率
                ,T1.IRATING                                 --内部评级
                ,T1.PD                                      --违约概率
                ,T1.CLIENTERATING                           --参与主体外部评级
                ,T1.CCPFLAG                                 --中央交易对手标识
                ,T1.QUALCCPFLAG                             --是否合格中央交易对手
                ,T1.CLEARMEMBERFLAG                         --清算会员标识
                ,T1.MSMBFLAG                                --工信部微小企业标识
                ,T1.SSMBFLAG                                --标准小微企业标识
                ,T1.SSMBFLAGSTD                             --权重法标准小微企业标识
                ,T1.ANNUALSALE                              --公司客户年销售额
                ,T1.MODELID                                 --模型ID
                ,T1.NEWDEFAULTFLAG                          --新增违约标识
                ,T1.DEFAULTDATE                             --违约时点
                ,NVL(T1.COMPANYSIZE,'01')                   --企业规模
    FROM RWA_DEV.RWA_LC_CLIENT T1
    WHERE       T1.DATANO = P_DATA_DT_STR
    AND NOT EXISTS (SELECT 1 FROM RWA_EI_CLIENT T6 WHERE T1.CLIENTID = T6.CLIENTID AND T6.DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD'))
    ;

    COMMIT;

    --插入市场风险财务债券投资发行人到EI客户表
    INSERT INTO RWA_DEV.RWA_EI_CLIENT(
                 DATADATE                                --数据日期
                ,DATANO                                  --数据流水号
                ,CLIENTID                                --参与主体ID
                ,SOURCECLIENTID                          --源参与主体ID
                ,SSYSID                                  --源系统ID
                ,CLIENTNAME                              --参与主体名称
                ,SORGID                                  --源机构ID
                ,SORGNAME                                --源机构名称
                ,ORGSORTNO                               --所属机构排序号
                ,ORGID                                   --所属机构ID
                ,ORGNAME                                 --所属机构名称
                ,INDUSTRYID                              --所属行业代码
                ,INDUSTRYNAME                            --所属行业名称
                ,CLIENTTYPE                              --参与主体大类
                ,CLIENTSUBTYPE                           --参与主体小类
                ,COUNTRYCODE                             --国家/地区代码
                ,REGISTSTATE                             --注册国家或地区
                ,RCERATING                               --境外注册地外部评级
                ,RCERAGENCY                              --境外注册地外部评级机构
                ,ORGANIZATIONCODE                        --组织机构代码
                ,CONSOLIDATEDSCFLAG                      --是否并表子公司
                ,SLCLIENTFLAG                            --专业贷款客户标识
                ,SLCLIENTTYPE                            --专业贷款客户类型
                ,EXPOCATEGORYIRB                         --内评法暴露类别
                ,DEFAULTFLAG                             --违约标识
                ,MODELIRATING                            --模型内部评级
                ,MODELPD                                 --模型违约概率
                ,IRATING                                 --内部评级
                ,PD                                      --违约概率
                ,CLIENTERATING                           --参与主体外部评级
                ,CCPFLAG                                 --中央交易对手标识
                ,QUALCCPFLAG                             --是否合格中央交易对手
                ,CLEARMEMBERFLAG                         --清算会员标识
                ,MSMBFLAG                                --工信部微小企业标识
                ,SSMBFLAG                                --标准小微企业标识
                ,SSMBFLAGSTD         										 --权重法标准小微企业标识
                ,ANNUALSALE                              --公司客户年销售额
                ,MODELID                                 --模型ID
                ,NEWDEFAULTFLAG                          --新增违约标识
                ,DEFAULTDATE                             --违约时点
                ,COMPANYSIZE                             --企业规模
    )
    WITH TEMP_BND_BOOK AS (
    										SELECT BOND_ID,
												       INITIAL_COST,
												       INT_ADJUST,
												       MKT_VALUE_CHANGE,
												       RECEIVABLE_INT,
												       ACCOUNTABLE_INT,
												       PAR_VALUE
												  FROM (SELECT BOND_ID,
												               INITIAL_COST,
												               INT_ADJUST,
												               MKT_VALUE_CHANGE,
												               RECEIVABLE_INT,
												               ACCOUNTABLE_INT,
												               PAR_VALUE,
												               ROW_NUMBER() OVER(PARTITION BY BOND_ID ORDER BY SORT_SEQ DESC) RM
												          FROM FNS_BND_BOOK_B
												         WHERE AS_OF_DATE <= P_DATA_DT_STR
												           AND DATANO = P_DATA_DT_STR)
												 WHERE RM = 1
												   AND NVL(INITIAL_COST, 0) + NVL(INT_ADJUST, 0) + NVL(MKT_VALUE_CHANGE, 0) + NVL(ACCOUNTABLE_INT, 0) <> 0
		)
		,TMP_CUST_ERATING AS (
							    SELECT CUSTOMERID
							    			,CLIENTERATING
							  		FROM (SELECT T1.CUSTOMERID 	AS CUSTOMERID,
							                    T1.EVALUTEORG 	AS EVALUTEORG,
							                    T2.DESCRATING		AS CLIENTERATING,
							                    T1.EVALUTEDATE	AS EVALUTEDATE,
							                    RANK() OVER(PARTITION BY T1.CUSTOMERID ORDER BY T1.EVALUTEDATE DESC) AS RK,
							                    ROW_NUMBER() OVER(PARTITION BY T1.CUSTOMERID, T1.EVALUTEDATE ORDER BY T2.DESCRATING) AS RM,
							                    COUNT(1) OVER(PARTITION BY T1.CUSTOMERID, T1.EVALUTEDATE) AS RN
							               FROM RWA_DEV.NCM_CUSTOMER_RATING T1
							          LEFT JOIN RWA_DEV.RWA_CD_RATING_MAPPING T2
							                 ON T1.EVALUTEORG = T2.SRCRATINGORG
							                AND T1.EVALUTELEVEL = T2.SRCRATINGNAME
							                AND T2.MAPPINGTYPE = '01' --全量类型
							                AND T2.SRCRATINGTYPE = '01' --长期评级
							              WHERE T1.EVALUTEDATE <= P_DATA_DT_STR
							              	AND T1.DATANO = P_DATA_DT_STR)
								 WHERE RK = 1
								   AND RM = (CASE WHEN RN = 1 THEN 1 ELSE 2 END)							--取最新的一笔评级，如果最近一天存在多笔评级，取第二好的评级
   	)
    SELECT
                 TO_DATE(p_data_dt_str,'YYYYMMDD')          --数据日期
                ,p_data_dt_str                              --数据流水号
                ,T1.CUSTOMERID                                          AS CLIENTID            --参与主体ID
                ,T1.CUSTOMERID                                          AS SOURCECLIENTID      --源参与主体ID
                ,'TZZQ'                                                 AS SSYSID              --源系统ID
                ,T1.CUSTOMERNAME                                        AS CLIENTNAME          --参与主体名称
                ,T1.ORGID                                               AS SORGID              --源机构ID
                ,T8.ORGNAME                                             AS SORGNAME            --源机构名称
                ,T8.SORTNO                                              AS ORGSORTNO           --所属机构排序号
                --,T1.ORGID                                               AS ORGID               --所属机构ID
                ,DECODE(SUBSTR(T1.ORGID,1,1),'@','01000000',T1.ORGID)
                --,T8.ORGNAME                                             AS ORGNAME             --所属机构名称
                ,nvl(T8.ORGNAME,'总行')
                ,T1.INDUSTRYTYPE                                        AS INDUSTRYID          --所属行业代码
                ,T9.ITEMNAME                                            AS INDUSTRYNAME        --所属行业名称
                ,''/*SUBSTR(T1.RWACUSTOMERTYPE,1,2)*/                         AS CLIENTTYPE          --参与主体大类
                ,''/*T1.RWACUSTOMERTYPE */                                    AS CLIENTSUBTYPE       --参与主体小类
                ,CASE WHEN  (T1.COUNTRYCODE IS NULL OR T1.COUNTRYCODE='OTH')
                      THEN  'CHN'
                      ELSE  T1.COUNTRYCODE
                 END                                                    AS COUNTRYCODE         --国家/地区代码
                ,CASE WHEN (T1.COUNTRYCODE IS NULL OR T1.COUNTRYCODE='OTH' OR T1.COUNTRYCODE='CHN') THEN '01'
                      ELSE '02'
                 END                                                    AS REGISTSTATE         --注册国家或地区
                ,CASE WHEN (T1.COUNTRYCODE IS NULL OR T1.COUNTRYCODE='OTH' OR T1.COUNTRYCODE='CHN') THEN ''
                      ELSE NVL(T7.RATINGRESULT,'0124')
                 END                                                    AS RCERATING           --境外注册地外部评级
                ,CASE WHEN (T1.COUNTRYCODE IS NULL OR T1.COUNTRYCODE='OTH' OR T1.COUNTRYCODE='CHN') THEN ''
                      ELSE NVL(T7.RATINGORG,'01')
                 END                                                    AS RCERAGENCY          --境外注册地外部评级机构
                ,CASE WHEN T1.CUSTOMERTYPE='0321000001'
                      THEN ''
                      ELSE T1.CERTID
                 END                                                    AS ORGANIZATIONCODE    --组织机构代码
                ,CASE WHEN T10.ORGANIZATIONCODE IS NOT NULL THEN '1'
                      ELSE '0'
                 END                                                    AS CONSOLIDATEDSCFLAG  --是否并表子公司
                ,'0'                                                    AS SLCLIENTFLAG        --专业贷款客户标识
                ,''                                                     AS SLCLIENTTYPE        --专业贷款客户类型
                ,''                                                     AS EXPOCATEGORYIRB     --内评法暴露类别
                ,CASE WHEN T11.PDADJCODE='D' THEN '1'
                      ELSE '0'
                 END                                                    AS DEFAULTFLAG         --违约标识
                ,T11.PDLEVEL										                        AS MODELIRATING        --模型内部评级
                ,T11.PD								                                  AS MODELPD             --模型违约概率
                ,T11.PDADJLEVEL										                      AS IRATING             --内部评级
                ,T11.PD								                                  AS PD                  --违约概率
                ,T12.CLIENTERATING                                      AS CLIENTERATING       --参与主体外部评级
                ,'0'                                                    AS CCPFLAG             --中央交易对手标识
                ,'0'                                                    AS QUALCCPFLAG         --是否合格中央交易对手
                ,'0'                                                    AS CLEARMEMBERFLAG     --清算会员标识
                ,CASE WHEN T1.SCOPE IN ('02','03')
                      THEN '1'
                      ELSE '0'
                 END                                                    AS MSMBFLAG            --工信部微小企业标识
                ,NVL(T1.ISSUPERVISESTANDARSMENT,'0')                    AS SSMBFLAG            --标准小微企业标识
                ,'0'                                                    AS SSMBFLAGSTD         --权重法标准小微企业标识
                ,T1.AVEFINANCESUM                                       AS ANNUALSALE          --公司客户年销售额
                ,T11.MODELID                                            AS MODELID             --模型ID
                ,DECODE(NVL(T1.NEWDEFAULTFLAG,'1'),'0','1','0')         AS NEWDEFAULTFLAG      --新增违约标识
                ,CASE WHEN T11.PDADJCODE = 'D' THEN TO_DATE(T11.PDVAVLIDDATE,'YYYYMMDD')
                      ELSE NULL
                 END                                         						AS DEFAULTDATE         --违约时点
                ,CASE WHEN T1.SCOPE = '2' THEN '00'                                             --大型企业
                      WHEN T1.SCOPE = '3' THEN '01'                                            --中型企业
                      WHEN T1.SCOPE = '4' THEN '02'                                            --小型企业
                      WHEN T1.SCOPE = '5' THEN '03'                                            --微型企业
                      ELSE NVL(T1.SCOPE,'01')                                                  --默认中型企业
                 END                                                    AS COMPANYSIZE         --企业规模
    FROM        NCM_CUSTOMER_INFO T1
    INNER JOIN  (SELECT DISTINCT BONDPUBLISHID
                 FROM        RWA_DEV.NCM_BOND_INFO T2
                 INNER JOIN  RWA_DEV.NCM_BUSINESS_DUEBILL T3
                 ON          T2.OBJECTNO=T3.RELATIVESERIALNO2
                 AND         T3.DATANO =P_DATA_DT_STR
                 INNER JOIN  TEMP_BND_BOOK T4
                 ON          T3.THIRDPARTYACCOUNTS='CW_IMPORTDATA' || T4.BOND_ID
                 INNER JOIN  RWA_DEV.FNS_BND_INFO_B T5
                 ON          T4.BOND_ID=T5.BOND_ID
                 AND         T5.ASSET_CLASS = '10'                                        --仅交易性账户进入市场风险
                 AND         T5.DATANO =P_DATA_DT_STR
                 WHERE       T2.OBJECTTYPE = 'BusinessContract'
                 AND         T2.DATANO = P_DATA_DT_STR
                 ) T2
    ON T1.CUSTOMERID=T2.BONDPUBLISHID
    LEFT JOIN   RWA.RWA_WP_COUNTRYRATING T7
    ON          T1.COUNTRYCODE = T7.COUNTRYCODE
    LEFT JOIN   RWA.ORG_INFO T8
    ON          T1.ORGID = T8.ORGID
    LEFT JOIN   RWA.CODE_LIBRARY T9
    ON          T1.INDUSTRYTYPE = T9.ITEMNO
    AND         T9.CODENO = 'IndustryType'
    LEFT JOIN   RWA.RWA_WP_SUBCOMPANY T10
    ON          T1.CERTID = T10.ORGANIZATIONCODE
    LEFT JOIN   RWA_DEV.RWA_TEMP_PDLEVEL T11
    ON          T1.CUSTOMERID = T11.CUSTID
    LEFT JOIN		TMP_CUST_ERATING T12
    ON					T1.CUSTOMERID = T12.CUSTOMERID
    WHERE       T1.DATANO = P_DATA_DT_STR
    AND NOT EXISTS (SELECT 1 FROM RWA_EI_CLIENT T6 WHERE T1.CUSTOMERID = T6.CLIENTID AND T6.DATADATE = TO_DATE(P_DATA_DT_STR, 'YYYYMMDD'))
    ;

    COMMIT;



 --同业客户信息+评级  OPICS 
INSERT INTO RWA_DEV.RWA_EI_CLIENT(
                DATADATE                           --数据日期
                ,DATANO                                  --数据流水号
                ,CLIENTID                                --参与主体ID
                ,SOURCECLIENTID                          --源参与主体ID
                ,SSYSID                                  --源系统ID
                ,CLIENTNAME                              --参与主体名称
                ,SORGID                                  --源机构ID
                ,SORGNAME                                --源机构名称
                ,ORGSORTNO                               --所属机构排序号
                ,ORGID                                   --所属机构ID
                ,ORGNAME                                 --所属机构名称
                ,INDUSTRYID                              --所属行业代码
                ,INDUSTRYNAME                            --所属行业名称
                ,CLIENTTYPE                              --参与主体大类
                ,CLIENTSUBTYPE                           --参与主体小类
                ,COUNTRYCODE                             --国家/地区代码
                ,REGISTSTATE                             --注册国家或地区
                ,RCERATING                               --境外注册地外部评级
                ,RCERAGENCY                              --境外注册地外部评级机构
                ,ORGANIZATIONCODE                        --组织机构代码
                ,CONSOLIDATEDSCFLAG                      --是否并表子公司
                ,SLCLIENTFLAG                            --专业贷款客户标识
                ,SLCLIENTTYPE                            --专业贷款客户类型
                ,EXPOCATEGORYIRB                         --内评法暴露类别
                ,DEFAULTFLAG                             --违约标识
                ,MODELIRATING                            --模型内部评级
                ,MODELPD                                 --模型违约概率
                ,IRATING                                 --内部评级
                ,PD                                      --违约概率
                ,CLIENTERATING                           --参与主体外部评级
                ,CCPFLAG                                 --中央交易对手标识
                ,QUALCCPFLAG                             --是否合格中央交易对手
                ,CLEARMEMBERFLAG                         --清算会员标识
                ,MSMBFLAG                                --工信部微小企业标识
                ,SSMBFLAG                                --标准小微企业标识
                ,SSMBFLAGSTD                              --权重法标准小微企业标识
                ,ANNUALSALE                              --公司客户年销售额
                ,MODELID                                 --模型ID
                ,NEWDEFAULTFLAG                          --新增违约标识
                ,DEFAULTDATE                             --违约时点
                ,COMPANYSIZE                             --企业规模
     )
     SELECT
                 TO_DATE(T1.DATANO,'YYYYMMDD')                              --数据日期
                ,T1.DATANO                                  --数据流水号
                ,'OPI'||TRIM(T1.CNO)                   --参与主体ID
                ,TRIM(T1.CNO)                          --源参与主体ID
                ,'TY'                                  --源系统ID
                ,T1.CFN1                             --参与主体名称
                ,'9998'                                  --源机构ID
                ,'重庆银行'                                --源机构名称
                ,'1'                               --所属机构排序号
                ,'9998'                                   --所属机构ID
                ,'重庆银行'                                 --所属机构名称
                ,'J6621'                              --所属行业代码
                ,''                            --所属行业名称
                ,CASE 
                 WHEN TRIM(T1.ACCTNGTYPE) = 'D-CTRL-BNK' THEN '01'
                      ELSE '02' 
                 END                            --参与主体大类
                ,CASE 
                   WHEN TRIM(T1.ACCTNGTYPE) IN('D-CCOM-BNK','D-ECOM-BNK','D-FORE-BNK','D-RCOM-BNK','D-T4SO-BNK') THEN '0202'
                   WHEN TRIM(T1.ACCTNGTYPE) IN('O-CHIN-BNK','O-FORE-BNK') THEN '0206'
                   WHEN TRIM(T1.ACCTNGTYPE) ='D-CTRL-BNK' THEN '0103' 
                   WHEN TRIM(T1.ACCTNGTYPE) ='OW-D-INS' THEN '0205'   
                   WHEN TRIM(T1.ACCTNGTYPE) ='DEFAULT' THEN '0206'
                   WHEN TRIM(T1.ACCTNGTYPE) ='D-POLY-BNK' THEN '0201'
                   ELSE ''
                 END               --参与主体小类
                ,T2.COUNTRYCODE                            --国家/地区代码
                ,DECODE(T1.CCODE,'CN','01','02')                            --注册国家或地区
                ,NVL(T2.RATINGRESULT,'0102')                                                      --境外注册地外部评级
                ,'01'                              --境外注册地外部评级机构
                ,''                         --组织机构代码
                ,'0'                       --是否并表子公司
                ,'0'                            --专业贷款客户标识
                ,''                             --专业贷款客户类型
                ,'020201'                          --内评法暴露类别
                ,'0'                             --违约标识
                ,''                             --模型内部评级
                ,''                                 --模型违约概率
                ,''                                  --内部评级
                ,''                                      --违约概率
                ,NVL(T2.RATINGRESULT,'0102')          --参与主体外部评级
                ,'0'                                  --中央交易对手标识
                ,'0'                             --是否合格中央交易对手
                ,'0'                         --清算会员标识
                ,''                                --工信部微小企业标识
                ,''                                --标准小微企业标识
                ,''                              --权重法标准小微企业标识
                ,''                               --公司客户年销售额
                ,''                                  --模型ID
                ,'0'                           --新增违约标识
                ,''                              --违约时点
                ,''                                          --企业规模 默认中型企业
    FROM RWA_DEV.OPI_CUST T1
    LEFT JOIN RWA.RWA_WP_COUNTRYRATING T2
    ON T1.CCODE=T2.OPICODE
    WHERE T1.DATANO=p_data_dt_str;
    
    COMMIT;


     /*   
     消费金融客户信息已在信贷客户时处理了
     
     --插入消费金融个人客户信息到EI客户表
    INSERT INTO RWA_DEV.RWA_EI_CLIENT(
                 DATADATE                                --数据日期
                ,DATANO                                  --数据流水号
                ,CLIENTID                                --参与主体ID
                ,SOURCECLIENTID                          --源参与主体ID
                ,SSYSID                                  --源系统ID
                ,CLIENTNAME                              --参与主体名称
                ,SORGID                                  --源机构ID
                ,SORGNAME                                --源机构名称
                ,ORGSORTNO                               --所属机构排序号
                ,ORGID                                   --所属机构ID
                ,ORGNAME                                 --所属机构名称
                ,INDUSTRYID                              --所属行业代码
                ,INDUSTRYNAME                            --所属行业名称
                ,CLIENTTYPE                              --参与主体大类
                ,CLIENTSUBTYPE                           --参与主体小类
                ,COUNTRYCODE                             --国家/地区代码
                ,REGISTSTATE                             --注册国家或地区
                ,RCERATING                               --境外注册地外部评级
                ,RCERAGENCY                              --境外注册地外部评级机构
                ,ORGANIZATIONCODE                        --组织机构代码
                ,CONSOLIDATEDSCFLAG                      --是否并表子公司
                ,SLCLIENTFLAG                            --专业贷款客户标识
                ,SLCLIENTTYPE                            --专业贷款客户类型
                ,EXPOCATEGORYIRB                         --内评法暴露类别
                ,DEFAULTFLAG                             --违约标识
                ,MODELIRATING                            --模型内部评级
                ,MODELPD                                 --模型违约概率
                ,IRATING                                 --内部评级
                ,PD                                      --违约概率
                ,CLIENTERATING                           --参与主体外部评级
                ,CCPFLAG                                 --中央交易对手标识
                ,QUALCCPFLAG                             --是否合格中央交易对手
                ,CLEARMEMBERFLAG                         --清算会员标识
                ,MSMBFLAG                                --工信部微小企业标识
                ,SSMBFLAG                                --标准小微企业标识
                ,SSMBFLAGSTD         										 --权重法标准小微企业标识
                ,ANNUALSALE                              --公司客户年销售额
                ,MODELID                                 --模型ID
                ,NEWDEFAULTFLAG                          --新增违约标识
                ,DEFAULTDATE                             --违约时点
                ,COMPANYSIZE                             --企业规模
    )
    SELECT
                 DATADATE                                --数据日期
                ,DATANO                                  --数据流水号
                ,CLIENTID                                --参与主体ID
                ,SOURCECLIENTID                          --源参与主体ID
                ,SSYSID                                  --源系统ID
                ,CLIENTNAME                              --参与主体名称
                ,SORGID                                  --源机构ID
                ,SORGNAME                                --源机构名称
                ,ORGSORTNO                               --所属机构排序号
                ,ORGID                                   --所属机构ID
                ,ORGNAME                                 --所属机构名称
                ,INDUSTRYID                              --所属行业代码
                ,INDUSTRYNAME                            --所属行业名称
                ,CLIENTTYPE                              --参与主体大类
                ,CLIENTSUBTYPE                           --参与主体小类
                ,COUNTRYCODE                             --国家/地区代码
                ,REGISTSTATE                             --注册国家或地区
                ,RCERATING                               --境外注册地外部评级
                ,RCERAGENCY                              --境外注册地外部评级机构
                ,ORGANIZATIONCODE                        --组织机构代码
                ,CONSOLIDATEDSCFLAG                      --是否并表子公司
                ,SLCLIENTFLAG                            --专业贷款客户标识
                ,SLCLIENTTYPE                            --专业贷款客户类型
                ,EXPOCATEGORYIRB                         --内评法暴露类别
                ,DEFAULTFLAG                             --违约标识
                ,MODELIRATING                            --模型内部评级
                ,MODELPD                                 --模型违约概率
                ,IRATING                                 --内部评级
                ,PD                                      --违约概率
                ,CLIENTERATING                           --参与主体外部评级
                ,CCPFLAG                                 --中央交易对手标识
                ,QUALCCPFLAG                             --是否合格中央交易对手
                ,CLEARMEMBERFLAG                         --清算会员标识
                ,MSMBFLAG                                --工信部微小企业标识
                ,SSMBFLAG                                --标准小微企业标识
                ,SSMBFLAGSTD         										 --权重法标准小微企业标识
                ,ANNUALSALE                              --公司客户年销售额
                ,MODELID                                 --模型ID
                ,NEWDEFAULTFLAG                          --新增违约标识
                ,DEFAULTDATE                             --违约时点
                ,COMPANYSIZE                             --企业规模
    FROM RWA_DEV.RWA_XF_CLIENT C
    WHERE C.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD');
 
    COMMIT;*/
    
    
    ----------20191120  by wzb ----
    
       update rwa_ei_client
   set ORGID='9998',SORGID='9998',ORGNAME='特殊处理'
   where  ORGID IS NULL AND  datano=p_data_dt_str;
  COMMIT;
    
 
    --整理表信息
    --dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_CLIENT',cascade => true);
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_CLIENT',partname => 'CLIENT'||p_data_dt_str,granularity => 'PARTITION',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_CLIENT WHERE DATANO = p_data_dt_str;
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_CLIENT表当前插入的数据记录为:' || v_count || '条');

    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功'||'-'||v_count;
    --定义异常
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '汇总参与主体表('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_EI_CLIENT;
/

