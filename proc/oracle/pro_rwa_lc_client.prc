CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_LC_CLIENT(p_data_dt_str IN  VARCHAR2, --数据日期
                                              p_po_rtncode  OUT VARCHAR2, --返回编号
                                              p_po_rtnmsg   OUT VARCHAR2  --返回描述
)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_LC_CLIENT
    实现功能:理财资管系统，将相关信息全量导入RWA接口表参与主体中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :QHJIANG
    编写时间:2016-04-14
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.ZGS_ATBOND|债券信息表
    源  表2 :RWA_DEV.ZGS_ATINTRUST_PLAN|资产管理计划表
    源  表3 :RWA_DEV.ZGS_INVESTASSETDETAIL|交易明细表
    源  表4 :RWA_DEV.ZGS_FINANCING_INFO|产品信息表
    目标表1 :RWA_DEV.RWA_LC_CLIENT|RWA参与主体信息表
    辅助表  :无
    变更记录(修改人|修改时间|修改内容):
    */

  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_LC_CLIENT';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN
    --DBMS_OUTPUT.PUT_LINE('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --清除目标表中的原有记录
    --DELETE FROM RWA_DEV.RWA_LC_CLIENT WHERE DATADATE=TO_DATE(p_data_dt_str,'yyyyMMdd');
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_LC_CLIENT';

    --DBMS_OUTPUT.PUT_LINE('开始【步骤1】：导入【参与主体-债券】' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    --步骤1 导入【参与主体-债券】
    INSERT INTO RWA_DEV.RWA_LC_CLIENT(
         DataDate                   --数据日期
        ,DataNo                     --数据流水号
        ,ClientID                   --参与主体ID
        ,SourceClientID             --源参与主体ID
        ,SSysID                     --源系统ID
        ,ClientName                 --参与主体名称
        ,SOrgID                     --源机构ID
        ,SOrgName                   --源机构名称
        ,OrgSortNo                  --所属机构排序号
        ,OrgID                      --所属机构ID
        ,OrgName                    --所属机构名称
        ,IndustryID                 --所属行业代码
        ,IndustryName               --所属行业名称
        ,ClientType                 --参与主体大类
        ,ClientSubType              --参与主体小类
        ,RegistState                --注册国家或地区
        ,RCERating                  --境外注册地外部评级
        ,RCERAgency                 --境外注册地外部评级机构
        ,OrganizationCode           --组织机构代码
        ,ConsolidatedSCFlag         --是否并表子公司
        ,SLClientFlag               --专业贷款客户标识
        ,SLClientType               --专业贷款客户类型
        ,ExpoCategoryIRB            --内评法暴露类别
        ,ModelID                    --模型ID
        ,ModelIRating               --模型内部评级
        ,ModelPD                    --模型违约概率
        ,IRating                    --内部评级
        ,PD                         --违约概率
        ,DefaultFlag                --违约标识
        ,NewDefaultFlag             --新增违约标识
        ,DefaultDate                --违约时点
        ,ClientERating              --参与主体外部评级
        ,CCPFlag                    --中央交易对手标识
        ,QualCCPFlag                --是否合格中央交易对手
        ,ClearMemberFlag            --清算会员标识
        ,CompanySize                --企业规模
        ,SSMBFlag                   --标准小微企业标识
        ,SSMBFLAGSTD         				--权重法标准小微企业标识
        ,AnnualSale                 --公司客户年销售额
        ,CountryCode                --注册国家代码
        ,MSMBFlag										--工信部微小企业标识
    )
  	WITH TEMP_INVESTASSETDETAIL AS (
							        SELECT  DISTINCT
							        				T3.FLD_ASSET_CODE						AS FLD_ASSET_CODE
							          FROM RWA_DEV.ZGS_INVESTASSETDETAIL T3
							    INNER JOIN RWA_DEV.ZGS_FINANCING_INFO T4
							            ON T3.FLD_FINANC_CODE = T4.FLD_FINANC_CODE
							           AND T4.FLD_INCOME_TYPE <> '3'																		--3：排除非保本类型
							           AND T4.DATANO = p_data_dt_str
                         AND T3.DATANO=T4.DATANO
							         WHERE T3.FLD_ASSET_TYPE = '2'																			-- 2：债券，24：资产管理计划
							           AND T3.FLD_ASSET_STATUS = '1' 																		--1：状态正常
							           AND T3.FLD_ASSET_FLAG = '1'   																		--1：理财产品
							           AND (T3.C_ACC_TYPE <> 'D' OR T3.C_ACC_TYPE IS NULL)							--D：交易类，排除该类数据
							           AND T3.FLD_DATE = p_data_dt_str																	--有效的理财产品其估值日期每日更新
							           AND T3.DATANO = p_data_dt_str
    )
    , TEMP_CUST_INFO AS (
    								SELECT CUSTOMERID
    											,CUSTOMERNAME
    											,ORGID
    											,CERTTYPE
    											,CERTID
    											,INDUSTRYTYPE
    											,COUNTRYCODE
    											,RWACUSTOMERTYPE
    											,NEWDEFAULTFLAG
    											,DEFAULTDATE
    											,SCOPE
    											,ISSUPERVISESTANDARSMENT
    											,AVEFINANCESUM
    									FROM RWA_DEV.NCM_CUSTOMER_INFO
    								 WHERE ROWID IN (SELECT MAX(ROWID) FROM RWA_DEV.NCM_CUSTOMER_INFO WHERE DATANO = p_data_dt_str AND CERTTYPE IN ('Ent01','Ent02') GROUP BY CERTID)
    								 	 AND DATANO = p_data_dt_str
    )
 		, TMP_BND_CUST_INFO AS ( --国债默认发行人为中国中央政府
 										SELECT CASE WHEN T1.C_BOND_TYPE IN ('01','17','19') THEN 'ZGZYZF'
 													 ELSE 'LC' || T1.C_ISSUER_IDENTIFICATION_TYPE || T1.C_ISSUER_IDENTIFICATION_NO
 													 END														 AS CUSTOMERID
 													,CASE WHEN T1.C_BOND_TYPE IN ('01','17','19') THEN '中国中央政府'
 													 ELSE NVL(T1.C_RWA_PUBLISHNAME,T3.C_ORG_NAME)
 													 END														 AS CUSTOMERNAME
 													,CASE WHEN T1.C_BOND_TYPE IN ('01','17','19') THEN '0' --组织机构代码
 													 ELSE T1.C_ISSUER_IDENTIFICATION_TYPE
 													 END														 AS CERTTYPE
 													,CASE WHEN T1.C_BOND_TYPE IN ('01','17','19') THEN 'ZGZYZFZZJGDM'
 													 ELSE T1.C_ISSUER_IDENTIFICATION_NO
 													 END														 AS CERTID
 													,CASE WHEN T1.C_BOND_TYPE IN ('01','17','19') THEN 'CHN'
 													 ELSE T1.C_ISSUER_REGCOUNTRY_CODE
 													 END														 AS COUNTRYCODE
 													,CASE WHEN T1.C_BOND_TYPE IN ('01','17','19') THEN '999999'
 													 ELSE DECODE(T1.C_ISSUER_INDUSTRY_CODE,NULL,'J66','','J66',T1.C_ISSUER_INDUSTRY_CODE)
 													 END														 AS INDUSTRYTYPE
 													,T1.C_ISSUER_ENTERPRISE_SIZE		 AS SCOPE
 													,CASE WHEN T1.C_BOND_TYPE IN ('01','17','19') THEN '01'
 													 ELSE T1.C_SCORE_TYPE
 													 END														 AS ERATINGORG
 													,CASE WHEN T1.C_BOND_TYPE IN ('01','17','19') THEN (SELECT RATINGRESULT FROM RWA.RWA_WP_COUNTRYRATING WHERE COUNTRYCODE = 'CHN' AND ISINUSE = '1')
 													 ELSE T4.DESCRATING
 													 END														 AS ERATING
 													,T1.C_ISSUER_RELEASE_DATE				 AS ERATINGDATE
 													,T1.C_ISSUERTYPE_1							 AS CLIENTTYPE
 													,T1.C_ISSUERTYPE_2							 AS CLIENTSUBTYPE
 											FROM RWA_DEV.ZGS_ATBOND T1
 								INNER JOIN TEMP_INVESTASSETDETAIL T2
 												ON T1.C_BOND_CODE = T2.FLD_ASSET_CODE
                        AND T1.DATANO=p_data_dt_str
 								 LEFT JOIN RWA_DEV.ZGS_ATTYORG T3
 								 				ON T1.C_PUBLISHER = T3.C_ORG_ID
    									 AND T3.DATANO = p_data_dt_str
    						 LEFT JOIN RWA_DEV.RWA_CD_RATING_MAPPING T4
    										ON T1.C_SCORE_TYPE = T4.SRCRATINGORG
    									 AND T1.C_BODY_SCORE = T4.SRCRATING
    									 AND T4.MAPPINGTYPE = 'LCI' 
 										 WHERE T1.DATANO = p_data_dt_str
 											 AND T1.ROWID IN (SELECT MAX(T3.ROWID)
 											 										FROM RWA_DEV.ZGS_ATBOND T3
 											 							INNER JOIN TEMP_INVESTASSETDETAIL T4
 											 											ON T3.C_BOND_CODE = T4.FLD_ASSET_CODE
 											 										 AND T3.DATANO = p_data_dt_str
 											 								GROUP BY CASE WHEN T3.C_BOND_TYPE IN ('01','17','19') THEN '0' ELSE T3.C_ISSUER_IDENTIFICATION_TYPE END,CASE WHEN T3.C_BOND_TYPE IN ('01','17','19') THEN 'ZGZYZFZZJGDM' ELSE T3.C_ISSUER_IDENTIFICATION_NO END)
 		)
 		, TMP_CUST_IRATING AS (
   								SELECT CUSTID,
									       CUSTNAME,
									       ORGCERTCODE,
									       MODELID,
									       PDLEVEL,
									       PDADJLEVEL,
									       PD,
									       PDVAVLIDDATE
									  FROM RWA_DEV.RWA_TEMP_PDLEVEL
									 WHERE ROWID IN
									       (SELECT MAX(ROWID) FROM RWA_DEV.RWA_TEMP_PDLEVEL GROUP BY ORGCERTCODE)
   	)
    SELECT
        DISTINCT
        				 TO_DATE(p_data_dt_str,'YYYYMMDD')     																	AS DataDate            		--数据日期
        				,p_data_dt_str                         																	AS DataNo              		--数据流水号
        				,'LC' || T1.CERTTYPE || T1.CERTID																				AS ClientID            		--参与主体ID
        				,'LC' || T1.CERTTYPE || T1.CERTID																				AS SourceClientID      		--源参与主体ID
        				,'LC'                                 																	AS SSysID              		--源系统ID
        				,T1.CUSTOMERNAME																												AS ClientName          		--参与主体名称
        				,'9998'							              																	AS SOrgID              		--源机构ID
        				,'重庆银行'								      																	AS SOrgName            		--源机构名称
        				,'1'							              																	AS OrgSortNo           		--所属机构排序号
        				,'9998'								             																	AS OrgID               		--所属机构ID
        				,'重庆银行'								      																	AS OrgName             		--所属机构名称
        				,T1.INDUSTRYTYPE										  																	AS IndustryID          		--所属行业代码
        				,T4.ITEMNAME								          																	AS IndustryName        		--所属行业名称
        				,T10.DITEMNO																														AS ClientType          		--参与主体大类
        				,T11.DITEMNO																														AS ClientSubType       		--参与主体小类
        				,CASE WHEN T1.COUNTRYCODE = 'CHN' THEN '01'
        				 ELSE '02'
        				 END	                                 																	AS RegistState         		--注册国家或地区
        				,T7.RATINGRESULT                       																	AS RCERating           		--境外注册地外部评级
        				,'01'                                  																	AS RCERAgency          		--境外注册地外部评级机构
        				,T1.CERTID							              																	AS OrganizationCode    		--组织机构代码
        				,CASE WHEN T1.CERTID = '91522301573318868K' OR REPLACE(T1.CERTID,'-','') = '573318868' THEN '1'
        				 ELSE '0'
        				 END	                                																	AS ConsolidatedSCFlag  		--是否并表子公司
        				,'0'                                  																	AS SLClientFlag        		--专业贷款客户标识
        				,''	                                  																	AS SLClientType        		--专业贷款客户类型
        				,''                                   																	AS ExpoCategoryIRB     		--内评法暴露类别
        				,T8.MODELID                           																	AS ModelID             		--模型ID
        				,T8.PDLEVEL											                         								AS MODELIRATING        		--模型内部评级
                ,T8.PD									                                 								AS MODELPD             		--模型违约概率
                ,T8.PDADJLEVEL										                      								AS IRATING             		--内部评级
                ,T8.PD									                                 								AS PD                  		--违约概率
        				,CASE WHEN T8.PDADJLEVEL = '0116' THEN '1' ELSE '0' END									AS DefaultFlag         		--违约标识
        				,DECODE(NVL(T2.NEWDEFAULTFLAG,'1'),'0','1','0')													AS NewDefaultFlag      		--新增违约标识
        				,CASE WHEN T8.PDADJLEVEL = '0116' THEN TO_DATE(T8.PDVAVLIDDATE,'YYYYMMDD')
                 ELSE NULL
                 END														      																	AS DefaultDate         		--违约时点
        				,T1.ERATING											      																	AS ClientERating       		--参与主体外部评级
        				,'0'                                  																	AS CCPFlag             		--中央交易对手标识
        				,'0'                                   																	AS QualCCPFlag         		--是否合格中央交易对手
        				,'0'                                   																	AS ClearMemberFlag     		--清算会员标识
        				,T1.SCOPE							                																	AS CompanySize         		--企业规模
        				,NVL(T2.ISSUPERVISESTANDARSMENT,'0')   																	AS SSMBFlag            		--标准小微企业标识
        				,'0'											            																	AS SSMBFlagSTD         		--权重法标准小微企业标识
        				,T2.AVEFINANCESUM                      																	AS AnnualSale          		--公司客户年销售额
        				,T1.COUNTRYCODE																													AS CountryCode            --注册国家代码
        				,''																																			AS MSMBFlag								--工信部微小企业标识

    FROM 				TMP_BND_CUST_INFO T1
    LEFT JOIN		TEMP_CUST_INFO T2
    ON					REPLACE(T1.CERTID,'-','') = REPLACE(T2.CERTID,'-','')
    LEFT JOIN		RWA.CODE_LIBRARY T4
    ON					T1.INDUSTRYTYPE = T4.ITEMNO
    AND					T4.CODENO = 'IndustryType'
    LEFT JOIN		RWA.RWA_WP_COUNTRYRATING T7
    ON					T1.COUNTRYCODE = T7.COUNTRYCODE
    AND					T7.ISINUSE = '1'
    LEFT JOIN		TMP_CUST_IRATING T8
    ON					REPLACE(T1.CERTID,'-','') = REPLACE(T8.ORGCERTCODE,'-','')
    LEFT JOIN		RWA_DEV.RWA_CD_CODE_MAPPING T10
	  ON					T1.CLIENTTYPE = T10.SITEMNO
	  AND					T10.SCODENO = 'ClientCategory'
	  AND					T10.SYSID = 'LC'
	  LEFT JOIN		RWA_DEV.RWA_CD_CODE_MAPPING T11
	  ON					T1.CLIENTSUBTYPE = T11.SITEMNO
	  AND					T11.SCODENO = 'ClientCategory'
	  AND					T11.SYSID = 'LC'
   	;

		COMMIT;

    --步骤2 导入【参与主体-资管】
    INSERT INTO RWA_DEV.RWA_LC_CLIENT(
         DataDate                   --数据日期
        ,DataNo                     --数据流水号
        ,ClientID                   --参与主体ID
        ,SourceClientID             --源参与主体ID
        ,SSysID                     --源系统ID
        ,ClientName                 --参与主体名称
        ,SOrgID                     --源机构ID
        ,SOrgName                   --源机构名称
        ,OrgSortNo                  --所属机构排序号
        ,OrgID                      --所属机构ID
        ,OrgName                    --所属机构名称
        ,IndustryID                 --所属行业代码
        ,IndustryName               --所属行业名称
        ,ClientType                 --参与主体大类
        ,ClientSubType              --参与主体小类
        ,RegistState                --注册国家或地区
        ,RCERating                  --境外注册地外部评级
        ,RCERAgency                 --境外注册地外部评级机构
        ,OrganizationCode           --组织机构代码
        ,ConsolidatedSCFlag         --是否并表子公司
        ,SLClientFlag               --专业贷款客户标识
        ,SLClientType               --专业贷款客户类型
        ,ExpoCategoryIRB            --内评法暴露类别
        ,ModelID                    --模型ID
        ,ModelIRating               --模型内部评级
        ,ModelPD                    --模型违约概率
        ,IRating                    --内部评级
        ,PD                         --违约概率
        ,DefaultFlag                --违约标识
        ,NewDefaultFlag             --新增违约标识
        ,DefaultDate                --违约时点
        ,ClientERating              --参与主体外部评级
        ,CCPFlag                    --中央交易对手标识
        ,QualCCPFlag                --是否合格中央交易对手
        ,ClearMemberFlag            --清算会员标识
        ,CompanySize                --企业规模
        ,SSMBFlag                   --标准小微企业标识
        ,SSMBFLAGSTD         				--权重法标准小微企业标识
        ,AnnualSale                 --公司客户年销售额
        ,CountryCode                --注册国家代码
        ,MSMBFlag										--工信部微小企业标识
    )
  	WITH TEMP_INVESTASSETDETAIL AS (
							        SELECT  DISTINCT
							        				T3.FLD_ASSET_CODE						AS FLD_ASSET_CODE
							          FROM RWA_DEV.ZGS_INVESTASSETDETAIL T3
							    INNER JOIN RWA_DEV.ZGS_FINANCING_INFO T4
							            ON T3.FLD_FINANC_CODE = T4.FLD_FINANC_CODE
							           --AND T4.FLD_INCOME_TYPE <> '3' --3：排除非保本类型 --20190625 --该条件过滤导致查询结果为0
							           AND T4.DATANO = p_data_dt_str
                         AND T3.DATANO=T4.DATANO
							         WHERE T3.FLD_ASSET_TYPE = '24'																			-- 2：债券，24：资产管理计划
							           AND T3.FLD_ASSET_STATUS = '1' 																		--1：状态正常
							           AND T3.FLD_ASSET_FLAG = '1'   																		--1：理财产品
							           AND T3.FLD_DATE = p_data_dt_str																	--有效的理财产品其估值日期每日更新
							           AND T3.DATANO = p_data_dt_str
    )
    , TEMP_CUST_INFO AS (
    								SELECT CUSTOMERID
    											,CUSTOMERNAME
    											,ORGID
    											,CERTTYPE
    											,CERTID
    											,INDUSTRYTYPE
    											,COUNTRYCODE
    											,RWACUSTOMERTYPE
    											,NEWDEFAULTFLAG
    											,DEFAULTDATE
    											,SCOPE
    											,ISSUPERVISESTANDARSMENT
    											,AVEFINANCESUM
    									FROM RWA_DEV.NCM_CUSTOMER_INFO
    								 WHERE ROWID IN (SELECT MAX(ROWID) FROM RWA_DEV.NCM_CUSTOMER_INFO WHERE DATANO = p_data_dt_str AND CERTTYPE IN ('Ent01','Ent02') GROUP BY CERTID)
    								 	 AND DATANO = p_data_dt_str
    )
 		, TMP_PLN_CUST_INFO AS (
 										SELECT T1.C_COUNTERPARTY_NAME					 AS CUSTOMERNAME
 													,T1.C_COUNTERPARTY_PAPERTYPE		 AS CERTTYPE
 													,T1.C_COUNTERPARTY_PAPERNO			 AS CERTID
 													,T1.C_COUNTERPARTY_COUNTRYCODE	 AS COUNTRYCODE
 													,T1.C_COUNTERPARTY_INDUSTRYCODE	 AS INDUSTRYTYPE
 													,T1.C_COUNTERPARTY_LNSIZE				 AS SCOPE
 													,T1.C_COUNTERPARTY_FIRST				 AS CLIENTTYPE
 													,T1.C_COUNTERPARTY_SECOND				 AS CLIENTSUBTYPE
 											FROM RWA_DEV.ZGS_ATINTRUST_PLAN T1
 								INNER JOIN TEMP_INVESTASSETDETAIL T2
 												ON T1.C_PRD_CODE = T2.FLD_ASSET_CODE
 										 WHERE T1.DATANO = p_data_dt_str
 											 AND T1.ROWID IN (SELECT MAX(T3.ROWID)
 											 										FROM RWA_DEV.ZGS_ATINTRUST_PLAN T3
 											 							INNER JOIN TEMP_INVESTASSETDETAIL T4
 											 											ON T3.C_PRD_CODE = T4.FLD_ASSET_CODE
 											 										 AND T3.DATANO = p_data_dt_str
 											 								GROUP BY T3.C_COUNTERPARTY_PAPERTYPE,T3.C_COUNTERPARTY_PAPERNO)
 		)
 		, TMP_CUST_IRATING AS (
   								SELECT CUSTID,
									       CUSTNAME,
									       ORGCERTCODE,
									       MODELID,
									       PDLEVEL,
									       PDADJLEVEL,
									       PD,
									       PDVAVLIDDATE
									  FROM RWA_DEV.RWA_TEMP_PDLEVEL
									 WHERE ROWID IN
									       (SELECT MAX(ROWID) FROM RWA_DEV.RWA_TEMP_PDLEVEL GROUP BY ORGCERTCODE)
   	)
    SELECT
        				 TO_DATE(p_data_dt_str,'YYYYMMDD')     																	AS DataDate            		--数据日期
        				,p_data_dt_str                         																	AS DataNo              		--数据流水号
        				,'LC' || T1.CERTTYPE || T1.CERTID																				AS ClientID            		--参与主体ID
        				,'LC' || T1.CERTTYPE || T1.CERTID																				AS SourceClientID      		--源参与主体ID
        				,'LC'                                 																	AS SSysID              		--源系统ID
        				,T1.CUSTOMERNAME																												AS ClientName          		--参与主体名称
        				,'9998'							              																	AS SOrgID              		--源机构ID
        				,'重庆银行'								      																	AS SOrgName            		--源机构名称
        				,'1'							              																	AS OrgSortNo           		--所属机构排序号
        				,'9998'								             																	AS OrgID               		--所属机构ID
        				,'重庆银行'								      																	AS OrgName             		--所属机构名称
        				,T1.INDUSTRYTYPE										  																	AS IndustryID          		--所属行业代码
        				,T4.ITEMNAME								          																	AS IndustryName        		--所属行业名称
        				,T10.DITEMNO																														AS ClientType          		--参与主体大类
        				,T11.DITEMNO																														AS ClientSubType       		--参与主体小类
        				,CASE WHEN T1.COUNTRYCODE = 'CHN' THEN '01'
        				 ELSE '02'
        				 END	                                 																	AS RegistState         		--注册国家或地区
        				,T7.RATINGRESULT                       																	AS RCERating           		--境外注册地外部评级
        				,'01'                                  																	AS RCERAgency          		--境外注册地外部评级机构
        				,T1.CERTID								             																	AS OrganizationCode    		--组织机构代码
        				,CASE WHEN T1.CERTID = '91522301573318868K' OR REPLACE(T1.CERTID,'-','') = '57331886-8' THEN '1'
        				 ELSE '0'
        				 END	                                																	AS ConsolidatedSCFlag  		--是否并表子公司
        				,'0'                                  																	AS SLClientFlag        		--专业贷款客户标识
        				,''	                                  																	AS SLClientType        		--专业贷款客户类型
        				,''                                   																	AS ExpoCategoryIRB     		--内评法暴露类别
        				,T8.MODELID                           																	AS ModelID             		--模型ID
        				,T8.PDLEVEL											                         								AS MODELIRATING        		--模型内部评级
                ,T8.PD									                                 								AS MODELPD             		--模型违约概率
                ,T8.PDADJLEVEL										                      								AS IRATING             		--内部评级
                ,T8.PD									                                 								AS PD                  		--违约概率
        				,CASE WHEN T8.PDADJLEVEL = '0116' THEN '1' ELSE '0' END									AS DefaultFlag         		--违约标识
        				,DECODE(NVL(T2.NEWDEFAULTFLAG,'1'),'0','1','0')													AS NewDefaultFlag      		--新增违约标识
        				,CASE WHEN T8.PDADJLEVEL = '0116' THEN TO_DATE(T8.PDVAVLIDDATE,'YYYYMMDD')
                 ELSE NULL
                 END														      																	AS DefaultDate         		--违约时点
        				,''															      																	AS ClientERating       		--参与主体外部评级
        				,'0'                                  																	AS CCPFlag             		--中央交易对手标识
        				,'0'                                   																	AS QualCCPFlag         		--是否合格中央交易对手
        				,'0'                                   																	AS ClearMemberFlag     		--清算会员标识
        				,T1.SCOPE							                																	AS CompanySize         		--企业规模
        				,NVL(T2.ISSUPERVISESTANDARSMENT,'0')   																	AS SSMBFlag            		--标准小微企业标识
        				,'0'											            																	AS SSMBFlagSTD         		--权重法标准小微企业标识
        				,T2.AVEFINANCESUM                      																	AS AnnualSale          		--公司客户年销售额
        				,T1.COUNTRYCODE																													AS CountryCode            --注册国家代码
        				,''																																			AS MSMBFlag								--工信部微小企业标识

    FROM 				TMP_PLN_CUST_INFO T1
    LEFT JOIN		TEMP_CUST_INFO T2
    ON					REPLACE(T1.CERTID,'-','') = REPLACE(T2.CERTID,'-','')
    LEFT JOIN		RWA.CODE_LIBRARY T4
    ON					T1.INDUSTRYTYPE = T4.ITEMNO
    AND					T4.CODENO = 'IndustryType'
    LEFT JOIN		RWA.RWA_WP_COUNTRYRATING T7
    ON					T1.COUNTRYCODE = T7.COUNTRYCODE
    AND					T7.ISINUSE = '1'
    LEFT JOIN		TMP_CUST_IRATING T8
    ON					REPLACE(T1.CERTID,'-','') = REPLACE(T8.ORGCERTCODE,'-','')
    LEFT JOIN		RWA_DEV.RWA_CD_CODE_MAPPING T10
	  ON					T1.CLIENTTYPE = T10.SITEMNO
	  AND					T10.SCODENO = 'ClientCategory'
	  AND					T10.SYSID = 'LC'
	  LEFT JOIN		RWA_DEV.RWA_CD_CODE_MAPPING T11
	  ON					T1.CLIENTSUBTYPE = T11.SITEMNO
	  AND					T11.SCODENO = 'ClientCategory'
	  AND					T11.SYSID = 'LC'
    WHERE				'LC' || T1.CERTTYPE || T1.CERTID NOT IN (SELECT CLIENTID FROM RWA_DEV.RWA_LC_CLIENT)
    ;

		COMMIT;

		--步骤3 导入【担保人-资管】
		INSERT INTO RWA_DEV.RWA_LC_CLIENT(
         DataDate                   --数据日期
        ,DataNo                     --数据流水号
        ,ClientID                   --参与主体ID
        ,SourceClientID             --源参与主体ID
        ,SSysID                     --源系统ID
        ,ClientName                 --参与主体名称
        ,SOrgID                     --源机构ID
        ,SOrgName                   --源机构名称
        ,OrgSortNo                  --所属机构排序号
        ,OrgID                      --所属机构ID
        ,OrgName                    --所属机构名称
        ,IndustryID                 --所属行业代码
        ,IndustryName               --所属行业名称
        ,ClientType                 --参与主体大类
        ,ClientSubType              --参与主体小类
        ,RegistState                --注册国家或地区
        ,RCERating                  --境外注册地外部评级
        ,RCERAgency                 --境外注册地外部评级机构
        ,OrganizationCode           --组织机构代码
        ,ConsolidatedSCFlag         --是否并表子公司
        ,SLClientFlag               --专业贷款客户标识
        ,SLClientType               --专业贷款客户类型
        ,ExpoCategoryIRB            --内评法暴露类别
        ,ModelID                    --模型ID
        ,ModelIRating               --模型内部评级
        ,ModelPD                    --模型违约概率
        ,IRating                    --内部评级
        ,PD                         --违约概率
        ,DefaultFlag                --违约标识
        ,NewDefaultFlag             --新增违约标识
        ,DefaultDate                --违约时点
        ,ClientERating              --参与主体外部评级
        ,CCPFlag                    --中央交易对手标识
        ,QualCCPFlag                --是否合格中央交易对手
        ,ClearMemberFlag            --清算会员标识
        ,CompanySize                --企业规模
        ,SSMBFlag                   --标准小微企业标识
        ,SSMBFLAGSTD         				--权重法标准小微企业标识
        ,AnnualSale                 --公司客户年销售额
        ,CountryCode                --注册国家代码
        ,MSMBFlag										--工信部微小企业标识
    )
  	WITH TEMP_INVESTASSETDETAIL AS (
							        SELECT  DISTINCT
							        				T3.FLD_ASSET_CODE						AS FLD_ASSET_CODE
							          FROM RWA_DEV.ZGS_INVESTASSETDETAIL T3
							    INNER JOIN RWA_DEV.ZGS_FINANCING_INFO T4
							            ON T3.FLD_FINANC_CODE = T4.FLD_FINANC_CODE
							           AND T4.FLD_INCOME_TYPE <> '3'																		--3：排除非保本类型
							           AND T4.DATANO = p_data_dt_str
							         WHERE T3.FLD_ASSET_TYPE = '24'																			-- 2：债券，24：资产管理计划
							           AND T3.FLD_ASSET_STATUS = '1' 																		--1：状态正常
							           AND T3.FLD_ASSET_FLAG = '1'   																		--1：理财产品
							           AND T3.FLD_DATE = p_data_dt_str																	--有效的理财产品其估值日期每日更新
							           AND T3.DATANO = p_data_dt_str
    )
    , TEMP_CUST_INFO AS (
    								SELECT CUSTOMERID
    											,CUSTOMERNAME
    											,ORGID
    											,CERTTYPE
    											,CERTID
    											,INDUSTRYTYPE
    											,COUNTRYCODE
    											,RWACUSTOMERTYPE
    											,NEWDEFAULTFLAG
    											,DEFAULTDATE
    											,SCOPE
    											,ISSUPERVISESTANDARSMENT
    											,AVEFINANCESUM
    									FROM RWA_DEV.NCM_CUSTOMER_INFO
    								 WHERE ROWID IN (SELECT MAX(ROWID) FROM RWA_DEV.NCM_CUSTOMER_INFO WHERE DATANO = p_data_dt_str AND CERTTYPE IN ('Ent01','Ent02') GROUP BY CERTID)
    								 	 AND DATANO = p_data_dt_str
    )
 		, TMP_PLN_CUST_INFO AS (
 										SELECT T1.C_GUARANTOR_NAME						 AS CUSTOMERNAME
 													,T1.C_GUARANTOR_PAPERTYPE				 AS CERTTYPE
 													,T1.C_GUARANTOR_NO							 AS CERTID  --20190625 ZGS_ATINTRUST_PLAN表C_GUARANTOR_NO全为空
 													,T1.C_GUARANTOR_COUNTRY					 AS COUNTRYCODE
 													,'999999'												 AS INDUSTRYTYPE
 													,T1.C_GUARANTOR_TYPE						 AS CLIENTTYPE
 													,T1.C_GUARANTOR_TYPETWO					 AS CLIENTSUBTYPE
 											FROM RWA_DEV.ZGS_ATINTRUST_PLAN T1
 								INNER JOIN TEMP_INVESTASSETDETAIL T2
 												ON T1.C_PRD_CODE = T2.FLD_ASSET_CODE
 										 WHERE T1.DATANO = p_data_dt_str
 											 AND T1.ROWID IN (SELECT MAX(T3.ROWID)
 											 										FROM RWA_DEV.ZGS_ATINTRUST_PLAN T3
 											 							INNER JOIN TEMP_INVESTASSETDETAIL T4
 											 											ON T3.C_PRD_CODE = T4.FLD_ASSET_CODE
 											 										 AND T3.DATANO = p_data_dt_str
 											 										 AND T3.C_GUARANTOR_NO IS NOT NULL ----20190625该条件过滤导致查询结果为0 
 											 								GROUP BY T3.C_GUARANTOR_PAPERTYPE,T3.C_GUARANTOR_NO)
 		)
 		, TMP_CUST_IRATING AS (
   								SELECT CUSTID,
									       CUSTNAME,
									       ORGCERTCODE,
									       MODELID,
									       PDLEVEL,
									       PDADJLEVEL,
									       PD,
									       PDVAVLIDDATE
									  FROM RWA_DEV.RWA_TEMP_PDLEVEL
									 WHERE ROWID IN
									       (SELECT MAX(ROWID) FROM RWA_DEV.RWA_TEMP_PDLEVEL GROUP BY ORGCERTCODE)
   	)
    SELECT
        				 TO_DATE(p_data_dt_str,'YYYYMMDD')     																	AS DataDate            		--数据日期
        				,p_data_dt_str                         																	AS DataNo              		--数据流水号
        				,'LC' || T1.CERTTYPE || T1.CERTID																				AS ClientID            		--参与主体ID
        				,'LC' || T1.CERTTYPE || T1.CERTID																				AS SourceClientID      		--源参与主体ID
        				,'LC'                                 																	AS SSysID              		--源系统ID
        				,T1.CUSTOMERNAME																												AS ClientName          		--参与主体名称
        				,'9998'							              																	AS SOrgID              		--源机构ID
        				,'重庆银行'								      																	AS SOrgName            		--源机构名称
        				,'1'							              																	AS OrgSortNo           		--所属机构排序号
        				,'9998'								             																	AS OrgID               		--所属机构ID
        				,'重庆银行'								      																	AS OrgName             		--所属机构名称
        				,T1.INDUSTRYTYPE										  																	AS IndustryID          		--所属行业代码
        				,T4.ITEMNAME								          																	AS IndustryName        		--所属行业名称
        				,T10.DITEMNO																														AS ClientType          		--参与主体大类
        				,T11.DITEMNO																														AS ClientSubType       		--参与主体小类
        				,CASE WHEN T1.COUNTRYCODE = 'CHN' THEN '01'
        				 ELSE '02'
        				 END	                                 																	AS RegistState         		--注册国家或地区
        				,T7.RATINGRESULT                       																	AS RCERating           		--境外注册地外部评级
        				,'01'                                  																	AS RCERAgency          		--境外注册地外部评级机构
        				,T1.CERTID							              																	AS OrganizationCode    		--组织机构代码
        				,CASE WHEN T1.CERTID = '91522301573318868K' OR REPLACE(T1.CERTID,'-','') = '57331886-8' THEN '1'
        				 ELSE '0'
        				 END	                                																	AS ConsolidatedSCFlag  		--是否并表子公司
        				,'0'                                  																	AS SLClientFlag        		--专业贷款客户标识
        				,''	                                  																	AS SLClientType        		--专业贷款客户类型
        				,''                                   																	AS ExpoCategoryIRB     		--内评法暴露类别
        				,T8.MODELID                           																	AS ModelID             		--模型ID
        				,T8.PDLEVEL											                         								AS MODELIRATING        		--模型内部评级
                ,T8.PD									                                 								AS MODELPD             		--模型违约概率
                ,T8.PDADJLEVEL										                      								AS IRATING             		--内部评级
                ,T8.PD                            									     								AS PD                  		--违约概率
        				,CASE WHEN T8.PDADJLEVEL = '0116' THEN '1' ELSE '0' END									AS DefaultFlag         		--违约标识
        				,DECODE(NVL(T2.NEWDEFAULTFLAG,'1'),'0','1','0')													AS NewDefaultFlag      		--新增违约标识
        				,CASE WHEN T8.PDADJLEVEL = '0116' THEN TO_DATE(T8.PDVAVLIDDATE,'YYYYMMDD')
                 ELSE NULL
                 END														      																	AS DefaultDate         		--违约时点
        				,''															      																	AS ClientERating       		--参与主体外部评级
        				,'0'                                  																	AS CCPFlag             		--中央交易对手标识
        				,'0'                                   																	AS QualCCPFlag         		--是否合格中央交易对手
        				,'0'                                   																	AS ClearMemberFlag     		--清算会员标识
        				,T2.SCOPE							                																	AS CompanySize         		--企业规模
        				,NVL(T2.ISSUPERVISESTANDARSMENT,'0')   																	AS SSMBFlag            		--标准小微企业标识
        				,'0'											            																	AS SSMBFlagSTD         		--权重法标准小微企业标识
        				,T2.AVEFINANCESUM                      																	AS AnnualSale          		--公司客户年销售额
        				,T1.COUNTRYCODE																													AS CountryCode            --注册国家代码
        				,''																																			AS MSMBFlag								--工信部微小企业标识

    FROM 				TMP_PLN_CUST_INFO T1
    LEFT JOIN		TEMP_CUST_INFO T2
    ON					REPLACE(T1.CERTID,'-','') = REPLACE(T2.CERTID,'-','')
    LEFT JOIN		RWA.CODE_LIBRARY T4
    ON					T1.INDUSTRYTYPE = T4.ITEMNO
    AND					T4.CODENO = 'IndustryType'
    LEFT JOIN		RWA.RWA_WP_COUNTRYRATING T7
    ON					T1.COUNTRYCODE = T7.COUNTRYCODE
    AND					T7.ISINUSE = '1'
    LEFT JOIN		TMP_CUST_IRATING T8
    ON					REPLACE(T1.CERTID,'-','') = REPLACE(T8.ORGCERTCODE,'-','')
    LEFT JOIN		RWA_DEV.RWA_CD_CODE_MAPPING T10
	  ON					T1.CLIENTTYPE = T10.SITEMNO
	  AND					T10.SCODENO = 'ClientCategory'
	  AND					T10.SYSID = 'LC'
	  LEFT JOIN		RWA_DEV.RWA_CD_CODE_MAPPING T11
	  ON					T1.CLIENTSUBTYPE = T11.SITEMNO
	  AND					T11.SCODENO = 'ClientCategory'
	  AND					T11.SYSID = 'LC'
    WHERE				'LC' || T1.CERTTYPE || T1.CERTID NOT IN (SELECT CLIENTID FROM RWA_DEV.RWA_LC_CLIENT)
    ;

		COMMIT;

		dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_LC_CLIENT',cascade => true);


    /*目标表数据统计*/
    --统计插入的记录
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_LC_CLIENT;
    --DBMS_OUTPUT.PUT_LINE('RWA_DEV.RWA_LC_CLIENT，中插入数量为：' || v_count || '条');
    --DBMS_OUTPUT.PUT_LINE('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功' || '-' || v_count;

    commit;
    --定义异常
    EXCEPTION WHEN OTHERS THEN
    		--DBMS_OUTPUT.PUT_LINE('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
        ROLLBACK;
        p_po_rtncode := sqlcode;
        p_po_rtnmsg  := '理财资管系统-参与主体('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
				RETURN;
END PRO_RWA_LC_CLIENT;
/

