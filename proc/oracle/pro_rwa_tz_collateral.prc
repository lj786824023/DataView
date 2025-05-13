CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_TZ_COLLATERAL(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期 yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_TZ_COLLATERAL
    实现功能:财务系统-投资-抵质押品(从数据源财务系统将应收款投资相关信息全量导入RWA投资类接口表抵质押品表中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :LISY
    编写时间:2016-04-21
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.FNS_BND_INFO_B|财务系统债券信息表
    源  表2 :RWA_DEV.FNS_BND_BOOK_B|财务系统账面活动表
    源  表3 :RWA.RWA_WS_RECEIVABLE|应收款投资补录表
    源  表4 :RWA.RWA_WP_DATASUPPLEMENT|补录任务明细信息表
    源  表5 :RWA_DEV.CBS_BND|债券投资登记簿
    源  表6 :RWA_DEV.CBS_IAC|通用分户帐
    源  表7 :RWA.RWA_WS_B_RECEIVABLE|买入返售其他金融资产_应收账款投资补录表
    源  表8 :RWA.RWA_WSIB_ABS_ISSUE_EXPOSURE|发行机构资产证券化暴露铺底表
    源  表9 :RWA.RWA_WSIB_ABS_INVEST_EXPOSURE|投资机构资产证券化暴露铺底表
    目标表1 :RWA_DEV.RWA_TZ_COLLATERAL|财务系统投资类抵质押品表
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_TZ_COLLATERAL';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TZ_COLLATERAL';

    --2.将满足条件的数据从源表插入到目标表中
    --2.1 财务系统-应收款投资-抵质押品-非基于银行-合同
    INSERT INTO RWA_DEV.RWA_TZ_COLLATERAL(
                 DataDate                              --数据日期
                ,DataNo                                --数据流水号
                ,CollateralID                       	 --抵质押品ID
                ,SSysID                             	 --源系统ID
                ,SGuarContractID                    	 --源担保合同ID
                ,SCollateralID                      	 --源抵质押品ID
                ,CollateralName                     	 --抵质押品名称
                ,IssuerID                           	 --发行人ID
                ,ProviderID                         	 --提供人ID
                ,CreditRiskDataType                 	 --信用风险数据类型
                ,GuaranteeWay                       	 --担保方式
                ,SourceColType                      	 --源抵质押品大类
                ,SourceColSubType                   	 --源抵质押品小类
                ,SpecPurpBondFlag                   	 --是否为收购国有银行不良贷款而发行的债券
                ,QualFlagSTD                        	 --权重法合格标识
                ,QualFlagFIRB                 				 --内评初级法合格标识
                ,CollateralTypeSTD                  	 --权重法抵质押品类型
                ,CollateralSdvsSTD                  	 --权重法抵质押品细分
                ,CollateralTypeIRB                  	 --内评法抵质押品类型
                ,CollateralAmount                   	 --抵押总额
                ,Currency                           	 --币种
                ,StartDate                          	 --起始日期
                ,DueDate                            	 --到期日期
                ,OriginalMaturity                   	 --原始期限
                ,ResidualM                          	 --剩余期限
                ,InteHaircutsFlag                   	 --自行估计折扣系数标识
                ,InternalHc                         	 --内部折扣系数
                ,FCType                             	 --金融质押品类型
                ,ABSFlag                            	 --资产证券化标识
                ,RatingDurationType                 	 --评级期限类型
                ,FCIssueRating     										 --金融质押品发行等级
                ,FCIssuerType                          --金融质押品发行人类别
                ,FCIssuerState                         --金融质押品发行人注册国家
                ,FCResidualM                           --金融质押品剩余期限
                ,RevaFrequency                         --重估频率
                ,GroupID                               --分组编号
                ,RCERating														 --发行人境外注册地外部评级
    )
    WITH TMP_BND_CONTRACT AS (
							SELECT
										 T1.SCONTRACTID 			AS CONTRACTNO
										,MIN(T1.STARTDATE)		AS STARTDATE
										,MAX(T1.DUEDATE)			AS DUEDATE
								FROM RWA_DEV.RWA_TZ_CONTRACT T1	 																--投资合同表
					INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT TC														--信贷合同表
									ON T1.SCONTRACTID = TC.SERIALNO
								 AND (TC.BUSINESSSUBTYPE NOT LIKE '0010%' OR TC.BUSINESSSUBTYPE IS NULL) 			--非基于银行
								 AND TC.DATANO = p_data_dt_str
	    				 WHERE T1.BUSINESSTYPEID IN ('1040105060','1040105061','1040105062')	--应收款投资业务
	    			GROUP BY T1.SCONTRACTID
		)
		, TEMP_BND_COLLATERAL AS (
													SELECT T5.GUARANTYID
																,MIN(T1.STARTDATE)		AS STARTDATE
																,MAX(T1.DUEDATE)			AS DUEDATE
													  FROM TMP_BND_CONTRACT T1
											INNER JOIN (SELECT DISTINCT
																				 SERIALNO
																				,OBJECTNO
																		FROM RWA_DEV.NCM_CONTRACT_RELATIVE
						   										 WHERE OBJECTTYPE = 'GuarantyContract'
						     										 AND DATANO = p_data_dt_str) T2                       --信贷合同关联表
							    						ON T1.CONTRACTNO = T2.SERIALNO
									    INNER JOIN RWA_DEV.NCM_GUARANTY_CONTRACT T3													--信贷担保合同表
									    				ON T2.OBJECTNO = T3.SERIALNO
									    			 AND T3.DATANO = p_data_dt_str
									    INNER JOIN (SELECT DISTINCT
									    									 CONTRACTNO
									    									,GUARANTYID
									    							FROM RWA_DEV.NCM_GUARANTY_RELATIVE
									    						 WHERE DATANO = p_data_dt_str
									    						) T4																										--信贷担保合同与抵质押品关联表
    													ON T3.SERIALNO = T4.CONTRACTNO
									    INNER JOIN RWA_DEV.NCM_GUARANTY_INFO T5															--信贷抵质押品信息表
									    				ON T4.GUARANTYID = T5.GUARANTYID
									    			 AND T5.GUARANTYTYPEID NOT IN ('004001004001','004001005001','004001006001','004001006002','001001003001')   --信用证、备用信用证、融资性保函、非融资性保函都归为保证，保证金不取
									    			 AND T5.CLRSTATUS = '01'																			--押品实物状态：正常
    						 						 AND T5.CLRGNTSTATUS IN ('03','10')														--押品设押状态：03-已确立押权，10-已入库
									    			 AND T5.DATANO = p_data_dt_str
												GROUP BY T5.GUARANTYID
		)
		, TEMP_CUST_INFO AS (
    								SELECT CUSTOMERID, CERTTYPE, CERTID
                      FROM (SELECT T1.CUSTOMERID,
                                   T1.CERTTYPE,
                                   T1.CERTID,
                                   ROW_NUMBER() OVER(PARTITION BY T1.CERTTYPE, T1.CERTID ORDER BY T1.CUSTOMERID) AS RM
                              FROM RWA_DEV.NCM_CUSTOMER_INFO T1
                             WHERE EXISTS
                             (SELECT 1
                                      FROM RWA_DEV.NCM_GUARANTY_INFO T2
                                     WHERE T1.CERTID = T2.OBLIGEEIDNUMBER
                                       AND T2.DATANO = p_data_dt_str
                                       AND T2.GUARANTYTYPEID NOT IN
                                           ('004001004001',
                                            '004001005001',
                                            '004001006001',
                                            '004001006002',
                                            '001001003001')
                                       AND T2.AFFIRMVALUE0 > 0)
                               AND T1.DATANO = p_data_dt_str)
                     WHERE RM = 1
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DataDate                 --数据日期
                ,p_data_dt_str													     AS DataNo                   --数据流水号
                ,'YP' || T1.GUARANTYID											 AS CollateralID           	 --抵质押品ID
                ,'TZ'                  											 AS SSysID              	 	 --源系统ID
                ,''																					 AS SGuarContractID        	 --源担保合同ID                             默认 NULL
                ,T1.GUARANTYID															 AS SCollateralID          	 --源抵质押品ID
                ,T7.ITEMNAME		                   			 		 AS CollateralName         	 --抵质押品名称
                ,T3.OPENBANKNO                               AS IssuerID             	 	 --发行人ID
                ,T6.CUSTOMERID                           	 	 AS ProviderID             	 --提供人ID
                ,'01'                                  			 AS CreditRiskDataType     	 --信用风险数据类型                         默认 一般非零售(01)
                ,T1.GUARANTYTYPE                 		 				 AS GuaranteeWay           	 --担保方式
                ,SUBSTR(T1.GUARANTYTYPEID,1,6)       				 AS SourceColType     	     --源抵质押品大类
                ,T1.GUARANTYTYPEID										       AS SourceColSubType         --源抵质押品小类
                ,CASE WHEN SUBSTR(NVL(T3.BONDPUBLISHPURPOSE,'0020'),2,2) = '01' THEN '1'
                 ELSE '0'
                 END														             AS SpecPurpBondFlag  			 --是否为收购国有银行不良贷款而发行的债券
                ,''              									 			 		 AS QualFlagSTD            	 --权重法合格标识                           默认 NULL RWA规则处理
                ,CASE WHEN T1.QUALIFYFLAG03 = '01' THEN '1'
                			WHEN T1.QUALIFYFLAG03 = '02' THEN '0'
                 			ELSE ''
                 END                			 			 						 AS QualFlagFIRB           	 --内评初级法合格标识                       默认 NULL RWA规则处理
                ,''							              				 			 AS CollateralTypeSTD 			 --权重法抵质押品类型                    		默认 NULL RWA规则处理
                ,''			                          					 AS CollateralSdvsSTD 		 	 --权重法抵质押品细分                    		默认 NULL RWA规则处理
                ,CASE WHEN T1.QUALIFYFLAG03 = '01' THEN T4.SLOWRELEASETYPE
                 ELSE ''
                 END							 		 									 		 AS CollateralTypeIRB      	 --内评法抵质押品类型                       默认 NULL RWA规则处理
                ,T1.AFFIRMVALUE0		 				 								 AS CollateralAmount     	 	 --抵押总额
                ,NVL(T1.AFFIRMCURRENCY,'CNY')								 AS Currency               	 --币种
								,T2.STARTDATE       												 AS StartDate         			 --起始日期
                ,T2.DUEDATE													         AS DueDate                	 --到期日期
                ,CASE WHEN (TO_DATE(T2.DUEDATE,'YYYYMMDD') - TO_DATE(T2.STARTDATE,'YYYYMMDD')) / 365 < 0	THEN 0
                 ELSE (TO_DATE(T2.DUEDATE,'YYYYMMDD') - TO_DATE(T2.STARTDATE,'YYYYMMDD')) / 365
                 END					                             	 AS OriginalMaturity    	 	 --原始期限
                ,CASE WHEN (TO_DATE(T2.DUEDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365 < 0	THEN 0
                 ELSE (TO_DATE(T2.DUEDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 END                                         AS ResidualM           	 	 --剩余期限
                ,'0'                                         AS InteHaircutsFlag    	 	 --自行估计折扣系数标识                     默认 否(0)
                ,NULL                                        AS InternalHc          	 	 --内部折扣系数                             默认 NULL
                ,CASE WHEN T1.QUALIFYFLAG03 = '01' THEN T4.FINANCIALPLEDGETYPE
                 ELSE ''
                 END                                         AS FCType                 	 --金融质押品类型                           默认 NULL RWA规则处理
                ,CASE WHEN T3.ABSFLAG = '01' THEN '1'
                 ELSE '0'
                 END			                                   AS ABSFlag             	 	 --资产证券化标识
                ,T3.TIMELIMIT                                AS RatingDurationType  	 	 --评级期限类型                             需转换为长期、短期
                ,RWA_DEV.GETSTANDARDRATING1(T3.BONDRATING)   AS FCIssueRating     			 --金融质押品发行等级                    		需转换为标普
                ,CASE WHEN T3.OPENBANKTYPE LIKE '01%' OR T3.OPENBANKTYPE LIKE '10%' THEN '01'
                 ELSE '02'
                 END						                             AS FCIssuerType             --金融质押品发行人类别                  		根据发行人客户类型是否为主权来判断
                ,CASE WHEN NVL(T3.BONDPUBLISHCOUNTRY,'CHN') = 'CHN' THEN '01'
                 ELSE '02'
                 END									                     	 AS FCIssuerState            --金融质押品发行人注册国家
                ,CASE WHEN (TO_DATE(T2.DUEDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365 < 0	THEN 0
                 ELSE (TO_DATE(T2.DUEDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 END                                         AS FCResidualM              --金融质押品剩余期限
                ,1                                           AS RevaFrequency            --重估频率                              		默认 1
                ,''                                          AS GroupID                  --分组编号                              		默认 NULL
                ,T5.RATINGRESULT														 AS RCERating								 --发行人境外注册地外部评级

    FROM				RWA_DEV.NCM_GUARANTY_INFO T1															--信贷抵质押品信息表
    INNER JOIN	TEMP_BND_COLLATERAL	T2
   	ON					T1.GUARANTYID = T2.GUARANTYID
    LEFT JOIN		RWA_DEV.NCM_ASSET_FINANCE T3															--信贷金融质押品信息表
    ON					T1.GUARANTYID = T3.GUARANTYID
    AND					T3.DATANO = p_data_dt_str
    LEFT JOIN		RWA_DEV.NCM_COL_PARAM T4
    ON					T1.GUARANTYTYPEID = T4.GUARANTYTYPE
    AND					T4.DATANO = p_data_dt_str
    LEFT JOIN		RWA.RWA_WP_COUNTRYRATING T5
    ON					NVL(T3.BONDPUBLISHCOUNTRY,'CHN') = T5.COUNTRYCODE
    AND					T5.ISINUSE = '1'
    LEFT JOIN		TEMP_CUST_INFO T6
    ON					T1.OBLIGEEIDTYPE = T6.CERTTYPE
    AND 				T1.OBLIGEEIDNUMBER = T6.CERTID
    LEFT JOIN		RWA.CODE_LIBRARY T7
    ON					T1.GUARANTYTYPEID = T7.ITEMNO
    AND					T7.CODENO = 'GuarantyList'
		WHERE 			T1.GUARANTYTYPEID NOT IN ('004001004001','004001005001','004001006001','004001006002','001001003001')
																																					--信用证、备用信用证、融资性保函、非融资性保函都归为保证，保证金不取
		AND 				T1.AFFIRMVALUE0 > 0
		AND 				T1.DATANO = p_data_dt_str
		;

    COMMIT;

    --2.2 财务系统-应收款投资-抵质押品-非基于银行-出账
    INSERT INTO RWA_DEV.RWA_TZ_COLLATERAL(
                 DataDate                              --数据日期
                ,DataNo                                --数据流水号
                ,CollateralID                       	 --抵质押品ID
                ,SSysID                             	 --源系统ID
                ,SGuarContractID                    	 --源担保合同ID
                ,SCollateralID                      	 --源抵质押品ID
                ,CollateralName                     	 --抵质押品名称
                ,IssuerID                           	 --发行人ID
                ,ProviderID                         	 --提供人ID
                ,CreditRiskDataType                 	 --信用风险数据类型
                ,GuaranteeWay                       	 --担保方式
                ,SourceColType                      	 --源抵质押品大类
                ,SourceColSubType                   	 --源抵质押品小类
                ,SpecPurpBondFlag                   	 --是否为收购国有银行不良贷款而发行的债券
                ,QualFlagSTD                        	 --权重法合格标识
                ,QualFlagFIRB                 				 --内评初级法合格标识
                ,CollateralTypeSTD                  	 --权重法抵质押品类型
                ,CollateralSdvsSTD                  	 --权重法抵质押品细分
                ,CollateralTypeIRB                  	 --内评法抵质押品类型
                ,CollateralAmount                   	 --抵押总额
                ,Currency                           	 --币种
                ,StartDate                          	 --起始日期
                ,DueDate                            	 --到期日期
                ,OriginalMaturity                   	 --原始期限
                ,ResidualM                          	 --剩余期限
                ,InteHaircutsFlag                   	 --自行估计折扣系数标识
                ,InternalHc                         	 --内部折扣系数
                ,FCType                             	 --金融质押品类型
                ,ABSFlag                            	 --资产证券化标识
                ,RatingDurationType                 	 --评级期限类型
                ,FCIssueRating     										 --金融质押品发行等级
                ,FCIssuerType                          --金融质押品发行人类别
                ,FCIssuerState                         --金融质押品发行人注册国家
                ,FCResidualM                           --金融质押品剩余期限
                ,RevaFrequency                         --重估频率
                ,GroupID                               --分组编号
                ,RCERating														 --发行人境外注册地外部评级
    )
    WITH TMP_BND_CONTRACT AS (
							SELECT
										 T1.SCONTRACTID 			AS CONTRACTNO
										,MIN(T1.STARTDATE)		AS STARTDATE
										,MAX(T1.DUEDATE)			AS DUEDATE
								FROM RWA_DEV.RWA_TZ_CONTRACT T1	 																--投资合同表
					INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT TC														--信贷合同表
									ON T1.SCONTRACTID = TC.SERIALNO
								 AND (TC.BUSINESSSUBTYPE NOT LIKE '0010%' OR TC.BUSINESSSUBTYPE IS NULL) 			--非基于银行
								 AND TC.DATANO = p_data_dt_str
	    				 WHERE T1.BUSINESSTYPEID IN ('1040105060','1040105061','1040105062')	--应收款投资业务
	    			GROUP BY T1.SCONTRACTID
		)
		, TEMP_BND_COLLATERAL AS (
													SELECT T5.GUARANTYID
																,MIN(T1.STARTDATE)		AS STARTDATE
																,MAX(T1.DUEDATE)			AS DUEDATE
													  FROM TMP_BND_CONTRACT T1
											INNER JOIN RWA_DEV.NCM_BUSINESS_PUTOUT T2											--出账表
															ON T1.CONTRACTNO = T2.CONTRACTSERIALNO
														 AND T2.DATANO = p_data_dt_str
											INNER JOIN RWA_DEV.NCM_GUARANTY_RELATIVE T3										--押品关联表
															ON T2.SERIALNO = T3.OBJECTNO
														 AND T3.OBJECTTYPE = 'PutOutApply'
														 AND T3.DATANO = p_data_dt_str
									    INNER JOIN RWA_DEV.NCM_GUARANTY_INFO T5															--信贷抵质押品信息表
									    				ON T3.GUARANTYID = T5.GUARANTYID
									    			 AND T5.GUARANTYTYPEID NOT IN ('004001004001','004001005001','004001006001','004001006002','001001003001')   --信用证、备用信用证、融资性保函、非融资性保函都归为保证，保证金不取
									    			 AND T5.CLRSTATUS = '01'																			--押品实物状态：正常
    						 						 AND T5.CLRGNTSTATUS IN ('03','10')														--押品设押状态：03-已确立押权，10-已入库
									    			 AND T5.DATANO = p_data_dt_str
												GROUP BY T5.GUARANTYID
		)
		, TEMP_CUST_INFO AS (
    								SELECT CUSTOMERID, CERTTYPE, CERTID
                      FROM (SELECT T1.CUSTOMERID,
                                   T1.CERTTYPE,
                                   T1.CERTID,
                                   ROW_NUMBER() OVER(PARTITION BY T1.CERTTYPE, T1.CERTID ORDER BY T1.CUSTOMERID) AS RM
                              FROM RWA_DEV.NCM_CUSTOMER_INFO T1
                             WHERE EXISTS
                             (SELECT 1
                                      FROM RWA_DEV.NCM_GUARANTY_INFO T2
                                     WHERE T1.CERTID = T2.OBLIGEEIDNUMBER
                                       AND T2.DATANO = p_data_dt_str
                                       AND T2.GUARANTYTYPEID NOT IN
                                           ('004001004001',
                                            '004001005001',
                                            '004001006001',
                                            '004001006002',
                                            '001001003001')
                                       AND T2.AFFIRMVALUE0 > 0)
                               AND T1.DATANO = p_data_dt_str)
                     WHERE RM = 1
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DataDate                 --数据日期
                ,p_data_dt_str													     AS DataNo                   --数据流水号
                ,'YP' || T1.GUARANTYID											 AS CollateralID           	 --抵质押品ID
                ,'TZ'                  											 AS SSysID              	 	 --源系统ID
                ,''																					 AS SGuarContractID        	 --源担保合同ID                             默认 NULL
                ,T1.GUARANTYID															 AS SCollateralID          	 --源抵质押品ID
                ,T7.ITEMNAME		                   			 		 AS CollateralName         	 --抵质押品名称
                ,T3.OPENBANKNO                               AS IssuerID             	 	 --发行人ID
                ,T6.CUSTOMERID                           	 	 AS ProviderID             	 --提供人ID
                ,'01'                                  			 AS CreditRiskDataType     	 --信用风险数据类型                         默认 一般非零售(01)
                ,T1.GUARANTYTYPE                 		 				 AS GuaranteeWay           	 --担保方式
                ,SUBSTR(T1.GUARANTYTYPEID,1,6)       				 AS SourceColType     	     --源抵质押品大类
                ,T1.GUARANTYTYPEID										       AS SourceColSubType         --源抵质押品小类
                ,CASE WHEN SUBSTR(NVL(T3.BONDPUBLISHPURPOSE,'0020'),2,2) = '01' THEN '1'
                 ELSE '0'
                 END														             AS SpecPurpBondFlag  			 --是否为收购国有银行不良贷款而发行的债券
                ,''              									 			 		 AS QualFlagSTD            	 --权重法合格标识                           默认 NULL RWA规则处理
                ,CASE WHEN T1.QUALIFYFLAG03 = '01' THEN '1'
                			WHEN T1.QUALIFYFLAG03 = '02' THEN '0'
                 			ELSE ''
                 END                			 			 						 AS QualFlagFIRB           	 --内评初级法合格标识                       默认 NULL RWA规则处理
                ,''							              				 			 AS CollateralTypeSTD 			 --权重法抵质押品类型                    		默认 NULL RWA规则处理
                ,''			                          					 AS CollateralSdvsSTD 		 	 --权重法抵质押品细分                    		默认 NULL RWA规则处理
                ,CASE WHEN T1.QUALIFYFLAG03 = '01' THEN T4.SLOWRELEASETYPE
                 ELSE ''
                 END							 		 									 		 AS CollateralTypeIRB      	 --内评法抵质押品类型                       默认 NULL RWA规则处理
                ,T1.AFFIRMVALUE0		 				 								 AS CollateralAmount     	 	 --抵押总额
                ,NVL(T1.AFFIRMCURRENCY,'CNY')								 AS Currency               	 --币种
								,T2.STARTDATE       												 AS StartDate         			 --起始日期
                ,T2.DUEDATE													         AS DueDate                	 --到期日期
                ,CASE WHEN (TO_DATE(T2.DUEDATE,'YYYYMMDD') - TO_DATE(T2.STARTDATE,'YYYYMMDD')) / 365 < 0	THEN 0
                 ELSE (TO_DATE(T2.DUEDATE,'YYYYMMDD') - TO_DATE(T2.STARTDATE,'YYYYMMDD')) / 365
                 END					                             	 AS OriginalMaturity    	 	 --原始期限
                ,CASE WHEN (TO_DATE(T2.DUEDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365 < 0	THEN 0
                 ELSE (TO_DATE(T2.DUEDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 END                                         AS ResidualM           	 	 --剩余期限
                ,'0'                                         AS InteHaircutsFlag    	 	 --自行估计折扣系数标识                     默认 否(0)
                ,NULL                                        AS InternalHc          	 	 --内部折扣系数                             默认 NULL
                ,CASE WHEN T1.QUALIFYFLAG03 = '01' THEN T4.FINANCIALPLEDGETYPE
                 ELSE ''
                 END                                         AS FCType                 	 --金融质押品类型                           默认 NULL RWA规则处理
                ,CASE WHEN T3.ABSFLAG = '01' THEN '1'
                 ELSE '0'
                 END			                                   AS ABSFlag             	 	 --资产证券化标识
                ,T3.TIMELIMIT                                AS RatingDurationType  	 	 --评级期限类型                             需转换为长期、短期
                ,RWA_DEV.GETSTANDARDRATING1(T3.BONDRATING)   AS FCIssueRating     			 --金融质押品发行等级                    		需转换为标普
                ,CASE WHEN T3.OPENBANKTYPE LIKE '01%' OR T3.OPENBANKTYPE LIKE '10%' THEN '01'
                 ELSE '02'
                 END						                             AS FCIssuerType             --金融质押品发行人类别                  		根据发行人客户类型是否为主权来判断
                ,CASE WHEN NVL(T3.BONDPUBLISHCOUNTRY,'CHN') = 'CHN' THEN '01'
                 ELSE '02'
                 END									                     	 AS FCIssuerState            --金融质押品发行人注册国家
                ,CASE WHEN (TO_DATE(T2.DUEDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365 < 0	THEN 0
                 ELSE (TO_DATE(T2.DUEDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 END                                         AS FCResidualM              --金融质押品剩余期限
                ,1                                           AS RevaFrequency            --重估频率                              		默认 1
                ,''                                          AS GroupID                  --分组编号                              		默认 NULL
                ,T5.RATINGRESULT														 AS RCERating								 --发行人境外注册地外部评级

    FROM				RWA_DEV.NCM_GUARANTY_INFO T1															--信贷抵质押品信息表
    INNER JOIN	TEMP_BND_COLLATERAL	T2
   	ON					T1.GUARANTYID = T2.GUARANTYID
    LEFT JOIN		RWA_DEV.NCM_ASSET_FINANCE T3															--信贷金融质押品信息表
    ON					T1.GUARANTYID = T3.GUARANTYID
    AND					T3.DATANO = p_data_dt_str
    LEFT JOIN		RWA_DEV.NCM_COL_PARAM T4
    ON					T1.GUARANTYTYPEID = T4.GUARANTYTYPE
    AND					T4.DATANO = p_data_dt_str
    LEFT JOIN		RWA.RWA_WP_COUNTRYRATING T5
    ON					NVL(T3.BONDPUBLISHCOUNTRY,'CHN') = T5.COUNTRYCODE
    AND					T5.ISINUSE = '1'
    LEFT JOIN		TEMP_CUST_INFO T6
    ON					T1.OBLIGEEIDTYPE = T6.CERTTYPE
    AND 				T1.OBLIGEEIDNUMBER = T6.CERTID
    LEFT JOIN		RWA.CODE_LIBRARY T7
    ON					T1.GUARANTYTYPEID = T7.ITEMNO
    AND					T7.CODENO = 'GuarantyList'
		WHERE 			T1.GUARANTYTYPEID NOT IN ('004001004001','004001005001','004001006001','004001006002','001001003001')
																																					--信用证、备用信用证、融资性保函、非融资性保函都归为保证，保证金不取
		AND 				T1.AFFIRMVALUE0 > 0
		AND 				T1.DATANO = p_data_dt_str
		AND NOT EXISTS (SELECT 1 FROM RWA_DEV.RWA_TZ_COLLATERAL TC WHERE 'YP' || T1.GUARANTYID = TC.COLLATERALID)
		;

    COMMIT;

    --2.3 财务系统-应收款投资-保证金
    /*
    INSERT INTO RWA_DEV.RWA_TZ_COLLATERAL(
                 DataDate                              --数据日期
                ,DataNo                                --数据流水号
                ,CollateralID                       	 --抵质押品ID
                ,SSysID                             	 --源系统ID
                ,SGuarContractID                    	 --源担保合同ID
                ,SCollateralID                      	 --源抵质押品ID
                ,CollateralName                     	 --抵质押品名称
                ,IssuerID                           	 --发行人ID
                ,ProviderID                         	 --提供人ID
                ,CreditRiskDataType                 	 --信用风险数据类型
                ,GuaranteeWay                       	 --担保方式
                ,SourceColType                      	 --源抵质押品大类
                ,SourceColSubType                   	 --源抵质押品小类
                ,SpecPurpBondFlag                   	 --是否为收购国有银行不良贷款而发行的债券
                ,QualFlagSTD                        	 --权重法合格标识
                ,QualFlagFIRB                 				 --内评初级法合格标识
                ,CollateralTypeSTD                  	 --权重法抵质押品类型
                ,CollateralSdvsSTD                  	 --权重法抵质押品细分
                ,CollateralTypeIRB                  	 --内评法抵质押品类型
                ,CollateralAmount                   	 --抵押总额
                ,Currency                           	 --币种
                ,StartDate                          	 --起始日期
                ,DueDate                            	 --到期日期
                ,OriginalMaturity                   	 --原始期限
                ,ResidualM                          	 --剩余期限
                ,InteHaircutsFlag                   	 --自行估计折扣系数标识
                ,InternalHc                         	 --内部折扣系数
                ,FCType                             	 --金融质押品类型
                ,ABSFlag                            	 --资产证券化标识
                ,RatingDurationType                 	 --评级期限类型
                ,FCIssueRating     										 --金融质押品发行等级
                ,FCIssuerType                          --金融质押品发行人类别
                ,FCIssuerState                         --金融质押品发行人注册国家
                ,FCResidualM                           --金融质押品剩余期限
                ,RevaFrequency                         --重估频率
                ,GroupID                               --分组编号
                ,RCERating														 --发行人境外注册地外部评级
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DataDate                 --数据日期
                ,p_data_dt_str													     AS DataNo                   --数据流水号
                ,T1.SERIALNO																 AS CollateralID           	 --抵质押品ID
                ,'TZ'                  											 AS SSysID              	 	 --源系统ID
                ,''																					 AS SGuarContractID        	 --源担保合同ID                             默认 NULL
                ,T1.SERIALNO																 AS SCollateralID          	 --源抵质押品ID
                ,'保证金'	                         			 		 AS CollateralName         	 --抵质押品名称
                ,''						                               AS IssuerID             	 	 --发行人ID                                 默认 NULL
                ,T1.CUSTOMERID                           	 	 AS ProviderID             	 --提供人ID
                ,'01'                                  			 AS CreditRiskDataType     	 --信用风险数据类型                         默认 一般非零售(01)
                ,'060'                           		 				 AS GuaranteeWay           	 --担保方式                                 默认 质押(060)
                ,'001001'                            				 AS SourceColType     	     --源抵质押品大类                        		默认 现金及其等价物(001001)
                ,'001001003001'												       AS SourceColSubType         --源抵质押品小类                        		默认 保证金(001001003001)
                ,'0'														             AS SpecPurpBondFlag  			 --是否为收购国有银行不良贷款而发行的债券		默认 否(0)
                ,'1'             									 			 		 AS QualFlagSTD            	 --权重法合格标识                           默认 是(1)
                ,'1'	                           			 			 AS QualFlagFIRB           	 --内评初级法合格标识                       默认 是(1)
                ,'030103'				              				 			 AS CollateralTypeSTD 			 --权重法抵质押品类型                    		默认 保证金(030103)
                ,'01'		                          					 AS CollateralSdvsSTD 		 	 --权重法抵质押品细分                    		默认 现金类资产(01)
                ,'030201'					 		 									 		 AS CollateralTypeIRB      	 --内评法抵质押品类型                       默认 金融质押品(030201)
                ,T1.BAILSUM																	 AS CollateralAmount     	 	 --抵押总额
                ,NVL(T1.BAILCURRENCY,T1.BUSINESSCURRENCY)		 AS Currency               	 --币种
								,T3.STARTDATE													       AS StartDate         			 --起始日期
                ,T3.DUEDATE													         AS DueDate                	 --到期日期
                ,CASE WHEN (TO_DATE(T3.DUEDATE,'YYYYMMDD') - TO_DATE(T3.STARTDATE,'YYYYMMDD')) / 365 < 0	THEN 0
                 ELSE (TO_DATE(T3.DUEDATE,'YYYYMMDD') - TO_DATE(T3.STARTDATE,'YYYYMMDD')) / 365
                 END					                             	 AS OriginalMaturity    	 	 --原始期限
                ,CASE WHEN (TO_DATE(T3.DUEDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365 < 0	THEN 0
                 ELSE (TO_DATE(T3.DUEDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 END                                         AS ResidualM           	 	 --剩余期限
                ,'0'                                         AS InteHaircutsFlag    	 	 --自行估计折扣系数标识                     默认 否(0)
                ,NULL                                        AS InternalHc          	 	 --内部折扣系数                             默认 NULL
                ,'01'                                        AS FCType                 	 --金融质押品类型                           默认 现金及现金等价物(01)
                ,'0'                                         AS ABSFlag             	 	 --资产证券化标识
                ,''                                          AS RatingDurationType  	 	 --评级期限类型                             默认 NULL
                ,''                                          AS FCIssueRating     			 --金融质押品发行等级                    		默认 NULL
                ,''	                                         AS FCIssuerType             --金融质押品发行人类别                  		默认 NULL
                ,''                                       	 AS FCIssuerState            --金融质押品发行人注册国家              		默认 NULL
                ,CASE WHEN (TO_DATE(T3.DUEDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365 < 0	THEN 0
                 ELSE (TO_DATE(T3.DUEDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 END                                         AS FCResidualM              --金融质押品剩余期限
                ,1                                           AS RevaFrequency            --重估频率                              		默认 1
                ,''                                          AS GroupID                  --分组编号                              		默认 NULL
                ,''																					 AS RCERating								 --发行人境外注册地外部评级

    FROM				RWA_DEV.NCM_BUSINESS_CONTRACT T1											--信贷合同表
    INNER JOIN	RWA_DEV.NCM_BUSINESS_DUEBILL T2												--信贷借据表
    ON					T1.SERIALNO = T2.RELATIVESERIALNO2
    AND					T2.DATANO = p_data_dt_str
		INNER JOIN	RWA_DEV.RWA_TZ_EXPOSURE T3	 													--投资暴露表
		ON 					T2.SERIALNO = T3.EXPOSUREID
		AND					T3.BUSINESSTYPEID IN ('1040105060','1040105061','1040105062')	--应收款投资业务
		WHERE 			T1.BUSINESSTYPE = '1040105060'												--应收款投资业务
		AND					T1.BAILSUM > 0																				--保证金大于0
		AND					T1.DATANO = p_data_dt_str
		;
		*/
		--2.3 财务系统-应收款投资-保证金
		INSERT INTO RWA_DEV.RWA_TZ_COLLATERAL(
                 DataDate                              --数据日期
                ,DataNo                                --数据流水号
                ,CollateralID                       	 --抵质押品ID
                ,SSysID                             	 --源系统ID
                ,SGuarContractID                    	 --源担保合同ID
                ,SCollateralID                      	 --源抵质押品ID
                ,CollateralName                     	 --抵质押品名称
                ,IssuerID                           	 --发行人ID
                ,ProviderID                         	 --提供人ID
                ,CreditRiskDataType                 	 --信用风险数据类型
                ,GuaranteeWay                       	 --担保方式
                ,SourceColType                      	 --源抵质押品大类
                ,SourceColSubType                   	 --源抵质押品小类
                ,SpecPurpBondFlag                   	 --是否为收购国有银行不良贷款而发行的债券
                ,QualFlagSTD                        	 --权重法合格标识
                ,QualFlagFIRB                 				 --内评初级法合格标识
                ,CollateralTypeSTD                  	 --权重法抵质押品类型
                ,CollateralSdvsSTD                  	 --权重法抵质押品细分
                ,CollateralTypeIRB                  	 --内评法抵质押品类型
                ,CollateralAmount                   	 --抵押总额
                ,Currency                           	 --币种
                ,StartDate                          	 --起始日期
                ,DueDate                            	 --到期日期
                ,OriginalMaturity                   	 --原始期限
                ,ResidualM                          	 --剩余期限
                ,InteHaircutsFlag                   	 --自行估计折扣系数标识
                ,InternalHc                         	 --内部折扣系数
                ,FCType                             	 --金融质押品类型
                ,ABSFlag                            	 --资产证券化标识
                ,RatingDurationType                 	 --评级期限类型
                ,FCIssueRating     										 --金融质押品发行等级
                ,FCIssuerType                          --金融质押品发行人类别
                ,FCIssuerState                         --金融质押品发行人注册国家
                ,FCResidualM                           --金融质押品剩余期限
                ,RevaFrequency                         --重估频率
                ,GroupID                               --分组编号
                ,RCERating														 --发行人境外注册地外部评级
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DataDate                 --数据日期
                ,p_data_dt_str													     AS DataNo                   --数据流水号
                ,'HT' || T1.CONTRACTNO || T1.BAILCURRENCY		 AS CollateralID           	 --抵质押品ID
                ,'TZ'                  											 AS SSysID              	 	 --源系统ID
                ,''																					 AS SGuarContractID        	 --源担保合同ID                             默认 NULL
                ,T1.CONTRACTNO															 AS SCollateralID          	 --源抵质押品ID
                ,'保证金'	                         			 		 AS CollateralName         	 --抵质押品名称
                ,''						                               AS IssuerID             	 	 --发行人ID                                 默认 NULL
                ,T2.CLIENTID                           	 	 	 AS ProviderID             	 --提供人ID
                ,'01'                                  			 AS CreditRiskDataType     	 --信用风险数据类型                         默认 一般非零售(01)
                ,'060'                           		 				 AS GuaranteeWay           	 --担保方式                                 默认 质押(060)
                ,'001001'                            				 AS SourceColType     	     --源抵质押品大类                        		默认 现金及其等价物(001001)
                ,'001001003001'												       AS SourceColSubType         --源抵质押品小类                        		默认 保证金(001001003001)
                ,'0'														             AS SpecPurpBondFlag  			 --是否为收购国有银行不良贷款而发行的债券		默认 否(0)
                ,'1'             									 			 		 AS QualFlagSTD            	 --权重法合格标识                           默认 是(1)
                ,'1'	                           			 			 AS QualFlagFIRB           	 --内评初级法合格标识                       默认 是(1)
                ,'030103'				              				 			 AS CollateralTypeSTD 			 --权重法抵质押品类型                    		默认 保证金(030103)
                ,'01'		                          					 AS CollateralSdvsSTD 		 	 --权重法抵质押品细分                    		默认 现金类资产(01)
                ,'030201'					 		 									 		 AS CollateralTypeIRB      	 --内评法抵质押品类型                       默认 金融质押品(030201)
                ,T1.BAILBALANCE															 AS CollateralAmount     	 	 --抵押总额
                ,T1.BAILCURRENCY		 												 AS Currency               	 --币种
								,T2.STARTDATE													       AS StartDate         			 --起始日期
                ,T2.DUEDATE													         AS DueDate                	 --到期日期
                ,CASE WHEN (TO_DATE(T2.DUEDATE,'YYYYMMDD') - TO_DATE(T2.STARTDATE,'YYYYMMDD')) / 365 < 0	THEN 0
                 ELSE (TO_DATE(T2.DUEDATE,'YYYYMMDD') - TO_DATE(T2.STARTDATE,'YYYYMMDD')) / 365
                 END					                             	 AS OriginalMaturity    	 	 --原始期限
                ,CASE WHEN (TO_DATE(T2.DUEDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365 < 0	THEN 0
                 ELSE (TO_DATE(T2.DUEDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 END                                         AS ResidualM           	 	 --剩余期限
                ,'0'                                         AS InteHaircutsFlag    	 	 --自行估计折扣系数标识                     默认 否(0)
                ,NULL                                        AS InternalHc          	 	 --内部折扣系数                             默认 NULL
                ,'01'                                        AS FCType                 	 --金融质押品类型                           默认 现金及现金等价物(01)
                ,'0'                                         AS ABSFlag             	 	 --资产证券化标识
                ,''                                          AS RatingDurationType  	 	 --评级期限类型                             默认 NULL
                ,''                                          AS FCIssueRating     			 --金融质押品发行等级                    		默认 NULL
                ,''	                                         AS FCIssuerType             --金融质押品发行人类别                  		默认 NULL
                ,''                                       	 AS FCIssuerState            --金融质押品发行人注册国家              		默认 NULL
                ,CASE WHEN (TO_DATE(T2.DUEDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365 < 0	THEN 0
                 ELSE (TO_DATE(T2.DUEDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 END                                         AS FCResidualM              --金融质押品剩余期限
                ,1                                           AS RevaFrequency            --重估频率                              		默认 1
                ,''                                          AS GroupID                  --分组编号                              		默认 NULL
                ,''																					 AS RCERating								 --发行人境外注册地外部评级

    FROM				RWA_DEV.RWA_TEMP_BAIL2 T1															--信贷合同表
    INNER JOIN	RWA_DEV.RWA_TZ_CONTRACT T2														--投资合同表
    ON					T1.CONTRACTNO = T2.SCONTRACTID
		AND					T2.BUSINESSTYPEID IN ('1040105060','1040105061','1040105062')	--应收款投资业务
		AND					T2.DATANO = p_data_dt_str
		WHERE 			T1.ISMAX = '1'																				--取相同合同下最大的一笔作为结果
		;

    COMMIT;

    --2.4 财务系统-应收款投资-票据资管业务-票据信息
    INSERT INTO RWA_DEV.RWA_TZ_COLLATERAL(
                 DataDate                              --数据日期
                ,DataNo                                --数据流水号
                ,CollateralID                       	 --抵质押品ID
                ,SSysID                             	 --源系统ID
                ,SGuarContractID                    	 --源担保合同ID
                ,SCollateralID                      	 --源抵质押品ID
                ,CollateralName                     	 --抵质押品名称
                ,IssuerID                           	 --发行人ID
                ,ProviderID                         	 --提供人ID
                ,CreditRiskDataType                 	 --信用风险数据类型
                ,GuaranteeWay                       	 --担保方式
                ,SourceColType                      	 --源抵质押品大类
                ,SourceColSubType                   	 --源抵质押品小类
                ,SpecPurpBondFlag                   	 --是否为收购国有银行不良贷款而发行的债券
                ,QualFlagSTD                        	 --权重法合格标识
                ,QualFlagFIRB                 				 --内评初级法合格标识
                ,CollateralTypeSTD                  	 --权重法抵质押品类型
                ,CollateralSdvsSTD                  	 --权重法抵质押品细分
                ,CollateralTypeIRB                  	 --内评法抵质押品类型
                ,CollateralAmount                   	 --抵押总额
                ,Currency                           	 --币种
                ,StartDate                          	 --起始日期
                ,DueDate                            	 --到期日期
                ,OriginalMaturity                   	 --原始期限
                ,ResidualM                          	 --剩余期限
                ,InteHaircutsFlag                   	 --自行估计折扣系数标识
                ,InternalHc                         	 --内部折扣系数
                ,FCType                             	 --金融质押品类型
                ,ABSFlag                            	 --资产证券化标识
                ,RatingDurationType                 	 --评级期限类型
                ,FCIssueRating     										 --金融质押品发行等级
                ,FCIssuerType                          --金融质押品发行人类别
                ,FCIssuerState                         --金融质押品发行人注册国家
                ,FCResidualM                           --金融质押品剩余期限
                ,RevaFrequency                         --重估频率
                ,GroupID                               --分组编号
                ,RCERating														 --发行人境外注册地外部评级
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DataDate                 --数据日期
                ,p_data_dt_str													     AS DataNo                   --数据流水号
                ,'TZBILL' || T1.SERIALNO										 AS CollateralID           	 --抵质押品ID
                ,'TZ'                  											 AS SSysID              	 	 --源系统ID
                ,''																					 AS SGuarContractID        	 --源担保合同ID                             默认 NULL
                ,T1.SERIALNO																 AS SCollateralID          	 --源抵质押品ID
                ,CASE WHEN T1.BILLTYPE='2' THEN '商业承兑汇票'
                 ELSE '中国商业银行承兑汇票'
                 END	                         			 		 		 AS CollateralName         	 --抵质押品名称
                ,T1.ACCEPTORID		                           AS IssuerID             	 	 --发行人ID                                 默认 NULL
                ,T1.HOLDERID	                           	 	 AS ProviderID             	 --提供人ID
                ,'01'                                  			 AS CreditRiskDataType     	 --信用风险数据类型                         默认 一般非零售(01)
                ,'060'                           		 				 AS GuaranteeWay           	 --担保方式                                 默认 质押(060)
                ,'001004'                            				 AS SourceColType     	     --源抵质押品大类                        		默认 票据(001004)
                ,CASE WHEN T1.BILLTYPE='2' THEN '001004004001'													 --商业承兑汇票
                 ELSE '001004002001'																										 --中国商业银行承兑汇票
                 END																	       AS SourceColSubType         --源抵质押品小类
                ,'0'														             AS SpecPurpBondFlag  			 --是否为收购国有银行不良贷款而发行的债券		默认 否(0)
                ,''             									 			 		 AS QualFlagSTD            	 --权重法合格标识                           默认 是(1)
                ,''		                           			 			 AS QualFlagFIRB           	 --内评初级法合格标识                       默认 是(1)
                ,''							              				 			 AS CollateralTypeSTD 			 --权重法抵质押品类型                    		默认 保证金(030103)
                ,''			                          					 AS CollateralSdvsSTD 		 	 --权重法抵质押品细分                    		默认 现金类资产(01)
                ,''								 		 									 		 AS CollateralTypeIRB      	 --内评法抵质押品类型                       默认 金融质押品(030201)
                ,T1.BILLSUM					 				 								 AS CollateralAmount     	 	 --抵押总额
                ,T1.LCCURRENCY															 AS Currency               	 --币种
								/*
								,T1.ISSUEDATE													       AS StartDate         			 --起始日期
                ,T1.MATURITY												         AS DueDate                	 --到期日期
                ,CASE WHEN (TO_DATE(T1.MATURITY,'YYYYMMDD') - TO_DATE(T1.ISSUEDATE,'YYYYMMDD')) / 365 < 0	THEN 0
                 ELSE (TO_DATE(T1.MATURITY,'YYYYMMDD') - TO_DATE(T1.ISSUEDATE,'YYYYMMDD')) / 365
                 END					                             	 AS OriginalMaturity    	 	 --原始期限
                ,CASE WHEN (TO_DATE(T1.MATURITY,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365 < 0	THEN 0
                 ELSE (TO_DATE(T1.MATURITY,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 END                                         AS ResidualM           	 	 --剩余期限
                 */
                ,T7.STARTDATE													       AS StartDate         			 --起始日期
                ,T7.DUEDATE													         AS DueDate                	 --到期日期
                ,CASE WHEN (TO_DATE(T7.DUEDATE,'YYYYMMDD') - TO_DATE(T7.STARTDATE,'YYYYMMDD')) / 365 < 0	THEN 0
                 ELSE (TO_DATE(T7.DUEDATE,'YYYYMMDD') - TO_DATE(T7.STARTDATE,'YYYYMMDD')) / 365
                 END					                             	 AS OriginalMaturity    	 	 --原始期限
                ,CASE WHEN (TO_DATE(T7.DUEDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365 < 0	THEN 0
                 ELSE (TO_DATE(T7.DUEDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 END                                         AS ResidualM           	 	 --剩余期限
                ,'0'                                         AS InteHaircutsFlag    	 	 --自行估计折扣系数标识                     默认 否(0)
                ,NULL                                        AS InternalHc          	 	 --内部折扣系数                             默认 NULL
                ,''	                                         AS FCType                 	 --金融质押品类型                           默认 NULL
                ,'0'                                         AS ABSFlag             	 	 --资产证券化标识
                ,CASE WHEN NVL(T3.COUNTRYCODE,'CHN') <> 'CHN' THEN '01'
                 ELSE ''
                 END                                         AS RatingDurationType  	 	 --评级期限类型                             默认 NULL
                ,CASE WHEN NVL(T3.COUNTRYCODE,'CHN') <> 'CHN' THEN T5.RATINGRESULT
                 ELSE ''
                 END                                         AS FCIssueRating     			 --金融质押品发行等级                    		默认 NULL
                ,CASE WHEN T3.RWACUSTOMERTYPE LIKE '01%' THEN '01'
                			WHEN T3.CUSTOMERID IS NOT NULL THEN '02'
                 ELSE ''
                 END                                         AS FCIssuerType             --金融质押品发行人类别                  		默认 其他(02)
                ,CASE WHEN NVL(T3.COUNTRYCODE,'CHN') = 'CHN' THEN '01'
                 ELSE '02'
                 END					                            	 AS FCIssuerState            --金融质押品发行人注册国家              		默认 NULL
                /*
                ,CASE WHEN (TO_DATE(T1.MATURITY,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365 < 0	THEN 0
                 ELSE (TO_DATE(T1.MATURITY,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 END                                         AS FCResidualM              --金融质押品剩余期限
                 */
                ,CASE WHEN (TO_DATE(T7.DUEDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365 < 0	THEN 0
                 ELSE (TO_DATE(T7.DUEDATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 END                                         AS FCResidualM              --金融质押品剩余期限
                ,1                                           AS RevaFrequency            --重估频率                              		默认 1
                ,''                                          AS GroupID                  --分组编号                              		默认 NULL
                ,T5.RATINGRESULT														 AS RCERating								 --发行人境外注册地外部评级

    FROM				RWA_DEV.NCM_BILL_INFO T1															--信贷票据信息表
		INNER JOIN	RWA_DEV.NCM_BUSINESS_CONTRACT T6											--
		ON					T1.OBJECTNO = T6.SERIALNO
		AND					T6.BUSINESSSUBTYPE = '003050' 												--基于投资管理人-票据资管业务
		AND					T6.DATANO = p_data_dt_str
		INNER JOIN	RWA_DEV.RWA_TZ_CONTRACT T7
		ON					T7.SCONTRACTID = T6.SERIALNO
		AND					T7.BUSINESSTYPEID IN ('1040105060','1040105061','1040105062')	--应收款投资业务
		LEFT JOIN 	RWA_DEV.NCM_CUSTOMER_INFO T3
    ON 					T1.ACCEPTORID = T3.CUSTOMERID
    AND 				T3.DATANO=P_DATA_DT_STR
    LEFT JOIN 	RWA.RWA_WP_COUNTRYRATING T5
    ON 					T3.COUNTRYCODE = T5.COUNTRYCODE
    AND					T5.ISINUSE = '1'
		WHERE 			T1.OBJECTTYPE = 'BusinessContract'										--应收款投资业务
		AND					T1.DATANO = p_data_dt_str
		;

    COMMIT;

    --2.5 财务系统-债券投资-货币基金-补录
    INSERT INTO RWA_DEV.RWA_TZ_COLLATERAL(
                 DataDate                              --数据日期
                ,DataNo                                --数据流水号
                ,CollateralID                       	 --抵质押品ID
                ,SSysID                             	 --源系统ID
                ,SGuarContractID                    	 --源担保合同ID
                ,SCollateralID                      	 --源抵质押品ID
                ,CollateralName                     	 --抵质押品名称
                ,IssuerID                           	 --发行人ID
                ,ProviderID                         	 --提供人ID
                ,CreditRiskDataType                 	 --信用风险数据类型
                ,GuaranteeWay                       	 --担保方式
                ,SourceColType                      	 --源抵质押品大类
                ,SourceColSubType                   	 --源抵质押品小类
                ,SpecPurpBondFlag                   	 --是否为收购国有银行不良贷款而发行的债券
                ,QualFlagSTD                        	 --权重法合格标识
                ,QualFlagFIRB                 				 --内评初级法合格标识
                ,CollateralTypeSTD                  	 --权重法抵质押品类型
                ,CollateralSdvsSTD                  	 --权重法抵质押品细分
                ,CollateralTypeIRB                  	 --内评法抵质押品类型
                ,CollateralAmount                   	 --抵押总额
                ,Currency                           	 --币种
                ,StartDate                          	 --起始日期
                ,DueDate                            	 --到期日期
                ,OriginalMaturity                   	 --原始期限
                ,ResidualM                          	 --剩余期限
                ,InteHaircutsFlag                   	 --自行估计折扣系数标识
                ,InternalHc                         	 --内部折扣系数
                ,FCType                             	 --金融质押品类型
                ,ABSFlag                            	 --资产证券化标识
                ,RatingDurationType                 	 --评级期限类型
                ,FCIssueRating     										 --金融质押品发行等级
                ,FCIssuerType                          --金融质押品发行人类别
                ,FCIssuerState                         --金融质押品发行人注册国家
                ,FCResidualM                           --金融质押品剩余期限
                ,RevaFrequency                         --重估频率
                ,GroupID                               --分组编号
                ,RCERating														 --发行人境外注册地外部评级
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DataDate                 --数据日期
                ,p_data_dt_str													     AS DataNo                   --数据流水号
                ,T1.MITIGATIONID														 AS CollateralID           	 --抵质押品ID
                ,'TZHBJJ'              											 AS SSysID              	 	 --源系统ID
                ,''																					 AS SGuarContractID        	 --源担保合同ID                             默认 NULL
                ,T1.BOND_ID																	 AS SCollateralID          	 --源抵质押品ID
                ,T4.ITEMNAME                   			 		 		 AS CollateralName         	 --抵质押品名称
                ,CASE WHEN T1.GUARANTYTYPE LIKE '001003001%' THEN 'ZGZYZF'
                 ELSE T1.CUSTID1
                 END							                           AS IssuerID             	 	 --发行人ID                                 默认 NULL
                ,T3.CLIENTID	                           	 	 AS ProviderID             	 --提供人ID
                ,'01'                                  			 AS CreditRiskDataType     	 --信用风险数据类型                         默认 一般非零售(01)
                ,'060'                           		 				 AS GuaranteeWay           	 --担保方式                                 默认 质押(060)
                ,SUBSTR(T1.GUARANTYTYPE,1,6)         				 AS SourceColType     	     --源抵质押品大类                        		默认 票据(001004)
                ,T1.GUARANTYTYPE											       AS SourceColSubType         --源抵质押品小类
                ,CASE WHEN T1.BONDISSUEINTENT = '01' THEN '1'
                 ELSE '0'
                 END														             AS SpecPurpBondFlag  			 --是否为收购国有银行不良贷款而发行的债券		默认 否(0)
                ,''             									 			 		 AS QualFlagSTD            	 --权重法合格标识                           默认 是(1)
                ,''		                           			 			 AS QualFlagFIRB           	 --内评初级法合格标识                       默认 是(1)
                ,''							              				 			 AS CollateralTypeSTD 			 --权重法抵质押品类型                    		默认 保证金(030103)
                ,''			                          					 AS CollateralSdvsSTD 		 	 --权重法抵质押品细分                    		默认 现金类资产(01)
                ,''								 		 									 		 AS CollateralTypeIRB      	 --内评法抵质押品类型                       默认 金融质押品(030201)
                ,ROUND(TO_NUMBER(REPLACE(NVL(T1.GUARANTYSUM,'0'),',','')),6)
                										 				 								 AS CollateralAmount     	 	 --抵押总额
                ,NVL(T1.GUARANTYCURRENCYCODE,'CNY')					 AS Currency               	 --币种
                ,T3.STARTDATE																 AS	STARTDATE            		 --起始日期
								,T3.DUEDATE																	 AS	DUEDATE              		 --到期日期
								,T3.ORIGINALMATURITY												 AS ORIGINALMATURITY   	 		 --原始期限
								,T3.RESIDUALM																 AS	RESIDUALM            		 --剩余期限
                ,'0'                                         AS InteHaircutsFlag    	 	 --自行估计折扣系数标识                     默认 否(0)
                ,NULL                                        AS InternalHc          	 	 --内部折扣系数                             默认 NULL
                ,''	                                         AS FCType                 	 --金融质押品类型                           默认 NULL
                ,'0'                                         AS ABSFlag             	 	 --资产证券化标识
                ,''                                          AS RatingDurationType  	 	 --评级期限类型                             默认 NULL
                ,''                                          AS FCIssueRating     			 --金融质押品发行等级                    		默认 NULL
                ,CASE WHEN T1.GUARANTORCATEGORY LIKE '01%' THEN '01'
                 ELSE '02'
                 END                                         AS FCIssuerType             --金融质押品发行人类别                  		默认 其他(02)
                ,CASE WHEN T1.GUARANTORCOUNTRYCODE = 'CHN' THEN '01'
                 ELSE '02'
                 END					                            	 AS FCIssuerState            --金融质押品发行人注册国家              		默认 NULL
                ,T3.RESIDUALM                                AS FCResidualM              --金融质押品剩余期限
                ,1                                           AS RevaFrequency            --重估频率                              		默认 1
                ,''                                          AS GroupID                  --分组编号                              		默认 NULL
                ,T5.RATINGRESULT														 AS RCERating								 --发行人境外注册地外部评级

    FROM				RWA.RWA_WS_BONDTRADE_MF T1	 																--货币基金债券投资补录表
		INNER JOIN  RWA.RWA_WP_DATASUPPLEMENT T2
    ON          T1.SUPPORGID = T2.ORGID
    AND         T2.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    AND         T2.SUPPTMPLID = 'M-0071'
    AND         T2.SUBMITFLAG = '1'
    LEFT JOIN		RWA_DEV.RWA_TZ_EXPOSURE T3
    ON					T1.BOND_ID = T3.DUEID
    LEFT JOIN		RWA.CODE_LIBRARY T4
    ON					T1.GUARANTYTYPE = T4.ITEMNO
    AND					T4.CODENO = 'GuarantyList0071'
    LEFT JOIN		RWA.RWA_WP_COUNTRYRATING T5
    ON 					T1.GUARANTORCOUNTRYCODE = T5.COUNTRYCODE
    AND					T5.ISINUSE = '1'
		WHERE 			T1.GUARANTYTYPE NOT IN ('004001004001','004001005001','004001006001','004001006002','010')
		AND					T1.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
		;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_TZ_COLLATERAL',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_TZ_COLLATERAL;
    --Dbms_output.Put_line('RWA_DEV.RWA_TZ_COLLATERAL表当前插入的财务系统-应收款投资数据记录为: ' || v_count || ' 条');




    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功' || '-' || v_count;
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '投资类抵质押品('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_TZ_COLLATERAL;
/

