CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_TZ_GUARANTEE(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_TZ_GUARANTEE
    实现功能:财务系统-投资-保证(从数据源财务系统将应收款投资相关信息全量导入RWA投资类接口表保证表中)
    数据口径:全量
    跑批频率:月初
    版  本  :V1.0.0
    编写人  :YUSJ
    编写时间:2015-05-26
    单  位	:上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.FNS_BND_INFO_B|财务系统债券信息表
    源  表2 :RWA_DEV.FNS_BND_BOOK_B|财务系统账面活动表
    源  表3 :RWA.RWA_WS_RECEIVABLE|应收款投资补录表
    源  表4 :RWA.RWA_WP_DATASUPPLEMENT|补录任务明细信息表
    源  表5 :RWA_DEV.CBS_BND|债券投资登记簿
    源  表6 :RWA_DEV.CBS_IAC|通用分户帐
    源  表7 :RWA.RWA_WS_B_RECEIVABLE|买入返售其他金融资产_应收账款投资补录表
    源  表8 :RWA.RWA_WSIB_ABS_ISSUE_EXPOSURE|发行机构资产证券化暴露铺底表
    源  表9 :RWA.RWA_WSIB_ABS_INVEST_EXPOSURE|投资机构资产证券化暴露铺底表
    目标表	 :RWA_DEV.RWA_TZ_GUARANTEE|财务系统投资类保证表
    变更记录(修改人|修改时间|修改内容):
  */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_TZ_GUARANTEE';
  --定义异常变量
  v_raise EXCEPTION;
  --定义判断值变量
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TZ_GUARANTEE';

    --2.将满足条件的数据从源表插入到目标表中
    --2.1 财务系统-应收款投资-保证-非基于银行-合同
    INSERT INTO RWA_DEV.RWA_TZ_GUARANTEE(
         				 DataDate          												  --数据日期
								,DataNo                                     --数据流水号
								,GuaranteeID                                --保证ID
								,SSysID                                     --源系统ID
								,GuaranteeConID                             --保证合同ID
								,GuarantorID                                --保证人ID
								,CreditRiskDataType                         --信用风险数据类型
								,GuaranteeWay                            		--担保方式
								,QualFlagSTD                            		--权重法合格标识
								,QualFlagFIRB                               --内评初级法合格标识
								,GuaranteeTypeSTD                           --权重法保证类型
								,GuarantorSdvsSTD                           --权重法保证人细分
								,GuaranteeTypeIRB                           --内评法保证类型
								,GuaranteeAmount                            --保证总额
								,Currency                                   --币种
								,StartDate                                  --起始日期
								,DueDate                                    --到期日期
								,OriginalMaturity                           --原始期限
								,ResidualM                                  --剩余期限
								,GuarantorIRating                           --保证人内部评级
								,GuarantorPD                                --保证人违约概率
								,GroupID                                    --分组编号
    )
    WITH TEMP_BND_GUARANTEE AS (
												  SELECT T5.SERIALNO
																,MIN(T1.STARTDATE)							AS STARTDATE
																,MAX(T1.DUEDATE)								AS DUEDATE
																,MIN(T4.QUALIFYFLAG) 						AS QUALIFYFLAG
													  FROM RWA_DEV.RWA_TZ_CONTRACT T1	 															--投资合同表
											INNER JOIN (SELECT DISTINCT
																				 SERIALNO
																				,OBJECTNO
																				,QUALIFYFLAG
																		FROM RWA_DEV.NCM_CONTRACT_RELATIVE
																	 WHERE OBJECTTYPE = 'GuarantyContract'
																		 AND DATANO = p_data_dt_str) T4                       --信贷合同关联表
									    				ON T1.SCONTRACTID = T4.SERIALNO
									    INNER JOIN RWA_DEV.NCM_GUARANTY_CONTRACT T5													--信贷担保合同表
									    				ON T4.OBJECTNO = T5.SERIALNO
									    			 AND T5.GUARANTYTYPE = '010'																	--保证
									    			 AND T5.DATANO = p_data_dt_str
									    INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T8													--信贷合同表
									    				ON T1.SCONTRACTID = T8.SERIALNO
									    			 AND (T8.BUSINESSSUBTYPE NOT LIKE '0010%' OR T8.BUSINESSSUBTYPE IS NULL) 			--非基于银行
									    			 AND T8.DATANO = p_data_dt_str
													 WHERE T1.BUSINESSTYPEID IN ('1040105060','1040105061','1040105062')	--应收款投资业务
												GROUP BY T5.SERIALNO
		)
		,TEMP_CUST_INFO AS (
    								SELECT CUSTOMERID, CUSTOMERNAME, CERTTYPE, CERTID
										  FROM (SELECT CUSTOMERID,
										               CUSTOMERNAME,
										               CERTTYPE,
										               CERTID,
										               ROW_NUMBER() OVER(PARTITION BY CERTTYPE, CERTID ORDER BY CUSTOMERID DESC) AS RN
										          FROM RWA_DEV.NCM_CUSTOMER_INFO
										         WHERE DATANO = P_DATA_DT_STR)
										 WHERE RN = 1
    )
    SELECT
         				 TO_DATE(p_data_dt_str,'YYYYMMDD')								AS  DATADATE          	 --数据日期
         				,p_data_dt_str														 		    AS	DATANO               --数据流水号
         				,'BZ' || T1.SERIALNO															AS	GUARANTEEID          --保证ID
								,'TZ'																						  AS	SSYSID               --源系统ID
								,T1.SERIALNO																			AS	GUARANTEECONID       --保证合同ID
								--,DECODE(T1.GUARANTORID,'NCM_','XN-YBGS',T1.GUARANTORID)
								,CASE WHEN REPLACE(T1.GUARANTORID,'NCM_','') IS NULL AND T4.CUSTOMERID IS NULL THEN
													 CASE WHEN T1.CERTTYPE LIKE 'Ind%' THEN 'XN-GRKH'
													 			ELSE 'XN-YBGS'
													 END
											WHEN REPLACE(T1.GUARANTORID,'NCM_','') IS NULL THEN T4.CUSTOMERID
											ELSE T1.GUARANTORID
								 END																							AS	GUARANTORID          --保证人ID              					如果保证人为空则默认保证人为一般公司，担保金额大于0的担保合同都有担保人
								,'01'																							AS	CREDITRISKDATATYPE   --信用风险数据类型      					默认：一般非零售(01)
								,T1.GUARANTYTYPE																	AS	GUARANTEEWAY       	 --担保方式
								,''																								AS	QUALFLAGSTD          --权重法合格标识
								,CASE WHEN T2.QUALIFYFLAG	= '01' THEN '1'
											WHEN T2.QUALIFYFLAG	= '02' THEN '0'
								 			ELSE ''
								 END																							AS	QUALFLAGFIRB         --内评初级法合格标识
								,''																								AS	GUARANTEETYPESTD     --权重法保证类型
								,''																								AS	GUARANTORSDVSSTD     --权重法保证人细分
								,CASE WHEN T2.QUALIFYFLAG = '01' THEN '020201'
								 ELSE ''
								 END																							AS	GUARANTEETYPEIRB     --内评法保证类型
								,T1.GUARANTYVALUE																	AS	GUARANTEEAMOUNT      --保证总额
								,NVL(T1.GUARANTYCURRENCY,'CNY')										AS	CURRENCY             --币种
								,T2.STARTDATE																		  AS	STARTDATE            --起始日期
								,T2.DUEDATE																				AS	DUEDATE              --到期日期
								,CASE WHEN (TO_DATE(T2.DUEDATE,'YYYYMMDD')-TO_DATE(T2.STARTDATE,'YYYYMMDD'))/365<0
								      THEN 0
								      ELSE (TO_DATE(T2.DUEDATE,'YYYYMMDD')-TO_DATE(T2.STARTDATE,'YYYYMMDD'))/365
								END																								AS  ORIGINALMATURITY   	 --原始期限
								,CASE WHEN (TO_DATE(T2.DUEDATE,'YYYYMMDD')-TO_DATE(p_data_dt_str,'YYYYMMDD'))/365<0
								      THEN 0
								      ELSE (TO_DATE(T2.DUEDATE,'YYYYMMDD')-TO_DATE(p_data_dt_str,'YYYYMMDD'))/365
								END																								AS	RESIDUALM            --剩余期限
								,T3.PDADJLEVEL																		AS	GUARANTORIRATING     --保证人内部评级
								,T3.PD																						AS	GUARANTORPD          --保证人违约概率
								,''																								AS	GROUPID              --分组编号
    FROM				RWA_DEV.NCM_GUARANTY_CONTRACT T1 									--担保合同信息表
		INNER JOIN  TEMP_BND_GUARANTEE T2				                      --应收款投资保证临时表
    ON          T1.SERIALNO = T2.SERIALNO
    LEFT JOIN 	TEMP_CUST_INFO T4
    ON					T1.CERTTYPE = T4.CERTTYPE
    AND					T1.CERTID = T4.CERTID
    LEFT JOIN		RWA_DEV.RWA_TEMP_PDLEVEL T3
    ON					DECODE(T1.GUARANTORID,'NCM_',T4.CUSTOMERID,NULL,T4.CUSTOMERID,'',T4.CUSTOMERID,T1.GUARANTORID) = T3.CUSTID
		WHERE 			T1.GUARANTYTYPE = '010'														--保证
		AND					T1.GUARANTYVALUE > 0
		AND 				T1.DATANO = p_data_dt_str
		;

    COMMIT;

    --2.2 财务系统-应收款投资-保证-非基于银行-出账
    INSERT INTO RWA_DEV.RWA_TZ_GUARANTEE(
         				 DataDate          												  --数据日期
								,DataNo                                     --数据流水号
								,GuaranteeID                                --保证ID
								,SSysID                                     --源系统ID
								,GuaranteeConID                             --保证合同ID
								,GuarantorID                                --保证人ID
								,CreditRiskDataType                         --信用风险数据类型
								,GuaranteeWay                            		--担保方式
								,QualFlagSTD                            		--权重法合格标识
								,QualFlagFIRB                               --内评初级法合格标识
								,GuaranteeTypeSTD                           --权重法保证类型
								,GuarantorSdvsSTD                           --权重法保证人细分
								,GuaranteeTypeIRB                           --内评法保证类型
								,GuaranteeAmount                            --保证总额
								,Currency                                   --币种
								,StartDate                                  --起始日期
								,DueDate                                    --到期日期
								,OriginalMaturity                           --原始期限
								,ResidualM                                  --剩余期限
								,GuarantorIRating                           --保证人内部评级
								,GuarantorPD                                --保证人违约概率
								,GroupID                                    --分组编号
    )
    WITH TEMP_BND_GUARANTEE AS (
												  SELECT T5.SERIALNO
																,MIN(T1.STARTDATE)		AS STARTDATE
																,MAX(T1.DUEDATE)			AS DUEDATE
													  FROM RWA_DEV.RWA_TZ_CONTRACT T1	 															--投资合同表
											INNER JOIN RWA_DEV.NCM_BUSINESS_PUTOUT T3														--出账表
															ON T1.SCONTRACTID = T3.CONTRACTSERIALNO
														 AND T3.DATANO = p_data_dt_str
											INNER JOIN RWA_DEV.NCM_GUARANTY_RELATIVE T4													--押品关联表
															ON T3.SERIALNO = T4.OBJECTNO
														 AND T4.OBJECTTYPE = 'PutOutApply'
														 AND T4.DATANO = p_data_dt_str
									    INNER JOIN RWA_DEV.NCM_GUARANTY_CONTRACT T5													--信贷担保合同表
									    				ON T4.OBJECTNO = T5.SERIALNO
									    			 AND T5.GUARANTYTYPE = '010'																	--保证
									    			 AND T5.DATANO = p_data_dt_str
									    INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT T8													--信贷合同表
									    				ON T1.SCONTRACTID = T8.SERIALNO
									    			 AND (T8.BUSINESSSUBTYPE NOT LIKE '0010%' OR T8.BUSINESSSUBTYPE IS NULL) 			--非基于银行
									    			 AND T8.DATANO = p_data_dt_str
													 WHERE T1.BUSINESSTYPEID IN ('1040105060','1040105061','1040105062')	--应收款投资业务
												GROUP BY T5.SERIALNO
		)
		,TEMP_CUST_INFO AS (
    								SELECT CUSTOMERID, CUSTOMERNAME, CERTTYPE, CERTID
										  FROM (SELECT CUSTOMERID,
										               CUSTOMERNAME,
										               CERTTYPE,
										               CERTID,
										               ROW_NUMBER() OVER(PARTITION BY CERTTYPE, CERTID ORDER BY CUSTOMERID DESC) AS RN
										          FROM RWA_DEV.NCM_CUSTOMER_INFO
										         WHERE DATANO = P_DATA_DT_STR)
										 WHERE RN = 1
    )
    SELECT
         				 TO_DATE(p_data_dt_str,'YYYYMMDD')								AS  DATADATE          	 --数据日期
         				,p_data_dt_str														 		    AS	DATANO               --数据流水号
         				,'BZ' || T1.SERIALNO															AS	GUARANTEEID          --保证ID
								,'TZ'																						  AS	SSYSID               --源系统ID
								,T1.SERIALNO																			AS	GUARANTEECONID       --保证合同ID
								--,DECODE(T1.GUARANTORID,'NCM_','XN-YBGS',T1.GUARANTORID)
								,CASE WHEN REPLACE(T1.GUARANTORID,'NCM_','') IS NULL AND T4.CUSTOMERID IS NULL THEN
													 CASE WHEN T1.CERTTYPE LIKE 'Ind%' THEN 'XN-GRKH'
													 			ELSE 'XN-YBGS'
													 END
											WHEN REPLACE(T1.GUARANTORID,'NCM_','') IS NULL THEN T4.CUSTOMERID
											ELSE T1.GUARANTORID
								 END																							AS	GUARANTORID          --保证人ID              					如果保证人为空则默认保证人为一般公司，担保金额大于0的担保合同都有担保人
								,'01'																							AS	CREDITRISKDATATYPE   --信用风险数据类型      					默认：一般非零售(01)
								,T1.GUARANTYTYPE																	AS	GUARANTEEWAY       	 --担保方式
								,''																								AS	QUALFLAGSTD          --权重法合格标识
								,''																								AS	QUALFLAGFIRB         --内评初级法合格标识
								,''																								AS	GUARANTEETYPESTD     --权重法保证类型
								,''																								AS	GUARANTORSDVSSTD     --权重法保证人细分
								,''																								AS	GUARANTEETYPEIRB     --内评法保证类型
								,T1.GUARANTYVALUE																	AS	GUARANTEEAMOUNT      --保证总额
								,NVL(T1.GUARANTYCURRENCY,'CNY')										AS	CURRENCY             --币种
								,T2.STARTDATE																		  AS	STARTDATE            --起始日期
								,T2.DUEDATE																				AS	DUEDATE              --到期日期
								,CASE WHEN (TO_DATE(T2.DUEDATE,'YYYYMMDD')-TO_DATE(T2.STARTDATE,'YYYYMMDD'))/365<0
								      THEN 0
								      ELSE (TO_DATE(T2.DUEDATE,'YYYYMMDD')-TO_DATE(T2.STARTDATE,'YYYYMMDD'))/365
								END																								AS  ORIGINALMATURITY   	 --原始期限
								,CASE WHEN (TO_DATE(T2.DUEDATE,'YYYYMMDD')-TO_DATE(p_data_dt_str,'YYYYMMDD'))/365<0
								      THEN 0
								      ELSE (TO_DATE(T2.DUEDATE,'YYYYMMDD')-TO_DATE(p_data_dt_str,'YYYYMMDD'))/365
								END																								AS	RESIDUALM            --剩余期限
								,T3.PDADJLEVEL																		AS	GUARANTORIRATING     --保证人内部评级
								,T3.PD																						AS	GUARANTORPD          --保证人违约概率
								,''																								AS	GROUPID              --分组编号
    FROM				RWA_DEV.NCM_GUARANTY_CONTRACT T1 									--担保合同信息表
		INNER JOIN  TEMP_BND_GUARANTEE T2				                      --应收款投资保证临时表
    ON          T1.SERIALNO = T2.SERIALNO
    LEFT JOIN 	TEMP_CUST_INFO T4
    ON					T1.CERTTYPE = T4.CERTTYPE
    AND					T1.CERTID = T4.CERTID
    LEFT JOIN		RWA_DEV.RWA_TEMP_PDLEVEL T3
    ON					DECODE(T1.GUARANTORID,'NCM_',T4.CUSTOMERID,NULL,T4.CUSTOMERID,'',T4.CUSTOMERID,T1.GUARANTORID) = T3.CUSTID
		WHERE 			T1.GUARANTYTYPE = '010'														--保证
		AND					T1.GUARANTYVALUE > 0
		AND 				T1.DATANO = p_data_dt_str
		AND NOT EXISTS (SELECT 1 FROM RWA_DEV.RWA_TZ_GUARANTEE TG WHERE 'BZ' || T1.SERIALNO = TG.GUARANTEEID)
		;

    COMMIT;

    --2.3 财务系统-应收款投资-信用证、备用信用证、融资性保函、非融资性保函-非基于银行-合同
    INSERT INTO RWA_DEV.RWA_TZ_GUARANTEE(
         				 DataDate          												  --数据日期
								,DataNo                                     --数据流水号
								,GuaranteeID                                --保证ID
								,SSysID                                     --源系统ID
								,GuaranteeConID                             --保证合同ID
								,GuarantorID                                --保证人ID
								,CreditRiskDataType                         --信用风险数据类型
								,GuaranteeWay                            		--担保方式
								,QualFlagSTD                            		--权重法合格标识
								,QualFlagFIRB                               --内评初级法合格标识
								,GuaranteeTypeSTD                           --权重法保证类型
								,GuarantorSdvsSTD                           --权重法保证人细分
								,GuaranteeTypeIRB                           --内评法保证类型
								,GuaranteeAmount                            --保证总额
								,Currency                                   --币种
								,StartDate                                  --起始日期
								,DueDate                                    --到期日期
								,OriginalMaturity                           --原始期限
								,ResidualM                                  --剩余期限
								,GuarantorIRating                           --保证人内部评级
								,GuarantorPD                                --保证人违约概率
								,GroupID                                    --分组编号
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
						     										 AND DATANO = p_data_dt_str) T2
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
						    						 AND T5.GUARANTYTYPEID IN ('004001004001','004001005001','004001006001','004001006002')     					--信用证、备用信用证、融资性保函、非融资性保函都归为保证
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
                                       AND T2.GUARANTYTYPEID IN
                                           ('004001004001',
                                            '004001005001',
                                            '004001006001',
                                            '004001006002',
                                            '001001003001')
                                       AND T2.AFFIRMVALUE0 > 0)
                               AND T1.DATANO = p_data_dt_str)
                     WHERE RM = 1
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
         				 TO_DATE(p_data_dt_str,'YYYYMMDD')								AS  DATADATE          	 --数据日期
         				,p_data_dt_str														 		    AS	DATANO               --数据流水号
         				,'YP' || T1.GUARANTYID														AS	GUARANTEEID          --保证ID
								,'TZ'																						  AS	SSYSID               --源系统ID
								,T1.GUARANTYID																		AS	GUARANTEECONID       --保证合同ID
								,NVL(T6.CUSTOMERID,'XN-YBGS')											AS	GUARANTORID          --保证人ID
								,'01'																							AS	CREDITRISKDATATYPE   --信用风险数据类型      					默认 一般非零售(01)
								,T1.GUARANTYTYPEID																AS	GUARANTEEWAY       	 --担保方式              					默认 保证(010)
								,''																								AS	QUALFLAGSTD          --权重法合格标识
								,CASE WHEN T1.QUALIFYFLAG03	= '01' THEN '1'
											WHEN T1.QUALIFYFLAG03	= '02' THEN '0'
								 ELSE ''
								 END																							AS	QUALFLAGFIRB         --内评初级法合格标识
								,''																								AS	GUARANTEETYPESTD     --权重法保证类型
								,''																								AS	GUARANTORSDVSSTD     --权重法保证人细分
								,CASE WHEN T1.QUALIFYFLAG03 = '01' THEN '020201'
								 ELSE ''
								 END																							AS	GUARANTEETYPEIRB     --内评法保证类型
								,T1.AFFIRMVALUE0																	AS	GUARANTEEAMOUNT      --保证总额
								,NVL(T1.AFFIRMCURRENCY,'CNY')											AS	CURRENCY             --币种
								,T2.STARTDATE																		  AS	STARTDATE            --起始日期
								,T2.DUEDATE																				AS	DUEDATE              --到期日期
								,CASE WHEN (TO_DATE(T2.DUEDATE,'YYYYMMDD')-TO_DATE(T2.STARTDATE,'YYYYMMDD'))/365<0
								      THEN 0
								      ELSE (TO_DATE(T2.DUEDATE,'YYYYMMDD')-TO_DATE(T2.STARTDATE,'YYYYMMDD'))/365
								END																								AS  ORIGINALMATURITY   	 --原始期限
								,CASE WHEN (TO_DATE(T2.DUEDATE,'YYYYMMDD')-TO_DATE(p_data_dt_str,'YYYYMMDD'))/365<0
								      THEN 0
								      ELSE (TO_DATE(T2.DUEDATE,'YYYYMMDD')-TO_DATE(p_data_dt_str,'YYYYMMDD'))/365
								END																								AS	RESIDUALM            --剩余期限
								,T8.PDADJLEVEL																		AS	GUARANTORIRATING     --保证人内部评级
								,T8.PD																						AS	GUARANTORPD          --保证人违约概率
								,''																								AS	GROUPID              --分组编号
    FROM				RWA_DEV.NCM_GUARANTY_INFO T1															--信贷抵质押品信息表
    INNER JOIN	TEMP_BND_COLLATERAL	T2																		--应收款投资抵质押品临时表
   	ON					T1.GUARANTYID = T2.GUARANTYID
   	LEFT JOIN		TEMP_CUST_INFO T6
    ON					T1.OBLIGEEIDTYPE = T6.CERTTYPE
    AND 				T1.OBLIGEEIDNUMBER = T6.CERTID
    LEFT JOIN		TMP_CUST_IRATING T8																--客户内部评级临时表
    ON					REPLACE(T1.OBLIGEEIDNUMBER,'-','') = REPLACE(T8.ORGCERTCODE,'-','')
    AND					T1.OBLIGEEIDTYPE IN ('Ent01','Ent02')
		WHERE 			T1.GUARANTYTYPEID IN ('004001004001','004001005001','004001006001','004001006002')
																																					--信用证、备用信用证、融资性保函、非融资性保函都归为保证
		AND					T1.AFFIRMVALUE0 > 0
		AND 				T1.DATANO = p_data_dt_str
		;

    COMMIT;

    --2.4 财务系统-应收款投资-信用证、备用信用证、融资性保函、非融资性保函-非基于银行-出账
    INSERT INTO RWA_DEV.RWA_TZ_GUARANTEE(
         				 DataDate          												  --数据日期
								,DataNo                                     --数据流水号
								,GuaranteeID                                --保证ID
								,SSysID                                     --源系统ID
								,GuaranteeConID                             --保证合同ID
								,GuarantorID                                --保证人ID
								,CreditRiskDataType                         --信用风险数据类型
								,GuaranteeWay                            		--担保方式
								,QualFlagSTD                            		--权重法合格标识
								,QualFlagFIRB                               --内评初级法合格标识
								,GuaranteeTypeSTD                           --权重法保证类型
								,GuarantorSdvsSTD                           --权重法保证人细分
								,GuaranteeTypeIRB                           --内评法保证类型
								,GuaranteeAmount                            --保证总额
								,Currency                                   --币种
								,StartDate                                  --起始日期
								,DueDate                                    --到期日期
								,OriginalMaturity                           --原始期限
								,ResidualM                                  --剩余期限
								,GuarantorIRating                           --保证人内部评级
								,GuarantorPD                                --保证人违约概率
								,GroupID                                    --分组编号
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
									    INNER JOIN RWA_DEV.NCM_BUSINESS_PUTOUT T2									--出账表
															ON T1.CONTRACTNO = T2.CONTRACTSERIALNO
												 		 AND T2.DATANO = p_data_dt_str
											INNER JOIN RWA_DEV.NCM_GUARANTY_RELATIVE T3								--押品关联表
															ON T2.SERIALNO = T3.OBJECTNO
												 		 AND T3.OBJECTTYPE = 'PutOutApply'
												 		 AND T3.DATANO = p_data_dt_str
						    			INNER JOIN RWA_DEV.NCM_GUARANTY_INFO T5															--信贷抵质押品信息表
						    							ON T3.GUARANTYID = T5.GUARANTYID
						    						 AND T5.GUARANTYTYPEID IN ('004001004001','004001005001','004001006001','004001006002')     					--信用证、备用信用证、融资性保函、非融资性保函都归为保证
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
                                       AND T2.GUARANTYTYPEID IN
                                           ('004001004001',
                                            '004001005001',
                                            '004001006001',
                                            '004001006002',
                                            '001001003001')
                                       AND T2.AFFIRMVALUE0 > 0)
                               AND T1.DATANO = p_data_dt_str)
                     WHERE RM = 1
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
         				 TO_DATE(p_data_dt_str,'YYYYMMDD')								AS  DATADATE          	 --数据日期
         				,p_data_dt_str														 		    AS	DATANO               --数据流水号
         				,'YP' || T1.GUARANTYID														AS	GUARANTEEID          --保证ID
								,'TZ'																						  AS	SSYSID               --源系统ID
								,T1.GUARANTYID																		AS	GUARANTEECONID       --保证合同ID
								,NVL(T6.CUSTOMERID,'XN-YBGS')											AS	GUARANTORID          --保证人ID
								,'01'																							AS	CREDITRISKDATATYPE   --信用风险数据类型      					默认 一般非零售(01)
								,T1.GUARANTYTYPEID																AS	GUARANTEEWAY       	 --担保方式              					默认 保证(010)
								,''																								AS	QUALFLAGSTD          --权重法合格标识
								,CASE WHEN T1.QUALIFYFLAG03	= '01' THEN '1'
											WHEN T1.QUALIFYFLAG03	= '02' THEN '0'
								 			ELSE ''
								 END																							AS	QUALFLAGFIRB         --内评初级法合格标识
								,''																								AS	GUARANTEETYPESTD     --权重法保证类型
								,''																								AS	GUARANTORSDVSSTD     --权重法保证人细分
								,CASE WHEN T1.QUALIFYFLAG03 = '01' THEN '020201'
								 ELSE ''
								 END																							AS	GUARANTEETYPEIRB     --内评法保证类型
								,T1.AFFIRMVALUE0																	AS	GUARANTEEAMOUNT      --保证总额
								,NVL(T1.AFFIRMCURRENCY,'CNY')											AS	CURRENCY             --币种
								,T2.STARTDATE																		  AS	STARTDATE            --起始日期
								,T2.DUEDATE																				AS	DUEDATE              --到期日期
								,CASE WHEN (TO_DATE(T2.DUEDATE,'YYYYMMDD')-TO_DATE(T2.STARTDATE,'YYYYMMDD'))/365<0
								      THEN 0
								      ELSE (TO_DATE(T2.DUEDATE,'YYYYMMDD')-TO_DATE(T2.STARTDATE,'YYYYMMDD'))/365
								END																								AS  ORIGINALMATURITY   	 --原始期限
								,CASE WHEN (TO_DATE(T2.DUEDATE,'YYYYMMDD')-TO_DATE(p_data_dt_str,'YYYYMMDD'))/365<0
								      THEN 0
								      ELSE (TO_DATE(T2.DUEDATE,'YYYYMMDD')-TO_DATE(p_data_dt_str,'YYYYMMDD'))/365
								END																								AS	RESIDUALM            --剩余期限
								,T8.PDADJLEVEL																		AS	GUARANTORIRATING     --保证人内部评级
								,T8.PD																						AS	GUARANTORPD          --保证人违约概率
								,''																								AS	GROUPID              --分组编号
    FROM				RWA_DEV.NCM_GUARANTY_INFO T1															--信贷抵质押品信息表
    INNER JOIN	TEMP_BND_COLLATERAL	T2																		--应收款投资抵质押品临时表
   	ON					T1.GUARANTYID = T2.GUARANTYID
   	LEFT JOIN		TEMP_CUST_INFO T6
    ON					T1.OBLIGEEIDTYPE = T6.CERTTYPE
    AND 				T1.OBLIGEEIDNUMBER = T6.CERTID
    LEFT JOIN		TMP_CUST_IRATING T8																--客户内部评级临时表
    ON					REPLACE(T1.OBLIGEEIDNUMBER,'-','') = REPLACE(T8.ORGCERTCODE,'-','')
    AND					T1.OBLIGEEIDTYPE IN ('Ent01','Ent02')
		WHERE 			T1.GUARANTYTYPEID IN ('004001004001','004001005001','004001006001','004001006002')
																																					--信用证、备用信用证、融资性保函、非融资性保函都归为保证
		AND					T1.AFFIRMVALUE0 > 0
		AND 				T1.DATANO = p_data_dt_str
		AND NOT EXISTS (SELECT 1 FROM RWA_DEV.RWA_TZ_GUARANTEE TG WHERE 'YP' || T1.GUARANTYID = TG.GUARANTEEID)
		;

    COMMIT;

    --2.5 财务系统-债券投资-外币-保证
    INSERT INTO RWA_DEV.RWA_TZ_GUARANTEE(
         				 DataDate          												  --数据日期
								,DataNo                                     --数据流水号
								,GuaranteeID                                --保证ID
								,SSysID                                     --源系统ID
								,GuaranteeConID                             --保证合同ID
								,GuarantorID                                --保证人ID
								,CreditRiskDataType                         --信用风险数据类型
								,GuaranteeWay                            		--担保方式
								,QualFlagSTD                            		--权重法合格标识
								,QualFlagFIRB                               --内评初级法合格标识
								,GuaranteeTypeSTD                           --权重法保证类型
								,GuarantorSdvsSTD                           --权重法保证人细分
								,GuaranteeTypeIRB                           --内评法保证类型
								,GuaranteeAmount                            --保证总额
								,Currency                                   --币种
								,StartDate                                  --起始日期
								,DueDate                                    --到期日期
								,OriginalMaturity                           --原始期限
								,ResidualM                                  --剩余期限
								,GuarantorIRating                           --保证人内部评级
								,GuarantorPD                                --保证人违约概率
								,GroupID                                    --分组编号
    )
    SELECT
         				 TO_DATE(p_data_dt_str,'YYYYMMDD')								AS  DATADATE          	 --数据日期
         				,p_data_dt_str														 		    AS	DATANO               --数据流水号
         				,'TZBOND' || T5.SERIALNO													AS	GUARANTEEID          --保证ID
								,'TZ'																						  AS	SSYSID               --源系统ID
								,T5.SERIALNO																			AS	GUARANTEECONID       --保证合同ID
								,T5.THIRDPARTYID1																	AS	GUARANTORID          --保证人ID
								,'01'																							AS	CREDITRISKDATATYPE   --信用风险数据类型      					默认 一般非零售(01)
								,'010'																						AS	GUARANTEEWAY       	 --担保方式              					默认 保证(010)
								,''																								AS	QUALFLAGSTD          --权重法合格标识
								,''																								AS	QUALFLAGFIRB         --内评初级法合格标识
								,''																								AS	GUARANTEETYPESTD     --权重法保证类型
								,''																								AS	GUARANTORSDVSSTD     --权重法保证人细分
								,''																								AS	GUARANTEETYPEIRB     --内评法保证类型
								,T1.CONTRACTAMOUNT																AS	GUARANTEEAMOUNT      --保证总额
								,T1.SETTLEMENTCURRENCY														AS	CURRENCY             --币种
								,T1.STARTDATE																		  AS	STARTDATE            --起始日期
								,T1.DUEDATE																				AS	DUEDATE              --到期日期
								,T1.ORIGINALMATURITY															AS  ORIGINALMATURITY   	 --原始期限
								,T1.RESIDUALM																			AS	RESIDUALM            --剩余期限
								,T6.PDADJLEVEL																		AS	GUARANTORIRATING     --保证人内部评级
								,T6.PD																						AS	GUARANTORPD          --保证人违约概率
								,''																								AS	GROUPID              --分组编号
    FROM				RWA_DEV.RWA_TZ_CONTRACT T1	 															--投资合同表
		INNER JOIN	RWA_DEV.NCM_BOND_INFO T4																	--信贷债券信息表
	  ON					T1.SCONTRACTID = T4.OBJECTNO
		AND					T4.OBJECTTYPE = 'BusinessContract'
		AND					T4.DATANO = p_data_dt_str
		INNER JOIN	(SELECT	 SERIALNO
												,THIRDPARTYID1
										FROM RWA_DEV.NCM_BUSINESS_CONTRACT										--信贷合同表
									 WHERE BUSINESSTYPE = '1040202010' 											--外币债券投资
									 	 AND VOUCHTYPE2 = '1'																	--有担保人信息
									 	 AND DATANO = p_data_dt_str) T5
		ON					T1.SCONTRACTID = T5.SERIALNO
		LEFT JOIN		RWA_DEV.RWA_TEMP_PDLEVEL T6
		ON					T5.THIRDPARTYID1 = T6.CUSTID
		LEFT JOIN		RWA_DEV.NCM_CUSTOMER_INFO T7
    ON					T5.THIRDPARTYID1 = T7.CUSTOMERID
    AND					T7.DATANO = p_data_dt_str
	  WHERE 			T1.BUSINESSTYPEID IN ('1040202010','1040202011')					--外币债券投资业务
		AND T1.DATANO=p_data_dt_str
    ;

    COMMIT;

    --2.6 财务系统-债券投资-货币基金-补录
    INSERT INTO RWA_DEV.RWA_TZ_GUARANTEE(
         				 DataDate          												  --数据日期
								,DataNo                                     --数据流水号
								,GuaranteeID                                --保证ID
								,SSysID                                     --源系统ID
								,GuaranteeConID                             --保证合同ID
								,GuarantorID                                --保证人ID
								,CreditRiskDataType                         --信用风险数据类型
								,GuaranteeWay                            		--担保方式
								,QualFlagSTD                            		--权重法合格标识
								,QualFlagFIRB                               --内评初级法合格标识
								,GuaranteeTypeSTD                           --权重法保证类型
								,GuarantorSdvsSTD                           --权重法保证人细分
								,GuaranteeTypeIRB                           --内评法保证类型
								,GuaranteeAmount                            --保证总额
								,Currency                                   --币种
								,StartDate                                  --起始日期
								,DueDate                                    --到期日期
								,OriginalMaturity                           --原始期限
								,ResidualM                                  --剩余期限
								,GuarantorIRating                           --保证人内部评级
								,GuarantorPD                                --保证人违约概率
								,GroupID                                    --分组编号
    )
    SELECT
         				 TO_DATE(p_data_dt_str,'YYYYMMDD')								AS  DATADATE          	 --数据日期
         				,p_data_dt_str														 		    AS	DATANO               --数据流水号
         				,T1.MITIGATIONID																	AS	GUARANTEEID          --保证ID
								,'TZHBJJ'																				  AS	SSYSID               --源系统ID
								,T1.BOND_ID																				AS	GUARANTEECONID       --保证合同ID
								,T1.CUSTID1																				AS	GUARANTORID          --保证人ID
								,'01'																							AS	CREDITRISKDATATYPE   --信用风险数据类型      					默认 一般非零售(01)
								,T1.GUARANTYTYPE																	AS	GUARANTEEWAY       	 --担保方式
								,''																								AS	QUALFLAGSTD          --权重法合格标识
								,''																								AS	QUALFLAGFIRB         --内评初级法合格标识
								,''																								AS	GUARANTEETYPESTD     --权重法保证类型
								,''																								AS	GUARANTORSDVSSTD     --权重法保证人细分
								,''																								AS	GUARANTEETYPEIRB     --内评法保证类型
								,ROUND(TO_NUMBER(REPLACE(NVL(T1.GUARANTYSUM,'0'),',','')),6)
																																	AS	GUARANTEEAMOUNT      --保证总额
								,NVL(T1.GUARANTYCURRENCYCODE,'CNY')								AS	CURRENCY             --币种
								,T3.STARTDATE																		  AS	STARTDATE            --起始日期
								,T3.DUEDATE																				AS	DUEDATE              --到期日期
								,T3.ORIGINALMATURITY															AS  ORIGINALMATURITY   	 --原始期限
								,T3.RESIDUALM																			AS	RESIDUALM            --剩余期限
								,''																								AS	GUARANTORIRATING     --保证人内部评级
								,NULL																							AS	GUARANTORPD          --保证人违约概率
								,''																								AS	GROUPID              --分组编号
    FROM				RWA.RWA_WS_BONDTRADE_MF T1	 																--货币基金债券投资补录表
		INNER JOIN  RWA.RWA_WP_DATASUPPLEMENT T2
    ON          T1.SUPPORGID = T2.ORGID
    AND         T2.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    AND         T2.SUPPTMPLID = 'M-0071'
    AND         T2.SUBMITFLAG = '1'
    LEFT JOIN		RWA_DEV.RWA_TZ_EXPOSURE T3
    ON					T1.BOND_ID = T3.DUEID
		WHERE 			T1.GUARANTYTYPE IN ('004001004001','004001005001','004001006001','004001006002','010')
		AND					T1.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
		;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_TZ_GUARANTEE',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_TZ_GUARANTEE;
    --Dbms_output.Put_line('RWA_DEV.RWA_TZ_GUARANTEE表当前插入的财务系统-应收款投资数据记录为:' || v_count || '条');


		--Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功' || '-' || v_count;
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '投资类保证('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_TZ_GUARANTEE;
/

