CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_TZ_CMRELEVENCE(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期 yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_TZ_CMRELEVENCE
    实现功能:财务系统-投资-合同缓释物关联(从数据源财务系统将应收款投资相关信息全量导入RWA投资类接口表合同缓释物关联表中)
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
    目标表1 :RWA_DEV.RWA_TZ_CMRELEVENCE|财务系统投资类合同缓释物关联表
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_TZ_CMRELEVENCE';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TZ_CMRELEVENCE';

    --2.将满足条件的数据从源表插入到目标表中
    --2.1财务系统-应收款投资-抵质押品-非基于银行-合同
    INSERT INTO RWA_DEV.RWA_TZ_CMRELEVENCE(
                DATADATE                               --数据日期
                ,DATANO                                --数据流水号
                ,CONTRACTID                       	 	 --合同ID
                ,MITIGATIONID                     	 	 --缓释物ID
                ,MITIGCATEGORY                    	 	 --缓释物类型
                ,SGUARCONTRACTID                  	 	 --源担保合同ID
                ,GROUPID                          	 	 --分组编号
    )
    WITH TMP_BND_CONTRACT AS (
							SELECT --DISTINCT
										 T1.CONTRACTID				AS BOND_ID
										,T1.SCONTRACTID 			AS CONTRACTNO
								FROM RWA_DEV.RWA_TZ_CONTRACT T1	 																--投资合同表
					INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT TC														--信贷合同表
									ON T1.SCONTRACTID = TC.SERIALNO
								 AND (TC.BUSINESSSUBTYPE NOT LIKE '0010%' OR TC.BUSINESSSUBTYPE IS NULL) 			--非基于银行
								 AND TC.DATANO = T1.DATANO
	    				 WHERE T1.BUSINESSTYPEID IN ('1040105060','1040105061','1040105062')	--应收款投资业务
		)
		, TMP_BND_GUARANTEE AS (
							SELECT DISTINCT
										 T1.BOND_ID					AS CONTRACTNO
										,T5.GUARANTYID			AS GUARANTYID
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
    									) T4													--信贷担保合同与抵质押品关联表
    							ON T3.SERIALNO = T4.CONTRACTNO
    			INNER JOIN RWA_DEV.NCM_GUARANTY_INFO T5															--信贷抵质押品信息表
    							ON T4.GUARANTYID = T5.GUARANTYID
    						 AND T5.GUARANTYTYPEID NOT IN ('004001004001','004001005001','004001006001','004001006002','001001003001')   --信用证、备用信用证、融资性保函、非融资性保函都归为保证，保证金不取
    						 AND T5.CLRSTATUS = '01'																			--押品实物状态：正常
    						 AND T5.CLRGNTSTATUS IN ('03','10')														--押品设押状态：03-已确立押权，10-已入库
    						 AND T5.AFFIRMVALUE0 > 0
    						 AND T5.DATANO = p_data_dt_str
		)
    SELECT      --DISTINCT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                ,p_data_dt_str													     AS DATANO                   --数据流水号
                ,T1.CONTRACTNO						                   AS CONTRACTID               --合同ID
                ,'YP' || T1.GUARANTYID											 AS MITIGATIONID     	 	 	 	 --缓释物ID
                ,'03'																				 AS MITIGCATEGORY            --缓释物类型                 默认 抵质押品(03)
                ,''																					 AS SGUARCONTRACTID          --源担保合同ID
                ,''                         				 				 AS GROUPID                	 --分组编号

    FROM				TMP_BND_GUARANTEE T1
		;

    COMMIT;

    --2.2财务系统-应收款投资-抵质押品-非基于银行-出账
    INSERT INTO RWA_DEV.RWA_TZ_CMRELEVENCE(
                DATADATE                               --数据日期
                ,DATANO                                --数据流水号
                ,CONTRACTID                       	 	 --合同ID
                ,MITIGATIONID                     	 	 --缓释物ID
                ,MITIGCATEGORY                    	 	 --缓释物类型
                ,SGUARCONTRACTID                  	 	 --源担保合同ID
                ,GROUPID                          	 	 --分组编号
    )
    WITH TMP_BND_GUARANTEE AS (
							SELECT DISTINCT
										 T1.CONTRACTID				AS CONTRACTNO
										,GI.GUARANTYID 				AS GUARANTYID
								FROM RWA_DEV.RWA_TZ_CONTRACT T1	 																--投资合同表
					INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT TC														--信贷合同表
									ON T1.SCONTRACTID = TC.SERIALNO
								 AND (TC.BUSINESSSUBTYPE NOT LIKE '0010%' OR TC.BUSINESSSUBTYPE IS NULL) 			--非基于银行
								 AND TC.DATANO = T1.DATANO
					INNER JOIN RWA_DEV.NCM_BUSINESS_PUTOUT TP											--出账表
									ON TC.SERIALNO = TP.CONTRACTSERIALNO
								 AND TP.DATANO = p_data_dt_str
					INNER JOIN RWA_DEV.NCM_GUARANTY_RELATIVE TG										--押品关联表
									ON TP.SERIALNO = TG.OBJECTNO
								 AND TG.OBJECTTYPE = 'PutOutApply'
								 AND TG.DATANO = p_data_dt_str
					INNER JOIN RWA_DEV.NCM_GUARANTY_INFO GI												--押品信息表
									ON TG.GUARANTYID = GI.GUARANTYID
								 AND GI.GUARANTYTYPEID NOT IN ('004001004001','004001005001','004001006001','004001006002','001001003001')   --信用证、备用信用证、融资性保函、非融资性保函都归为保证，保证金不取
				    		 AND GI.CLRSTATUS = '01'																			--押品实物状态：正常
				    		 AND GI.CLRGNTSTATUS IN ('03','10')														--押品设押状态：03-已确立押权，10-已入库
				    		 AND GI.AFFIRMVALUE0 > 0
				    		 AND GI.DATANO = p_data_dt_str
	    				 WHERE T1.BUSINESSTYPEID IN ('1040105060','1040105061','1040105062')	--应收款投资业务
		)
    SELECT      --DISTINCT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                ,p_data_dt_str													     AS DATANO                   --数据流水号
                ,T1.CONTRACTNO						                   AS CONTRACTID               --合同ID
                ,'YP' || T1.GUARANTYID											 AS MITIGATIONID     	 	 	 	 --缓释物ID
                ,'03'																				 AS MITIGCATEGORY            --缓释物类型                 默认 抵质押品(03)
                ,''																					 AS SGUARCONTRACTID          --源担保合同ID
                ,''                         				 				 AS GROUPID                	 --分组编号

    FROM				TMP_BND_GUARANTEE T1
    WHERE NOT EXISTS (SELECT 1 FROM RWA_TZ_CMRELEVENCE T2 WHERE T1.CONTRACTNO = T2.CONTRACTID AND 'YP' || T1.GUARANTYID = T2.MITIGATIONID)
		;

    COMMIT;

    --2.3财务系统-应收款投资-保证(信用证、备用信用证、融资性保函、非融资性保函)-非基于银行-合同
    INSERT INTO RWA_DEV.RWA_TZ_CMRELEVENCE(
                DATADATE                               --数据日期
                ,DATANO                                --数据流水号
                ,CONTRACTID                       	 	 --合同ID
                ,MITIGATIONID                     	 	 --缓释物ID
                ,MITIGCATEGORY                    	 	 --缓释物类型
                ,SGUARCONTRACTID                  	 	 --源担保合同ID
                ,GROUPID                          	 	 --分组编号
    )
    WITH TMP_BND_CONTRACT AS (
							SELECT --DISTINCT
										 T1.CONTRACTID				AS BOND_ID
										,T1.SCONTRACTID 			AS CONTRACTNO
								FROM RWA_DEV.RWA_TZ_CONTRACT T1	 																--投资合同表
					INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT TC														--信贷合同表
									ON T1.SCONTRACTID = TC.SERIALNO
								 AND (TC.BUSINESSSUBTYPE NOT LIKE '0010%' OR TC.BUSINESSSUBTYPE IS NULL) 			--非基于银行
								 AND TC.DATANO = T1.DATANO
	    				 WHERE T1.BUSINESSTYPEID IN ('1040105060','1040105061','1040105062')	--应收款投资业务
		)
		, TMP_BND_GUARANTEE AS (
							SELECT DISTINCT
										 T1.BOND_ID					AS CONTRACTNO
										,T5.GUARANTYID			AS GUARANTYID
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
    									) T4													--信贷担保合同与抵质押品关联表
    							ON T3.SERIALNO = T4.CONTRACTNO
    			INNER JOIN RWA_DEV.NCM_GUARANTY_INFO T5															--信贷抵质押品信息表
    							ON T4.GUARANTYID = T5.GUARANTYID
    						 AND T5.GUARANTYTYPEID IN ('004001004001','004001005001','004001006001','004001006002')     					--信用证、备用信用证、融资性保函、非融资性保函都归为保证
    						 AND T5.CLRSTATUS = '01'																			--押品实物状态：正常
    						 AND T5.CLRGNTSTATUS IN ('03','10')														--押品设押状态：03-已确立押权，10-已入库
    						 AND T5.AFFIRMVALUE0 > 0
    						 AND T5.DATANO = p_data_dt_str
		)
    SELECT      --DISTINCT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                ,p_data_dt_str													     AS DATANO                   --数据流水号
                ,T1.CONTRACTNO						                   AS CONTRACTID               --合同ID
                ,'YP' || T1.GUARANTYID											 AS MITIGATIONID     	 	 	 	 --缓释物ID
                ,'02'																				 AS MITIGCATEGORY            --缓释物类型                 默认 保证(02)
                ,''																					 AS SGUARCONTRACTID          --源担保合同ID
                ,''                         				 				 AS GROUPID                	 --分组编号

    FROM				TMP_BND_GUARANTEE T1
		;

    COMMIT;

    --2.4财务系统-应收款投资-保证(信用证、备用信用证、融资性保函、非融资性保函)-非基于银行-出账
    INSERT INTO RWA_DEV.RWA_TZ_CMRELEVENCE(
                DATADATE                               --数据日期
                ,DATANO                                --数据流水号
                ,CONTRACTID                       	 	 --合同ID
                ,MITIGATIONID                     	 	 --缓释物ID
                ,MITIGCATEGORY                    	 	 --缓释物类型
                ,SGUARCONTRACTID                  	 	 --源担保合同ID
                ,GROUPID                          	 	 --分组编号
    )
    WITH TMP_BND_GUARANTEE AS (
							SELECT DISTINCT
										 T1.CONTRACTID				AS CONTRACTNO
										,GI.GUARANTYID 				AS GUARANTYID
								FROM RWA_DEV.RWA_TZ_CONTRACT T1	 																--投资合同表
					INNER JOIN RWA_DEV.NCM_BUSINESS_CONTRACT TC														--信贷合同表
									ON T1.SCONTRACTID = TC.SERIALNO
								 AND (TC.BUSINESSSUBTYPE NOT LIKE '0010%' OR TC.BUSINESSSUBTYPE IS NULL) 			--非基于银行
								 AND TC.DATANO = p_data_dt_str
					INNER JOIN RWA_DEV.NCM_BUSINESS_PUTOUT TP															--出账表
									ON T1.SCONTRACTID = TP.CONTRACTSERIALNO
								 AND TP.DATANO = p_data_dt_str
					INNER JOIN RWA_DEV.NCM_GUARANTY_RELATIVE TG														--押品关联表
									ON TP.SERIALNO = TG.OBJECTNO
								 AND TG.OBJECTTYPE = 'PutOutApply'
								 AND TG.DATANO = p_data_dt_str
					INNER JOIN RWA_DEV.NCM_GUARANTY_INFO GI																--押品信息表
									ON TG.GUARANTYID = GI.GUARANTYID
								 AND GI.GUARANTYTYPEID IN ('004001004001','004001005001','004001006001','004001006002')     					--信用证、备用信用证、融资性保函、非融资性保函都归为保证
				    		 AND GI.CLRSTATUS = '01'																				--押品实物状态：正常
				    		 AND GI.CLRGNTSTATUS IN ('03','10')															--押品设押状态：03-已确立押权，10-已入库
				    		 AND GI.AFFIRMVALUE0 > 0
				    		 AND GI.DATANO = p_data_dt_str
	    				 WHERE T1.BUSINESSTYPEID IN ('1040105060','1040105061','1040105062')	--应收款投资业务
		)
    SELECT      --DISTINCT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                ,p_data_dt_str													     AS DATANO                   --数据流水号
                ,T1.CONTRACTNO						                   AS CONTRACTID               --合同ID
                ,'YP' || T1.GUARANTYID											 AS MITIGATIONID     	 	 	 	 --缓释物ID
                ,'02'																				 AS MITIGCATEGORY            --缓释物类型                 默认 保证(02)
                ,''																					 AS SGUARCONTRACTID          --源担保合同ID
                ,''                         				 				 AS GROUPID                	 --分组编号

    FROM				TMP_BND_GUARANTEE T1
    WHERE NOT EXISTS (SELECT 1 FROM RWA_TZ_CMRELEVENCE T2 WHERE T1.CONTRACTNO = T2.CONTRACTID AND 'YP' || T1.GUARANTYID = T2.MITIGATIONID)
		;

    COMMIT;


    --2.5财务系统-应收款投资-保证-非基于银行-合同
    INSERT INTO RWA_DEV.RWA_TZ_CMRELEVENCE(
                DATADATE                               --数据日期
                ,DATANO                                --数据流水号
                ,CONTRACTID                       	 	 --合同ID
                ,MITIGATIONID                     	 	 --缓释物ID
                ,MITIGCATEGORY                    	 	 --缓释物类型
                ,SGUARCONTRACTID                  	 	 --源担保合同ID
                ,GROUPID                          	 	 --分组编号
    )
    SELECT      DISTINCT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                ,p_data_dt_str													     AS DATANO                   --数据流水号
                ,T1.CONTRACTID						                   AS CONTRACTID               --合同ID
                ,'BZ' || T5.SERIALNO												 AS MITIGATIONID     	 	 	 	 --缓释物ID
                ,'02'																				 AS MITIGCATEGORY            --缓释物类型                 默认 保证(02)
                ,''																					 AS SGUARCONTRACTID          --源担保合同ID
                ,''                         				 				 AS GROUPID                	 --分组编号

    FROM				RWA_DEV.RWA_TZ_CONTRACT T1	 															--投资合同表
		INNER JOIN	RWA_DEV.NCM_BUSINESS_CONTRACT TC													--信贷合同表
		ON 					T1.SCONTRACTID = TC.SERIALNO
		AND 				(TC.BUSINESSSUBTYPE NOT LIKE '0010%' OR TC.BUSINESSSUBTYPE IS NULL) 			--非基于银行
		AND 				TC.DATANO = p_data_dt_str
		INNER JOIN  (SELECT  DISTINCT
												 SERIALNO
												,OBJECTNO
										FROM RWA_DEV.NCM_CONTRACT_RELATIVE
									 WHERE OBJECTTYPE = 'GuarantyContract'
										 AND DATANO = p_data_dt_str) T4                       --信贷合同关联表
    ON          TC.SERIALNO = T4.SERIALNO
    INNER JOIN	RWA_DEV.NCM_GUARANTY_CONTRACT T5													--信贷担保合同表
    ON					T4.OBJECTNO = T5.SERIALNO
    AND					T5.GUARANTYTYPE = '010'																		--保证
    AND					T5.GUARANTYVALUE > 0
    AND					T5.DATANO = p_data_dt_str
		WHERE 			T1.BUSINESSTYPEID IN ('1040105060','1040105061','1040105062')	--应收款投资业务
		;

    COMMIT;

    --2.6财务系统-应收款投资-保证-非基于银行-出账
    INSERT INTO RWA_DEV.RWA_TZ_CMRELEVENCE(
                DATADATE                               --数据日期
                ,DATANO                                --数据流水号
                ,CONTRACTID                       	 	 --合同ID
                ,MITIGATIONID                     	 	 --缓释物ID
                ,MITIGCATEGORY                    	 	 --缓释物类型
                ,SGUARCONTRACTID                  	 	 --源担保合同ID
                ,GROUPID                          	 	 --分组编号
    )
    SELECT      DISTINCT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                ,p_data_dt_str													     AS DATANO                   --数据流水号
                ,T1.CONTRACTID						                   AS CONTRACTID               --合同ID
                ,'BZ' || T5.SERIALNO												 AS MITIGATIONID     	 	 	 	 --缓释物ID
                ,'02'																				 AS MITIGCATEGORY            --缓释物类型                 默认 保证(02)
                ,''																					 AS SGUARCONTRACTID          --源担保合同ID
                ,''                         				 				 AS GROUPID                	 --分组编号

    FROM				RWA_DEV.RWA_TZ_CONTRACT T1	 															--投资合同表
		INNER JOIN	RWA_DEV.NCM_BUSINESS_CONTRACT TC													--信贷合同表
		ON 					T1.SCONTRACTID = TC.SERIALNO
		AND 				(TC.BUSINESSSUBTYPE NOT LIKE '0010%' OR TC.BUSINESSSUBTYPE IS NULL) 			--非基于银行
		AND 				TC.DATANO = p_data_dt_str
		INNER JOIN	RWA_DEV.NCM_BUSINESS_PUTOUT T3														--出账表
		ON					TC.SERIALNO = T3.CONTRACTSERIALNO
		AND					T3.DATANO = p_data_dt_str
		INNER JOIN	RWA_DEV.NCM_GUARANTY_RELATIVE T4													--押品关联表
		ON					T3.SERIALNO = T4.OBJECTNO
		AND					T4.OBJECTTYPE = 'PutOutApply'
		AND					T4.DATANO = p_data_dt_str
    INNER JOIN	RWA_DEV.NCM_GUARANTY_CONTRACT T5													--信贷担保合同表
    ON					T4.CONTRACTNO = T5.SERIALNO
    AND					T5.GUARANTYTYPE = '010'																		--保证
    AND					T5.GUARANTYVALUE > 0
    AND					T5.DATANO = p_data_dt_str
		WHERE 			T1.BUSINESSTYPEID IN ('1040105060','1040105061','1040105062')	--应收款投资业务
		AND NOT EXISTS (SELECT 1 FROM RWA_TZ_CMRELEVENCE T2 WHERE T1.CONTRACTID = T2.CONTRACTID AND 'BZ' || T5.SERIALNO = T2.MITIGATIONID)
		AND T1.DATANO=p_data_dt_str
    ;

    COMMIT;

    --2.7财务系统-应收款投资-票据资管业务-票据信息
    INSERT INTO RWA_DEV.RWA_TZ_CMRELEVENCE(
                DATADATE                               --数据日期
                ,DATANO                                --数据流水号
                ,CONTRACTID                       	 	 --合同ID
                ,MITIGATIONID                     	 	 --缓释物ID
                ,MITIGCATEGORY                    	 	 --缓释物类型
                ,SGUARCONTRACTID                  	 	 --源担保合同ID
                ,GROUPID                          	 	 --分组编号
    )
    SELECT      DISTINCT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                ,p_data_dt_str													     AS DATANO                   --数据流水号
                ,T1.CONTRACTID						                   AS CONTRACTID               --合同ID
                ,'TZBILL' || T4.SERIALNO										 AS MITIGATIONID     	 	 	 	 --缓释物ID
                ,'03'																				 AS MITIGCATEGORY            --缓释物类型                 默认 抵质押品(03)
                ,''																					 AS SGUARCONTRACTID          --源担保合同ID
                ,''                         				 				 AS GROUPID                	 --分组编号

    FROM				RWA_DEV.RWA_TZ_CONTRACT T1	 															--投资合同表
		INNER JOIN	RWA_DEV.NCM_BUSINESS_CONTRACT TC													--信贷合同表
		ON 					T1.SCONTRACTID = TC.SERIALNO
		AND 				TC.BUSINESSSUBTYPE = '003050' 														--基于投资管理人-票据资管业务
		AND 				TC.DATANO = p_data_dt_str
		INNER JOIN  RWA_DEV.NCM_BILL_INFO T4                       						--票据信息表
    ON          TC.SERIALNO = T4.OBJECTNO
    AND					T4.OBJECTTYPE = 'BusinessContract'
    AND					T4.DATANO = p_data_dt_str
		WHERE 			T1.BUSINESSTYPEID IN ('1040105060','1040105061','1040105062')	--应收款投资业务
		AND T1.DATANO=p_data_dt_str;

    COMMIT;

    --2.8财务系统-债券投资-外币-保证
    INSERT INTO RWA_DEV.RWA_TZ_CMRELEVENCE(
                DATADATE                               --数据日期
                ,DATANO                                --数据流水号
                ,CONTRACTID                       	 	 --合同ID
                ,MITIGATIONID                     	 	 --缓释物ID
                ,MITIGCATEGORY                    	 	 --缓释物类型
                ,SGUARCONTRACTID                  	 	 --源担保合同ID
                ,GROUPID                          	 	 --分组编号
    )
    SELECT      --DISTINCT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                ,p_data_dt_str													     AS DATANO                   --数据流水号
                ,T1.CONTRACTID						                   AS CONTRACTID               --合同ID
                ,'TZBOND' || T5.SERIALNO										 AS MITIGATIONID     	 	 	 	 --缓释物ID
                ,'02'																				 AS MITIGCATEGORY            --缓释物类型                 默认 保证(02)
                ,''																					 AS SGUARCONTRACTID          --源担保合同ID
                ,''                         				 				 AS GROUPID                	 --分组编号

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
	  WHERE 			T1.BUSINESSTYPEID IN ('1040202010','1040202011')					--外币债券投资业务
		AND T1.DATANO=p_data_dt_str
    ;

    COMMIT;

    --2.8财务系统-应收款投资-抵质押品(保证金)
    /*
    INSERT INTO RWA_DEV.RWA_TZ_CMRELEVENCE(
                DATADATE                               --数据日期
                ,DATANO                                --数据流水号
                ,CONTRACTID                       	 	 --合同ID
                ,MITIGATIONID                     	 	 --缓释物ID
                ,MITIGCATEGORY                    	 	 --缓释物类型
                ,SGUARCONTRACTID                  	 	 --源担保合同ID
                ,GROUPID                          	 	 --分组编号
    )
    SELECT      --DISTINCT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                ,p_data_dt_str													     AS DATANO                   --数据流水号
                ,T1.CONTRACTID						                   AS CONTRACTID               --合同ID
                ,T1.SCONTRACTID												 			 AS MITIGATIONID     	 	 	 	 --缓释物ID
                ,'03'																				 AS MITIGCATEGORY            --缓释物类型                 默认 抵质押品(03)
                ,''																					 AS SGUARCONTRACTID          --源担保合同ID
                ,''                         				 				 AS GROUPID                	 --分组编号

    FROM				RWA_DEV.RWA_TZ_CONTRACT T1	 															--投资合同表
		INNER JOIN	(SELECT  SERIALNO
										FROM RWA_DEV.NCM_BUSINESS_CONTRACT
									 WHERE BUSINESSTYPE = '1040105060'											--应收款投资业务
									 	 AND BAILSUM > 0																			--保证金大于0
									 	 AND DATANO = p_data_dt_str) T4
    ON          T1.SCONTRACTID = T4.SERIALNO
		WHERE 			T1.BUSINESSTYPEID IN ('1040105060','1040105061','1040105062')	--应收款投资业务
		;
		*/
		--2.8财务系统-应收款投资-抵质押品(保证金)
		INSERT INTO RWA_DEV.RWA_TZ_CMRELEVENCE(
                DATADATE                               --数据日期
                ,DATANO                                --数据流水号
                ,CONTRACTID                       	 	 --合同ID
                ,MITIGATIONID                     	 	 --缓释物ID
                ,MITIGCATEGORY                    	 	 --缓释物类型
                ,SGUARCONTRACTID                  	 	 --源担保合同ID
                ,GROUPID                          	 	 --分组编号
    )
    SELECT      --DISTINCT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                ,p_data_dt_str													     AS DATANO                   --数据流水号
                ,T1.CONTRACTID						                   AS CONTRACTID               --合同ID
                ,'HT' || T1.SCONTRACTID || T4.BAILCURRENCY
                																						 AS MITIGATIONID     	 	 	 	 --缓释物ID
                ,'03'																				 AS MITIGCATEGORY            --缓释物类型                 默认 抵质押品(03)
                ,''																					 AS SGUARCONTRACTID          --源担保合同ID
                ,''                         				 				 AS GROUPID                	 --分组编号

    FROM				RWA_DEV.RWA_TZ_CONTRACT T1	 															--投资合同表
		INNER JOIN	RWA_DEV.RWA_TEMP_BAIL2 T4
    ON          T1.SCONTRACTID = T4.CONTRACTNO
    AND					T4.ISMAX = '1'																						--取相同合同下最大的一笔作为结果
		WHERE 			T1.BUSINESSTYPEID IN ('1040105060','1040105061','1040105062')	--应收款投资业务
		AND T1.DATANO=p_data_dt_str
    ;

    COMMIT;

    --2.9财务系统-债券投资-货币基金-补录
    --2.9.1 更新押品ID
    UPDATE RWA.RWA_WS_BONDTRADE_MF T
		   SET T.MITIGATIONID =
		       (WITH TMP_BOND AS (SELECT BOND_ID,SUPPSERIALNO, p_data_dt_str || 'TZHBJJ' || lpad(rownum, 4, '0') AS MITIGATIONID
															  FROM (SELECT BOND_ID,SUPPSERIALNO
															          FROM RWA.RWA_WS_BONDTRADE_MF
															         WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
															         	 AND GUARANTYTYPE IS NOT NULL
															         ORDER BY BOND_ID,SUPPSERIALNO))
		         SELECT T1.MITIGATIONID
		           FROM TMP_BOND T1
		          WHERE T1.BOND_ID = T.BOND_ID
		          	AND T1.SUPPSERIALNO = T.SUPPSERIALNO)
		          WHERE T.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
		;

		COMMIT;

    --2.9.2 插入缓释关系
    INSERT INTO RWA_DEV.RWA_TZ_CMRELEVENCE(
                DATADATE                               --数据日期
                ,DATANO                                --数据流水号
                ,CONTRACTID                       	 	 --合同ID
                ,MITIGATIONID                     	 	 --缓释物ID
                ,MITIGCATEGORY                    	 	 --缓释物类型
                ,SGUARCONTRACTID                  	 	 --源担保合同ID
                ,GROUPID                          	 	 --分组编号
    )
    SELECT      --DISTINCT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                ,p_data_dt_str													     AS DATANO                   --数据流水号
                ,T1.BOND_ID								                   AS CONTRACTID               --合同ID
                ,T1.MITIGATIONID														 AS MITIGATIONID     	 	 	 	 --缓释物ID
                ,CASE WHEN T1.GUARANTYTYPE IN ('004001004001','004001005001','004001006001','004001006002','010') THEN  '02'
                 ELSE '03'
                 END																				 AS MITIGCATEGORY            --缓释物类型                 默认 抵质押品(03)
                ,''																					 AS SGUARCONTRACTID          --源担保合同ID
                ,''                         				 				 AS GROUPID                	 --分组编号

    FROM				RWA.RWA_WS_BONDTRADE_MF T1	 																--货币基金债券投资补录表
		INNER JOIN  RWA.RWA_WP_DATASUPPLEMENT T2
    ON          T1.SUPPORGID = T2.ORGID
    AND         T2.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    AND         T2.SUPPTMPLID = 'M-0071'
    AND         T2.SUBMITFLAG = '1'
		WHERE 			T1.GUARANTYTYPE IS NOT NULL
		AND					T1.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
		;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_TZ_CMRELEVENCE',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_TZ_CMRELEVENCE;
    --Dbms_output.Put_line('RWA_DEV.RWA_TZ_CMRELEVENCE表当前插入的财务系统-应收款投资数据记录为: ' || v_count || ' 条');



    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功' || '-' || v_count;
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '合同缓释物关联('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_TZ_CMRELEVENCE;
/

