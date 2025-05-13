CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_LC_COLLATERAL(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期 yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_LC_COLLATERAL
    实现功能:理财系统-资管计划投资-抵质押品(从数据源理财系统将资管计划投资相关信息全量导入RWA理财投资类接口表抵质押品表中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :LISY
    编写时间:2016-04-21
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.ZGS_INVESTASSETDETAIL|资产详情表
    源  表2 :RWA_DEV.ZGS_FINANCING_INFO|产品信息表
    源  表3 :RWA_DEV.ZGS_ATINTRUST_PLAN|资产管理计划表
    --源	 表4 :RWA.RWA_WS_FCII_PLAN|资管计划理财投资补录表 弃用
    --源  表5 :RWA.RWA_WP_DATASUPPLEMENT|补录任务明细信息表 弃用
    目标表  :RWA_DEV.RWA_LC_COLLATERAL|理财系统投资类抵质押品表
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_LC_COLLATERAL';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_LC_COLLATERAL';

    --2.将满足条件的数据从源表插入到目标表中
    INSERT INTO RWA_DEV.RWA_LC_COLLATERAL(
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
    WITH TEMP_INVESTASSETDETAIL AS (
        SELECT  DISTINCT
        				T3.FLD_ASSET_CODE						AS FLD_ASSET_CODE
          FROM RWA_DEV.ZGS_INVESTASSETDETAIL T3
    INNER JOIN RWA_DEV.ZGS_FINANCING_INFO T4
            ON T3.FLD_FINANC_CODE = T4.FLD_FINANC_CODE
           --AND T4.FLD_INCOME_TYPE <> '3'	--3：排除非保本类型  --20190625该条件过滤导致查询结果为0
           AND T4.DATANO = p_data_dt_str
         WHERE T3.FLD_ASSET_TYPE = '24'																			-- 2：债券，24：资产管理计划
           AND T3.FLD_ASSET_STATUS = '1' 																		--1：状态正常
           AND T3.FLD_ASSET_FLAG = '1'   																		--1：理财产品
           AND T3.FLD_DATE  = p_data_dt_str																	--有效的理财产品其估值日期每日更新
           AND T3.DATANO = p_data_dt_str
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                ,p_data_dt_str													     AS DATANO                   --数据流水号
                ,T2.C_PRD_CODE															 AS COLLATERALID           	 --抵质押品ID
                ,'LC'                  											 AS SSYSID              	 	 --源系统ID
                ,T2.C_PRD_CODE															 AS SGUARCONTRACTID        	 --源担保合同ID
                ,T2.C_PRD_CODE															 AS SCOLLATERALID          	 --源抵质押品ID
                ,''		  	                         			 		 AS COLLATERALNAME         	 --抵质押品名称                             依据押品目录映射
                ,'LC' || T2.C_GUARANTOR_PAPERTYPE || T2.C_GUARANTOR_NO
                																             AS ISSUERID             	 	 --发行人ID                                 补录
                ,'LC' || T2.C_COUNTERPARTY_PAPERTYPE || T2.C_COUNTERPARTY_PAPERNO
                						                             	 	 AS PROVIDERID             	 --提供人ID
                ,'01'                                  			 AS CREDITRISKDATATYPE     	 --信用风险数据类型                         默认：一般非零售(01)
                ,T3.ATTRIBUTE1   									 			 		 AS GUARANTEEWAY           	 --担保方式
                ,T2.C_GUARANTEE_SECOND          			 			 AS SOURCECOLTYPE     	     --源抵质押品大类                        		对接老押品目录：默认为质押类型(020)
                ,NVL(T2.C_GUARANTEE_FOURTH,NVL(T2.C_GUARANTEE_THIRD,T2.C_GUARANTEE_SECOND))
                											              				 AS SOURCECOLSUBTYPE         --源抵质押品小类                        		对接老押品目录：若票据种类是银行承兑汇票，则默认为“质押-银行承兑汇票(020210)”，否则默认为“质押-汇票、本票、支票(020220)”
               	,'0'																				 AS SPECPURPBONDFLAG  			 --是否为收购国有银行不良贷款而发行的债券		默认：否(0)
                ,''                              		 				 AS QUALFLAGSTD            	 --权重法合格标识                           RWA计算
                ,''                                 				 AS QUALFLAGFIRB           	 --内评初级法合格标识                       RWA计算
                ,''								 		 									 		 AS COLLATERALTYPESTD 			 --权重法抵质押品类型                    		新信贷上线前：RWA规则映射（权重法-老押品目录）新信贷上线后：RWA规则映射（权重法-新押品目录）
                ,''									 				 								 AS COLLATERALSDVSSTD 		 	 --权重法抵质押品细分                    		新信贷上线前：RWA规则映射（权重法-老押品目录）新信贷上线后：RWA规则映射（权重法-新押品目录）
                ,''			                          					 AS COLLATERALTYPEIRB      	 --内评法抵质押品类型                       RWA规则映射（内评法-新押品目录）
                ,T2.F_GUARANTEE_AMT										       AS COLLATERALAMOUNT     	 	 --抵押总额
                ,NVL(T2.C_GUARANTEE_CURR,'CNY')						   AS CURRENCY               	 --币种
                ,T2.D_VALUE_DATE                             AS StartDate             	 --起始日期
        				,T2.D_END_DATE                               AS DueDate               	 --到期日期
        				,CASE WHEN (TO_DATE(T2.D_END_DATE,'YYYYMMDD') - TO_DATE(T2.D_VALUE_DATE,'YYYYMMDD')) / 365 < 0
        				      THEN 0
        				      ELSE (TO_DATE(T2.D_END_DATE,'YYYYMMDD') - TO_DATE(T2.D_VALUE_DATE,'YYYYMMDD')) / 365
        				 END																				 AS OriginalMaturity      	 --原始期限
        				,CASE WHEN (TO_DATE(T2.D_END_DATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365 < 0
        				      THEN 0
        				      ELSE (TO_DATE(T2.D_END_DATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
        				 END																		     AS ResidualM             	 --剩余期限
                ,'0'                                         AS INTEHAIRCUTSFLAG    	 	 --自行估计折扣系数标识                     默认：否(0)
                ,NULL                                        AS INTERNALHC          	 	 --内部折扣系数                             默认：空
                ,''	                                         AS FCTYPE                 	 --金融质押品类型                           默认：空
                ,'0'                                         AS ABSFLAG             	 	 --资产证券化标识                           默认：否(0)
                ,''                                          AS RATINGDURATIONTYPE  	 	 --评级期限类型                             默认：空
                ,''                                          AS FCISSUERATING     			 --金融质押品发行等级                    		默认：空
                ,CASE WHEN T2.C_GUARANTOR_TYPE = '01' THEN '01'
                 ELSE '02'
                 END                                         AS FCISSUERTYPE             --金融质押品发行人类别                  		默认：其他发行人(02)
                ,CASE WHEN T2.C_GUARANTOR_COUNTRY = 'CHN' THEN '01'
                      ELSE '02'
                 END                                       	 AS FCISSUERSTATE            --金融质押品发行人注册国家              		补录
                ,CASE WHEN (TO_DATE(T2.D_END_DATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365<0
                      THEN 0
                      ELSE (TO_DATE(T2.D_END_DATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
                 END                                         AS FCRESIDUALM              --金融质押品剩余期限                    		默认：空
                ,1                                           AS REVAFREQUENCY            --重估频率                              		默认：1
                ,''                                          AS GROUPID                  --分组编号                              		默认：空
                ,T4.RATINGRESULT														 AS RCERating								 --发行人境外注册地外部评级

    FROM				TEMP_INVESTASSETDETAIL T1																					--交易明细表的最新记录
    INNER JOIN	RWA_DEV.ZGS_ATINTRUST_PLAN T2																			--资管计划表
    ON					T1.FLD_ASSET_CODE = T2.C_PRD_CODE																	--信托编号唯一，故以此字段关联
    --20190625 该2条件过滤导致查询结果为0
    --AND					T2.C_GUARANTEE_FIRST NOT IN ('005','010')													--排除信用(005)、保证(010)
    --AND					T2.C_GUARANTEE_FOURTH NOT IN ('004001004001','004001005001','004001006001','004001006002')     					--信用证、备用信用证、融资性保函、非融资性保函都归为保证
    AND					T2.DATANO = p_data_dt_str
    LEFT JOIN		RWA_DEV.NCM_COLLATERALTYPE_INFO T3
    ON					NVL(T2.C_GUARANTEE_FOURTH,NVL(T2.C_GUARANTEE_THIRD,T2.C_GUARANTEE_SECOND)) = T3.GUARANTYTYPE
    AND					T3.DATANO = p_data_dt_str
    LEFT JOIN		RWA.RWA_WP_COUNTRYRATING T4
    ON					T2.C_GUARANTOR_COUNTRY = T4.COUNTRYCODE
    AND					T4.ISINUSE = '1'
		;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_LC_COLLATERAL',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_LC_COLLATERAL;
    --Dbms_output.Put_line('RWA_DEV.RWA_LC_COLLATERAL表当前插入的理财系统-资管计划投资数据记录为: ' || v_count || ' 条');




    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功' || '-' || v_count;
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '理财投资类抵质押品('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_LC_COLLATERAL;
/

