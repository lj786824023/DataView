CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_LC_GUARANTEE(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_LC_GUARANTEE
    实现功能:理财系统-投资-保证(从数据源理财系统将资管计划投资相关信息全量导入RWA理财投资类接口表保证表中)
    数据口径:全量
    跑批频率:月初
    版  本  :V1.0.0
    编写人  :YUSJ
    编写时间:2015-05-26
    单  位	:上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.ZGS_INVESTASSETDETAIL|资产详情表
    源  表2 :RWA_DEV.ZGS_FINANCING_INFO|产品信息表
    源  表3 :RWA_DEV.ZGS_ATINTRUST_PLAN|资产管理计划表
    --源	 表4 :RWA.RWA_WS_FCII_PLAN|资管计划理财投资补录表 弃用
    --源  表5 :RWA.RWA_WP_DATASUPPLEMENT|补录任务明细信息表 弃用
    目标表	 :RWA_DEV.RWA_LC_GUARANTEE|财务系统投资类保证表
    变更记录(修改人|修改时间|修改内容):
  */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_LC_GUARANTEE';
  --定义异常变量
  v_raise EXCEPTION;
  --定义判断值变量
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_LC_GUARANTEE';

    --2.将满足条件的数据从源表插入到目标表中
    INSERT INTO RWA_DEV.RWA_LC_GUARANTEE(
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
    WITH TEMP_INVESTASSETDETAIL AS (
        SELECT  DISTINCT
        				T3.FLD_ASSET_CODE						AS FLD_ASSET_CODE
          FROM RWA_DEV.ZGS_INVESTASSETDETAIL T3
    INNER JOIN RWA_DEV.ZGS_FINANCING_INFO T4
            ON T3.FLD_FINANC_CODE = T4.FLD_FINANC_CODE
           --AND T4.FLD_INCOME_TYPE <> '3'	--3：排除非保本类型 20190625 该2条件过滤导致查询结果为0
           AND T4.DATANO = p_data_dt_str
         WHERE T3.FLD_ASSET_TYPE = '24'																			-- 2：债券，24：资产管理计划
           AND T3.FLD_ASSET_STATUS = '1' 																		--1：状态正常
           AND T3.FLD_ASSET_FLAG = '1'   																		--1：理财产品
           AND T3.FLD_DATE  = p_data_dt_str																	--有效的理财产品其估值日期每日更新
           AND T3.DATANO = p_data_dt_str
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
         				,T2.C_PRD_CODE																		AS	GUARANTEEID          --保证ID
								,'LC'																						  AS	SSYSID               --源系统ID
								,T2.C_PRD_CODE																		AS	GUARANTEECONID       --保证合同ID
								,'LC' || T2.C_GUARANTOR_PAPERTYPE || T2.C_GUARANTOR_NO
																																	AS	GUARANTORID          --保证人ID
								,'01'																							AS	CREDITRISKDATATYPE   --信用风险数据类型      					默认：一般非零售(01)
								,CASE WHEN T2.C_GUARANTEE_FIRST = '010' THEN '010'
								 ELSE T2.C_GUARANTEE_FOURTH
								 END																						  AS	GUARANTEEWAY       	 --担保方式
								,''																								AS	QUALFLAGSTD          --权重法合格标识
								,''																								AS	QUALFLAGFIRB         --内评初级法合格标识
								,''																								AS	GUARANTEETYPESTD     --权重法保证类型
								,''																								AS	GUARANTORSDVSSTD     --权重法保证人细分
								,''																								AS	GUARANTEETYPEIRB     --内评法保证类型
								,T2.F_GUARANTEE_AMT																AS	GUARANTEEAMOUNT      --保证总额
								,NVL(T2.C_GUARANTEE_CURR,'CNY')										AS	CURRENCY             --币种
                ,T2.D_VALUE_DATE                             			AS StartDate             --起始日期
        				,T2.D_END_DATE                               			AS DueDate               --到期日期
        				,CASE WHEN (TO_DATE(T2.D_END_DATE,'YYYYMMDD') - TO_DATE(T2.D_VALUE_DATE,'YYYYMMDD')) / 365 < 0
        				      THEN 0
        				      ELSE (TO_DATE(T2.D_END_DATE,'YYYYMMDD') - TO_DATE(T2.D_VALUE_DATE,'YYYYMMDD')) / 365
        				 END																				 			AS OriginalMaturity      --原始期限
        				,CASE WHEN (TO_DATE(T2.D_END_DATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365 < 0
        				      THEN 0
        				      ELSE (TO_DATE(T2.D_END_DATE,'YYYYMMDD') - TO_DATE(p_data_dt_str,'YYYYMMDD')) / 365
        				 END																		     			AS ResidualM             --剩余期限
								,T4.PDADJLEVEL																		AS	GUARANTORIRATING     --保证人内部评级
								,T4.PD																						AS	GUARANTORPD          --保证人违约概率
								,''																								AS	GROUPID              --分组编号
    FROM				TEMP_INVESTASSETDETAIL T1																					--交易明细表的最新记录
    INNER JOIN	RWA_DEV.ZGS_ATINTRUST_PLAN T2																			--资管计划表
    ON					T1.FLD_ASSET_CODE = T2.C_PRD_CODE																	--信托编号唯一，故以此字段关联
    --AND					(T2.C_GUARANTEE_FIRST = '010'																			--保证(010)
    --OR					T2.C_GUARANTEE_FOURTH IN ('004001004001','004001005001','004001006001','004001006002'))     					--信用证、备用信用证、融资性保函、非融资性保函都归为保证
    						--20190625 该条件过滤导致查询结果为0
    AND					T2.DATANO = p_data_dt_str
    LEFT JOIN		TMP_CUST_IRATING T4
    ON					REPLACE(T2.C_GUARANTOR_NO,'-','') = REPLACE(T4.ORGCERTCODE,'-','')
		;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_LC_GUARANTEE',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_LC_GUARANTEE;
    --Dbms_output.Put_line('RWA_DEV.RWA_LC_GUARANTEE表当前插入的理财系统-资管计划投资数据记录为:' || v_count || '条');


		--Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功' || '-' || v_count;
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '理财投资类保证('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_LC_GUARANTEE;
/

