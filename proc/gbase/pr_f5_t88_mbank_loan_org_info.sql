DELIMITER |

CREATE DEFINER="gbase"@"%" PROCEDURE "pr_f5_t88_mbank_loan_org_info"(
	OUT	OUT_RES_MSG		VARCHAR(200),
	IN	IN_TX_DATE		VARCHAR(8)
)
lable:BEGIN
/**********************************
 * whd 20210524 新建
 * 本行信贷机构信息
 * zlb 20220104 信贷机构的机构层级按照核心机构层级划分
 *******************************/
	
	DECLARE TX_DATE 				DATE 			DEFAULT CAST(IN_TX_DATE AS DATE);
	DECLARE MAX_DATE				DATE			DEFAULT CAST('9999-12-31' AS DATE);
	DECLARE LAST_TX_DATE			DATE			DEFAULT CAST(IN_TX_DATE AS DATE) - 1;
	DECLARE THIS_MONTH_BEGIN		DATE			DEFAULT CAST(SUBSTR(IN_TX_DATE,1,6)||'01' AS DATE);
	DECLARE LAST_MONTH_END			DATE			DEFAULT CAST(SUBSTR(IN_TX_DATE,1,6)||'01' AS DATE) - 1;
	DECLARE THIS_YEAR_BEGIN			DATE			DEFAULT CAST(SUBSTR(IN_TX_DATE,1,4)||'0101' AS DATE);
	DECLARE LAST_YEAR_END			DATE			DEFAULT CAST(SUBSTR(IN_TX_DATE,1,4)||'0101' AS DATE) - 1;
	
	
	DECLARE ETL_V_STEP              VARCHAR(10)    	DEFAULT '0';
	DECLARE ERR_MSG					VARCHAR(2000);
	DECLARE ERR_CODE				VARCHAR(2000);
	DECLARE PK_COUNT				BIGINT			DEFAULT 0;
	
	/*Exception handling 异常处理*/
	

	/*定义临时表*/
	SET ETL_V_STEP = '1';
	
	CREATE TEMPORARY TABLE VT_t88_mbank_loan_org_info LIKE pdm.t88_mbank_loan_org_info;
	
	/*数据首先插入临时表VT_*/
	SET ETL_V_STEP = '2';
	
insert into VT_t88_mbank_loan_org_info(
	Statt_Dt          , -- 1统计日期
	Crdt_Org_ID       , -- 2信贷机构编号
	Core_Org_ID       , -- 3核心机构编号
	Org_Cate_Cd       , -- 4机构类型代码
	Org_Nm            , -- 5机构名称
	Org_Hrcy_Cd       , -- 6机构层级代码
	Org_Stat_Cd       , -- 7机构状态代码
	Lvl1_Org_ID       , -- 8一级机构编号
	Lvl1_Org_Nm       , -- 9一级机构名称
	Lvl2_Org_ID       , -- 10二级机构编号
	Lvl2_Org_Nm       , -- 11二级机构名称
	Lvl3_Org_ID       , -- 12三级机构编号
	Lvl3_Org_Nm       , -- 13三级机构名称
	Lvl4_Org_ID       , -- 14四级机构编号
	Lvl4_Org_Nm       , -- 15四级机构名称
	Up_Org_ID         , -- 16上级机构编号
	Org_Regn_Cd       , -- 17机构区域代码
	Drct_Map_Ind      , -- 18直接映射标志
	Data_Src_Cd        -- 19数据来源代码
 )
SELECT       
	TX_DATE            	 	, -- 1统计日期  
	T1.Org_Id               , -- 2机构编号
	t1.Core_Org_Id          , -- 3核心机构编号
	T1.Org_Cate_Cd          , -- 4机构类型代码
	T1.Org_Nm               , -- 5机构名称
	T1.Org_Hrcy_Cd          , -- 6机构层级代码
	T1.Org_Stat_Cd          , -- 7机构状态代码
	T2.Lvl1_Org_Id          , -- 8一级机构编号
	T2.Lvl1_Org_Nm          , -- 9一级机构名称
	T2.Lvl2_Org_Id          , -- 10二级机构编号
	T2.Lvl2_Org_Nm          , -- 11二级机构名称
	T2.Lvl3_Org_Id          , -- 12三级机构编号
	T2.Lvl3_Org_Nm          , -- 13三级机构名称
	T2.Lvl4_Org_Id          , -- 14四级机构编号
	T2.Lvl4_Org_Nm          , -- 15四级机构名称
	T1.Up_Org_Id            , -- 16上级机构编号
	''                     , -- 17机构区域代码
	''                    , -- 18直接映射标志
	'NCM'                 -- 19数据来源代码
 FROM PDM.t04_crdt_org_h  T1 -- 信贷机构
LEFT JOIN PDM.t88_mbank_core_org_info	T2 -- 本行核心机构信息
	ON T1.Org_Id=T2.Core_Org_Id -- 关联核心机构号取核心机构层级
	and t2.Start_Dt <= TX_DATE
	and t2.End_Dt > TX_DATE
	AND T2.Data_Src_Cd='NCS'
 where t1.Start_Dt <= TX_DATE  and t1.End_Dt > TX_DATE
;

  
  /*检查插入的临时表数据是否有主键错误*/
  SET ETL_V_STEP = '3';
  
	SELECT COUNT(*) INTO PK_COUNT
	FROM 
	(
		SELECT Crdt_Org_ID FROM VT_t88_mbank_loan_org_info
		GROUP BY 1
		HAVING COUNT(*) > 1
	) A ;
	IF PK_COUNT > 0
	THEN
		SET OUT_RES_MSG = '9999';
		leave lable;
	END IF;
 	
	/* 支持数据重跑*/
	SET ETL_V_STEP = '4';
	
	DELETE FROM pdm.t88_mbank_loan_org_info WHERE Statt_Dt >= TX_DATE; -- Statt_Dt >= TX_DATE 是否要改成=
	
	/*通过主键检查的数据插入正式表中*/
	SET ETL_V_STEP = '5';
	
	INSERT INTO pdm.t88_mbank_loan_org_info
	SELECT * FROM VT_t88_mbank_loan_org_info where Statt_Dt = TX_DATE;
	
	DROP TEMPORARY TABLE VT_t88_mbank_loan_org_info;
	

END |