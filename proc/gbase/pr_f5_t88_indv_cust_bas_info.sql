DELIMITER |

CREATE DEFINER="cq_sjpt"@"%" PROCEDURE "pr_f5_t88_indv_cust_bas_info"(
	OUT	OUT_RES_MSG		VARCHAR(200),
	IN	IN_TX_DATE		VARCHAR(8)
	)
LABLE:BEGIN
/**********************************
 * 个人客户信息
 *********************************/
	
	DECLARE ETL_T_TAB_ENG_NAME 		VARCHAR(100)	DEFAULT 't88_indv_cust_bas_info';
	DECLARE ETL_STEP_NO				INTEGER			DEFAULT 1;
	DECLARE ETL_TX_DATE 			VARCHAR(8)		DEFAULT IN_TX_DATE;
	SET OUT_RES_MSG = 'FAILED';
	
	
	/*支持数据重跑*/
	SET @SQL_STR = 'DELETE FROM PDM.'||ETL_T_TAB_ENG_NAME||' WHERE STATT_DT >= ${TX_DATE}';
	CALL ETL.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	/*定义临时表*/
	SET @SQL_STR = 'DROP TEMPORARY TABLE IF EXISTS PDM.VT_'||ETL_T_TAB_ENG_NAME;
	CALL ETL.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	/*创建临时表*/
	SET @SQL_STR = 'CREATE  TABLE PDM.VT_'||ETL_T_TAB_ENG_NAME||' LIKE PDM.'||ETL_T_TAB_ENG_NAME;
	CALL ETL.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	/*数据首先插入临时表VT_*/

	--  ${NULL_STR}
	SET @SQL_STR = '
	insert into PDM.VT_t88_indv_cust_bas_info(
		Statt_Dt                  ,-- 1统计日期    
		Cust_ID                   ,-- 2客户编号
		Core_Cust_ID              ,-- 3核心客户编号
		Loan_Cust_ID              ,-- 4贷款客户编号
		Cust_Nm                   ,-- 5客户名称
		ID_Card_Num               ,-- 6身份证号
		Cert_Matr_Dt              ,-- 7证件到期日期
		OpenAcct_Org_ID           ,-- 8开户机构编号
		Belg_Org_ID               ,-- 9归属机构编号
		Gender_Cd                 ,-- 10性别代码
		Birth_Dt                  ,-- 11出生日期
		Birth_Urbn_Cd             ,-- 12出生城市代码
		Nation_Cd                 ,-- 13国籍代码
		Ethnic_Cd                 ,-- 14民族代码
		Natv_Plc                  ,-- 15籍贯
		Housd_Rgst_Belg_Urbn      ,-- 16户籍所在城市
		Max_EduDegr_Cd            ,-- 17最高学历代码
		Carr_Cd                   ,-- 18职业代码
		Marrg_Stat_Cd             ,-- 19婚姻状态代码
		Hlth_Situ_Cd              ,-- 20健康状况代码
		Work_Corp                 ,-- 21工作单位
		Corp_Tel                  ,-- 22单位电话
		Family_Dtl_Addr           ,-- 23家庭详细地址
		Zip_Cd                    ,-- 24邮政编码
		Fix_Tel                   ,-- 25固定电话
		Admin_Div_Cd              ,-- 26行政区划代码
		Mth_Incom_Cd              ,-- 27月收入代码
		Cust_Lvl_Cd               ,-- 28客户等级代码
		Cust_Cls_Cd               ,-- 29客户分类代码
		Cust_Stat_Cd              ,-- 30客户状态代码
		Inds_Cate_Cd              ,-- 31行业类型代码
		Farm_Cate_Cd              ,-- 32农户类型代码
		Ext_Crdt_Grade              ,-- 33外部信用评级
		Inn_Crdt_Grade              ,-- 34内部信用评级
		Hav_Chld_Ind              ,-- 35有无子女标志
		Mbank_Emp_Ind             ,-- 36本行员工标志
		Prtc_Endw_Insu_Ind        ,-- 37参加养老保险标志
		Prtc_Mdcl_Insu_Ind        ,-- 38参加医疗保险标志
		Prtc_Unmp_Insu_Ind        ,-- 39参加失业保险标志
		Prtc_Occ_Injry_Insu_Ind   ,-- 40参加工伤保险标志
		Prtc_Maternity_Insu_Ind   ,-- 41参加生育保险标志
		Mbank_Shrhd_Ind           ,-- 42本行股东标志
		VIP_Cust_Ind              ,-- 43VIP客户标志
		DisFit_9Elmnt_Cust_Ind    ,-- 44不满足9要素客户标志
		Domandfore_Ind            ,-- 45境内外标志
		Farm_Ind                  ,-- 46农户标志
		Poverty_Acct_Ind          ,-- 47贫困户标志
		Blk_List_Cust_Ind         -- 48黑名单客户标志
	)
	select 
		${TX_DATE}                       ,-- 1统计日期
		T1.Cust_Id                   ,-- 2客户编号
		\'\',-- case when T2.Src_Sys_Id=\'NCS\' then T2.Src_Sys_Cust_Id  end  ,-- 3核心客户编号 源系统客户编号
		\'\',-- case when T2.Src_Sys_Id=\'NCM\' then T2.Src_Sys_Cust_Id  end  ,-- 4贷款客户编号 源系统客户编号
		T1.Cust_Sht_Nm               ,-- 5客户简称
		nvl(T3.Cert_Num,${NULL_STR}) ,-- 6证件号码   身份证
		nvl(T3.Cert_Efft_Dt,\'0001-01-01\') ,-- 7证件生效日期
		nvl(T4.Openacct_Org_Id,${NULL_STR})           ,-- 8开户机构编号
		${NULL_STR}                           ,-- 9归属机构编号  基础层模型增加
		T1.Gender_Cd                 ,-- 10性别
		T1.Birth_Dt                  ,-- 11出生日期
		T1.Birth_Admin_Div_Cd        ,-- 12出生城市
		T1.Nation_Zone_Cd            ,-- 13国籍
		T1.Ethnic_Cd                 ,-- 14民族
		T1.Natv_Plc                  ,-- 15籍贯
		T1.Housd_Rgst_Admin_Div_Cd   ,-- 16户籍所在城市
		T1.Max_Edudegr_Cd            ,-- 17最高学历
		T1.Carr_Cate_Cd              ,-- 18职业类型代码
		T1.Marrg_Situ_Cd             ,-- 19婚姻状况
		T1.Hlth_Situ_Cd              ,-- 20健康状况
		nvl(T5.Corp_Nm,${NULL_STR})                   ,-- 21单位名称
		nvl(T5.Corp_Tel,${NULL_STR})                  ,-- 22单位电话
		nvl(T6.Dtl_Addr,${NULL_STR})  				  ,-- 23详细地址
		nvl(T6.Zip_Cd,${NULL_STR})                    ,-- 24邮政编码
		nvl(T7.Fix_Tel,${NULL_STR})                   ,-- 25固定电话
		nvl(T6.Admin_Div_Cd,${NULL_STR})              ,-- 26行政区划代码
		T1.Mth_Incom_Cd              ,-- 27月收入代码
		nvl(T8.Cust_Lvl_Cd,${NULL_STR})  ,-- 28客户等级代码
		T1.Indv_Cust_Cate_Cd         ,-- 29个人客户类型代码
		nvl(T4.Cust_Stat_Cd,${NULL_STR})              ,-- 30客户状态代码
		T1.Curr_Inds_Cd              ,-- 31从事行业代码
		T1.Farm_Cate_Cd              ,-- 32农户类型代码
		nvl(T9_A.Grade_Rest_Cd,${NULL_STR})    ,-- 33外部信用评级
		nvl(T9_B.Grade_Rest_Cd,${NULL_STR}) 	,-- 34内部信用评级
		nvl(T7.Hav_Chld_Ind,${NULL_STR})      ,-- 35有无子女标志
		nvl(t10.bhyg,${NULL_STR})  ,-- 	36本行员工标志
		nvl(t10.ylaobx,${NULL_STR})	,-- 	37参加养老保险标志
		nvl(t10.ylbx,${NULL_STR}) ,-- 	38参加医疗保险标志
		nvl(t10.sybx,${NULL_STR}),-- 	39参加失业保险标志
		nvl(t10.gsbx,${NULL_STR}) ,-- 	40参加工伤保险标志
		nvl(t10.syubx,${NULL_STR}) ,-- 	41参加生育保险标志
		nvl(t10.bhgd,${NULL_STR}) ,-- 	42本行股东标志
		nvl(t10.vip,${NULL_STR})  ,-- 	43VIP客户标志
		nvl(t10.bm9ys,${NULL_STR})	,-- 	44不满足9要素客户标志
		nvl(t10.jnwbz,${NULL_STR})	,-- 	45境内外标志
		nvl(t10.nhbz,${NULL_STR})	,-- 	46农户标志
		nvl(t10.pkhbz,${NULL_STR})	,-- 	47贫困户标志
		nvl(t10.hmdbz,${NULL_STR})	-- 	48黑名单客户标志
	FROM PDM.t01_indv_cust_h  T1  -- 个人客户
	/*LEFT JOIN PDM.t01_cust_map_info_h T2 -- 客户映射信息表
		ON T1.Cust_Id=T2.Cust_Id 
		and t2.Start_Dt <= ${TX_DATE}  and t2.End_Dt >= ${TX_DATE}*/
	LEFT JOIN PDM.t01_cust_cert_info T3 -- 客户证件信息
		ON T1.Cust_Id=T3.Cust_Id 
		and t3.Statt_Dt = ${TX_DATE}
		AND T3.Cust_Cate_Cd=\'2\'  -- 个人客户
		and T3.Cert_Cate_Cd=\'110112\' -- 身份证
		and t3.Cert_Fst_Choic_Ind = \'1\' -- 证件首选标志
	LEFT JOIN PDM.t01_cust_h T4 -- 客户
		ON T1.Cust_Id=T4.Cust_Id
		and t4.Start_Dt <= ${TX_DATE}  and t4.End_Dt >= ${TX_DATE}
		AND T4.Cust_Cate_Cd=\'2\'  -- 个人客户
	LEFT JOIN (select Cust_Id,Corp_Nm,Corp_Tel,
		   row_number() over(partition by cust_id order by last_update_tm desc ) rm 
		from  PDM.t01_indv_cust_work_situ  -- 个人客户工作情况
		 where statt_Dt = 20210911
		 and Valid_Ind= \'1\' -- 1取有效数据
		) t5 
		ON T1.Cust_Id=T5.Cust_Id
		and t5.rm=1
	LEFT JOIN PDM.t01_cust_addr_info_h	T6 -- 客户地址信息
		ON T1.Cust_Id=T6.Cust_Id
		and T6.Addr_Cate_Cd=\'101\' -- 家庭地址
		and t6.Start_Dt <= ${TX_DATE}  and t6.End_Dt >= ${TX_DATE}
	LEFT JOIN PDM.t01_indv_cust_family_bas_situ	T7 -- 个人客户家庭基本状况  --ods无数据
		ON T1.Cust_Id=T7.Cust_Id
		and t7.Statt_Dt = ${TX_DATE}
	LEFT JOIN PDM.t01_cust_lvl_h T8 -- 客户等级信息
		ON T1.Cust_Id=T8.Cust_Id 
	    and T8.Lvl_Cate_Cd=\'10\'  -- 对私客户等级
		and t8.Start_Dt <= ${TX_DATE}  and t8.End_Dt >= ${TX_DATE}
	LEFT JOIN PDM.t01_cust_grade_h T9_A -- 客户评级信息  
		ON T1.Cust_Id=T9_A.Cust_Id 
		and T9_A.Start_Dt <= ${TX_DATE}  and T9_A.End_Dt >= ${TX_DATE}
		and T9_A.Grade_Cate_Cd=\'11\' -- 外部信用评级
	LEFT JOIN PDM.t01_cust_grade_h T9_B -- 客户评级信息  
		ON T1.Cust_Id=T9_B.Cust_Id 
		and T9_B.Start_Dt <= ${TX_DATE}  and T9_B.End_Dt >= ${TX_DATE}
		and T9_B.Grade_Cate_Cd=\'12\' -- 内部信用评级
	LEFT JOIN (select
				t.Cust_ID,
				max(case when t.Impt_Ind_Cate_Cd = \'02005\' and Impt_Ind = 1 then 1 else 0 end ) as bhyg, -- 36本行员工标志
				max(case when t.Impt_Ind_Cate_Cd = \'02018\' and Impt_Ind = 1 then 1 else 0 end ) as ylaobx, -- 37参加养老保险标志
				max(case when T.Impt_Ind_Cate_Cd = \'02019\' and Impt_Ind = 1 then 1 else 0 end ) as ylbx, -- 38参加医疗保险标志
				max(case when T.Impt_Ind_Cate_Cd = \'02020\' and Impt_Ind = 1 then 1 else 0 end ) as sybx, -- 39参加失业保险标志
				max(case when T.Impt_Ind_Cate_Cd = \'02021\' and Impt_Ind = 1 then 1 else 0 end ) as gsbx, -- 40参加工伤保险标志
				max(case when T.Impt_Ind_Cate_Cd = \'02022\' and Impt_Ind = 1 then 1 else 0 end ) as syubx, -- 41参加生育保险标志
				max(case when T.Impt_Ind_Cate_Cd = \'02004\' and Impt_Ind = 1 then 1 else 0 end ) as bhgd, -- 42本行股东标志
				max(case when T.Impt_Ind_Cate_Cd = \'01035\' and Impt_Ind = 1 then 1 else 0 end ) as vip, -- 43VIP客户标志
				max(case when T.Impt_Ind_Cate_Cd = \'00003\' and Impt_Ind = 1 then 1 else 0 end ) as bm9ys,  -- 44不满足9要素客户标志
				max(case when T.Impt_Ind_Cate_Cd = \'00002\' and Impt_Ind = 1 then 1 else 0 end ) as jnwbz, -- 45境内外标志
				max(case when T.Impt_Ind_Cate_Cd = \'02002\' and Impt_Ind = 1 then 1 else 0 end ) as nhbz, -- 46农户标志
				max(case when T.Impt_Ind_Cate_Cd = \'02014\' and Impt_Ind = 1 then 1 else 0 end ) as pkhbz, -- 47贫困户标志
				max(case when T.Impt_Ind_Cate_Cd = \'01032\' and Impt_Ind = 1 then 1 else 0 end ) as hmdbz -- 48黑名单客户标志
			from PDM.t01_cust_impt_ind_h t 
			where T.Cust_Cate_Cd IN (\'2\') -- 个人客户
				and t.Start_Dt <= ${TX_DATE}  and t.End_Dt >= ${TX_DATE}
			group by Cust_ID) T10 -- 客户重要标志
		ON T1.Cust_Id=T10.Cust_Id 
	where t1.Start_Dt <= ${TX_DATE}  and t1.End_Dt >= ${TX_DATE}
	';
	CALL ETL.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	
	/*检查插入的临时表数据是否有主键错误*/
	
	-- 获取主键字段
	SELECT PHYSICAL_PRI_KEY INTO @PK_COLUMN FROM DATAMAPPING_TASK WHERE T_TAB_ENG_NAME=ETL_T_TAB_ENG_NAME;
	-- 0 正常
	SET @SQL_STR = 'SELECT COUNT(1) INTO @PK_COUNT FROM(SELECT 1 FROM PDM.VT_'||ETL_T_TAB_ENG_NAME||' GROUP BY '||@PK_COLUMN||' HAVING COUNT(1)>1) T';
	CALL ETL.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	
	IF @PK_COUNT <> 0
	THEN
		SET OUT_RES_MSG = '9999';
		UPDATE ETL.ETL_JOB_STATUS_EDW SET STEP_STATUS='Failed',STEP_ERR_LOG='主键重复', LAST_END_TIME=CURRENT_TIMESTAMP WHERE SQL_UNIT=ETL_T_TAB_ENG_NAME AND TX_DATE=TX_DATE AND STEP_NO=ETL_STEP_NO-1;
		LEAVE LABLE;
	END IF;
	
	/*通过主键检查的数据插入正式表中*/
	SET @SQL_STR='INSERT INTO PDM.'||ETL_T_TAB_ENG_NAME||' SELECT * FROM PDM.VT_'||ETL_T_TAB_ENG_NAME||' WHERE STATT_DT=${TX_DATE}'; 
	CALL ETL.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;

	SET OUT_RES_MSG='SUCCESSFUL';
	
END |