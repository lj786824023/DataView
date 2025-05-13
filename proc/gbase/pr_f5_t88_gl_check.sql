DELIMITER |

CREATE DEFINER="cqbank_sj"@"%" PROCEDURE "pr_f5_t88_gl_check"(
	OUT	OUT_RES_MSG		VARCHAR(200),
	IN	IN_TX_DATE		VARCHAR(8)
	)
lable:BEGIN
/**********************************
 * 总分校验
 * 20210922 新建
 *******************************/
	
	DECLARE ETL_T_TAB_ENG_NAME 		VARCHAR(50)		DEFAULT 't88_gl_check';
	DECLARE ETL_STEP_NO				INTEGER			DEFAULT 1;
	DECLARE ETL_TX_DATE 			VARCHAR(8)		DEFAULT IN_TX_DATE;
	DECLARE PK_COUNT				BIGINT			DEFAULT 0;
	DECLARE PK_ERR_CNT				BIGINT			DEFAULT 0;
	DECLARE RET_CODE				INTEGER			DEFAULT 0;
/*
 *  BRD_BILL_REPO 11110206买入返售金融资产-质押式买入返售电子银行承兑汇票 现在从贷款出的每天也平
	BRD_BILL      13010103贴现资产-电子银行承兑汇票贴现本金 / 13010303贴现资产-电子银行承兑汇票转贴现本金 / 13010511贴现资产-内转电子银行承兑汇票本金 /
			       13010901贴现资产-一级市场福费廷  现在从贷款出的每天也平

  */
		
	/* 支持数据重跑*/
	SET @SQL_STR = 'DELETE FROM ${AUTO_PDM}.t88_gl_check WHERE Statt_Dt = ${TX_DATE}';

	CALL etl.pr_exec_sql(@RTC, '', ETL_T_TAB_ENG_NAME, ETL_STEP_NO, @SQL_STR, ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;
	

	SET @SQL_STR ='INSERT INTO pdm.t88_gl_check
	SELECT 
	${TX_DATE},
	S1.SUBJ_ID
	,S1.SUBJ_NM
	,S1.ORG_ID
	,org.Org_Nm AS ORG_NAME
	,S1.CUR_CD
	,S1.CURR_BAL
	,S2.SUBJ_ID
	,S2.CUR_CD
	,S2.CURR_BAL
	,S1.CURR_BAL - S2.CURR_BAL AS CA
	,S2.DATA_SRC
	FROM (
	  SELECT
		CASE WHEN SUBSTRING(SUBJ_ID,1,4)=\'7001\' THEN \'7001\'
		     WHEN SUBSTRING(SUBJ_ID,1,4)=\'7002\' THEN \'7002\'
		     WHEN SUBSTRING(SUBJ_ID,1,4)=\'7018\' THEN \'7018\'
		     ELSE SUBJ_ID
		END SUBJ_ID,
		ORG_ID,
		CASE WHEN SUBSTRING(SUBJ_ID,1,4)=\'7001\' THEN \'开出信用证\'
		     WHEN SUBSTRING(SUBJ_ID,1,4)=\'7002\' THEN \'承兑汇票\'
		     WHEN SUBSTRING(SUBJ_ID,1,4)=\'7018\' THEN \'开出保函\'
		     ELSE SUBJ_NM
		END SUBJ_NM
		,CUR_CD
		,ABS(SUM(CURR_BAL)) AS CURR_BAL
		FROM PDM.T88_SUBJ_SUM_DTL
		WHERE STATT_DT = ${TX_DATE}
		AND SUBJ_ID IN 
		(\'2010\',\'70010100\',\'70010200\',\'70010300\',\'70020001\',\'70020002\',\'70180100\',\'70180200\',\'7119\',\'10010100\',\'10010200\',\'10030101\',\'10030102\'
	,\'10030110\',\'10110101\',\'10110102\',\'10110103\',\'10110104\',\'10110108\',\'10110109\',\'10110113\'
	,\'10110115\',\'10110116\',\'10110203\',\'10110301\',\'10110701\',\'10110702\',\'10110801\',\'10110802\'
	,\'10110803\',\'10110809\',\'11010101\',\'11010102\',\'11010103\',\'11010104\',\'11010301\',\'11010303\'
	,\'11012001\',\'11012002\',\'11020101\',\'11020201\',\'11020202\',\'11020301\',\'11110206\',\'11110208\'
	,\'11110301\',\'12220101\',\'12220102\',\'12220103\',\'12220201\',\'12220301\',\'12220600\',\'13010101\'
	,\'13010102\',\'13010103\',\'13010104\',\'13010203\',\'13010204\',\'13010303\',\'13010304\',\'13010511\'
	,\'13010512\',\'13010901\',\'13010902\',\'13010903\',\'13010904\',\'13011001\',\'13020101\',\'13020103\'
	,\'13020108\',\'13020301\',\'13030101\',\'13030103\',\'13030104\',\'13030106\',\'13030108\',\'13030201\'
	,\'13030203\',\'13030204\',\'13030206\',\'13030207\',\'13030301\',\'13030303\',\'13030306\',\'13030501\'
	,\'13030601\',\'13030603\',\'13030606\',\'13030701\',\'13030801\',\'13031103\',\'13031107\',\'13031110\'
	,\'13031201\',\'13031301\',\'13031401\',\'13031402\',\'13040101\',\'13040102\',\'13040103\',\'13050100\'
	,\'13050200\',\'13050501\',\'13050502\',\'13070100\',\'13070401\',\'13070402\',\'13070403\',\'13070404\'
	,\'13070500\',\'13100000\',\'13100001\',\'13100101\',\'13100102\',\'13100103\',\'13100104\',\'13210101\'
	,\'13210102\',\'13210110\',\'13212002\',\'13212003\',\'15010101\',\'15010102\',\'15012001\',\'15020100\'
	,\'15030101\',\'15030102\',\'15030103\',\'15030104\',\'15030201\',\'15030204\',\'15032001\',\'15050101\'
	,\'15050104\',\'15050205\',\'15050206\',\'15050302\',\'15050304\',\'15050306\',\'15050309\',\'15050311\'
	,\'15050401\',\'15110100\',\'15110200\',\'15120000\',\'15210100\',\'20020101\',\'20020102\',\'20020103\'
	,\'20020104\',\'20020106\',\'20020107\',\'20020108\',\'20020110\',\'20020120\',\'20020200\',\'20020202\'
	,\'20020220\',\'20020300\',\'20020500\',\'20020600\',\'20030101\',\'20030102\',\'20040000\',\'20090101\'
	,\'20090201\',\'20101000\',\'20110101\',\'20110111\',\'20110113\',\'20110114\',\'20110201\',\'20110202\'
	,\'20110204\',\'20110205\',\'20110206\',\'20110207\',\'20110208\',\'20110211\',\'20110212\',\'20110301\'
	,\'20110400\',\'20110501\',\'20110502\',\'20110504\',\'20110505\',\'20110510\',\'20110601\',\'20110603\'
	,\'20110604\',\'20110606\',\'20110607\',\'20110608\',\'20110700\',\'20110801\',\'20110803\',\'20120101\'
	,\'20120107\',\'20120110\',\'20120201\',\'20120202\',\'20120203\',\'20120206\',\'20120207\',\'20120209\'
	,\'20120210\',\'20120211\',\'20120213\',\'20120214\',\'20120218\',\'20120219\',\'20120302\',\'20130103\'
	,\'20140000\',\'20150100\',\'20150200\',\'20210403\',\'20210404\',\'21110207\',\'21110208\',\'21110300\'
	,\'22040100\',\'22040200\',\'22310101\',\'22310102\',\'22310104\',\'22310201\',\'22310202\',\'22310403\'
	,\'22310800\',\'22310900\',\'22311000\',\'22311100\',\'22311200\',\'22311300\',\'22311401\',\'22311402\'
	,\'22311501\',\'22311502\',\'22312000\',\'23120200\',\'23140101\',\'23140102\',\'23140110\',\'23142001\'
	,\'23142003\',\'23142004\',\'23142005\',\'23142006\',\'23142007\',\'23142010\',\'25020100\',\'25020101\'
	,\'25020102\',\'25020103\',\'25020104\',\'25020300\',\'25020301\',\'25020302\',\'25020303\',\'25020304\'
	,\'22410101\',\'10110808\',\'20110802\')
 GROUP BY CASE WHEN SUBSTRING(SUBJ_ID,1,4)=\'7001\' THEN \'7001\'
		     WHEN SUBSTRING(SUBJ_ID,1,4)=\'7002\' THEN \'7002\'
		     WHEN SUBSTRING(SUBJ_ID,1,4)=\'7018\' THEN \'7018\'
		     ELSE SUBJ_ID
		END, 
		   CASE WHEN SUBSTRING(SUBJ_ID,1,4)=\'7001\' THEN \'开出信用证\'
		     WHEN SUBSTRING(SUBJ_ID,1,4)=\'7002\' THEN \'承兑汇票\'
		     WHEN SUBSTRING(SUBJ_ID,1,4)=\'7018\' THEN \'开出保函\'
		     ELSE SUBJ_NM
		END,
    CUR_CD,
	ORG_ID  -- 机构
	
	UNION ALL
  SELECT
    \'XYK\' SUBJ_ID,
    \'9997\' ORG_ID,
    \'信用卡汇总【130704&131001】\' SUBJ_NM,
    \'CNY\'CUR_CD,
    ABS(SUM(CURR_BAL)) AS CURR_BAL
  FROM PDM.T88_SUBJ_SUM_DTL
    WHERE STATT_DT = ${TX_DATE} 
	AND SUBJ_ID IN 
    (\'13070401\',\'13070402\',\'13070403\',\'13070404\'
     ,\'13100101\',\'13100102\',\'13100103\',\'13100104\')
	) S1
	LEFT JOIN (
	  SELECT
		SUBJ_ID
		,org_id
 		,CUR_CD
		,SUM(CURR_BAL) AS CURR_BAL
		,GROUP_CONCAT(DATA_SRC) AS DATA_SRC
		FROM (
		-- 存款
		select Subj_ID,org_id,CUR_CD,SUM(CURR_BAL)  AS CURR_BAL,DATA_SRC FROM (
		SELECT 
		CASE
			WHEN SUBJ_ID =\'2010\' 
			THEN \'20201000\' 
			ELSE SUBJ_ID 
		END AS SUBJ_ID
		,ACCT_ORG_ID as org_id
 		,CUR_CD
		,ABS(SUM(CURR_BAL)) AS CURR_BAL
		,\'存款\' AS DATA_SRC
		from
		PDM.T88_DPST_ACCT_BAS_INFO
		WHERE STATT_DT = ${TX_DATE}
		  and substring(SUBJ_ID,1,6) <> \'201102\'
		  and substring(SUBJ_ID,1,4) <> \'2014\'
          and (substring(SUBJ_ID,1,2) not in( \'22\',\'23\') or SUBJ_ID=\'22410101\')
    GROUP BY SUBJ_ID,CUR_CD,ACCT_ORG_ID -- 机构   
		-- ORDER BY SUBJ_ID,CUR_CD
		 UNION ALL 
		 SELECT 
		 Subj_ID,
     TX_Org as org_id,
 		 Cur_Cd,
		 SUM(Bal) AS CURR_BAL,
		\'存款\' AS DATA_SRC
		FROM pdm.t03_manl_tran_doss_acct
		WHERE Statt_Dt = ${TX_DATE}
		GROUP BY   Cur_Cd, Subj_ID,TX_Org
    )CK GROUP BY Subj_ID,CUR_CD,DATA_SRC,org_id  -- 机构
		UNION ALL
		-- 内部帐
		SELECT 
		SUBJ_ID
    ,case when   Cust_Id=\'2059945616\' and ACCT_ORG_ID=\'9998\'  
			then  \'3501\'
		  when   Cust_Id=\'2054705177\' and ACCT_ORG_ID=\'9998\'
		  then  \'6501\'
		  else ACCT_ORG_ID  
		  end as org_id
 		,CUR_CD
		,ABS(SUM(CURR_BAL)) AS CURR_BAL
		,\'内部账\' AS DATA_SRC
		FROM PDM.T88_INN_ACCT_INFO
		WHERE STATT_DT = ${TX_DATE}
		AND SUBJ_ID <> \'\'
 		and SUBJ_ID not in( \'20110510\',\'22410101\')
    GROUP BY SUBJ_ID,CUR_CD,case when   Cust_Id=\'2059945616\' and ACCT_ORG_ID=\'9998\'  
			then   \'3501\'
		  when   Cust_Id=\'2054705177\' and ACCT_ORG_ID=\'9998\'
		  then  \'6501\'
		  else ACCT_ORG_ID  
		  end  -- 机构
		-- ORDER BY SUBJ_ID,CUR_CD
		UNION ALL
		-- 贷款
		SELECT 
		DTL_SUBJ_ID AS SUBJ_ID
		,case when SUBSTR(A.Agmt_ID,1,4)=''RMPS''
			  then A.Acct_Org_ID
			  when a.Prod_ID=''NCM11103030'' -- 微粒贷
			  -- then C.COREID
			  then a.MGMT_ORG_ID
        else  B.Org_Id
    end as org_id
 		,CUR_CD
		,ABS(SUM(DUBIL_BAL)) AS CURR_BAL
		,\'贷款\' AS DATA_SRC
    FROM PDM.T88_LOAN_DUBIL A
    LEFT JOIN (select distinct  Dubil_Agmt_Id  AS Agmt_ID,Org_Id  from pdm.t03_loan_acct_h where end_dt>=${TX_DATE} ) B
    ON A.Agmt_ID=B.Agmt_ID
    -- LEFT JOIN (  
    -- select  \'NCM\'||SERIALNO as Agmt_ID, D.COREID   from  ods.ncm_business_duebill t 
    -- LEFT JOIN (SELECT DISTINCT T2.ORGID, T1.COREID
    --          FROM ODS.NCM_ORG_INFO T1
    --         INNER JOIN ODS.NCM_ORG_INFO T2
    --          ON T1.ORGID = T2.BELONGORGID
    --           AND T1.SDATE = T2.SDATE
    --         WHERE CAST (T1.SDATE AS DATE) =${TX_DATE}
    --      ) D  --  微粒贷机构获取（by hl）
    -- ON T.INPUTORGID=D.ORGID
    -- where T.BUSINESSTYPE=''11103030'' AND CAST (T.SDATE AS DATE)=${TX_DATE} 
    --    ) C
    -- ON A.Agmt_ID=C.Agmt_ID
    -- AND a.Prod_ID=''NCM11103030''
		WHERE STATT_DT = ${TX_DATE}
    GROUP BY DTL_SUBJ_ID,CUR_CD,case when SUBSTR(A.Agmt_ID,1,4)=''RMPS''
        then    A.Acct_Org_ID
        when a.Prod_ID=''NCM11103030'' -- 微粒贷
        -- then C.COREID
        then a.MGMT_ORG_ID
        else  B.Org_Id
    end   -- 机构
		UNION ALL
		-- 债券回购
		SELECT  
		PRIN_SUBJ_ID AS SUBJ_ID
    ,''9998'' as org_id  --  fds的都是9998 
    -- ,TRUST_ORG_ID as org_id  --  非核心机构
 		,CUR_CD
		,ABS(SUM(BUY_BACK_PRIN)) AS  CURR_BAL
		,\'债券回购\' AS DATA_SRC
		FROM PDM.T88_CAP_BOND_BUY_BACK_BIZ_INFO
		WHERE STATT_DT = ${TX_DATE}
		GROUP BY PRIN_SUBJ_ID,CUR_CD 
		UNION ALL
		-- 债券投资
		SELECT 
			A.PRIN_SUBJ_ID AS SUBJ_ID -- 本金科目
    ,COALESCE(t3.COREORGID,''6001'') as Org_Id   
    ,A.CUR_CD
    ,ABS(SUM(A.BOOK_BAL)) AS CURR_BAL
			,\'债券投资\' AS DATA_SRC
    FROM PDM.T88_CAP_BOND_INVST_BIZ_INFO A
    LEFT JOIN PDM.T05_BOND_INVST_EVT T2
        ON a.Agmt_Id = T2.Bond_Agmt_Id
		AND t2.statt_dt = ${TX_DATE}
    LEFT JOIN ods.FDS_SYS_ORGAN T3
        ON T2.Core_Org_Id = T3.ORGID
		AND CAST (T3.SDATE AS DATE) = ${TX_DATE}
    WHERE A.STATT_DT = ${TX_DATE}
    GROUP BY A.PRIN_SUBJ_ID,A.CUR_CD,COALESCE(t3.COREORGID,''6001'')
		UNION ALL
		SELECT 
			a.Recvbl_Int_Subj_Id AS SUBJ_ID -- 应收利息科目
			,COALESCE(t3.COREORGID,''6001'') as Org_Id
			,a.CUR_CD
			,SUM(a.Recvbl_Int_Bal) AS CURR_BAL
			,\'债券投资\' AS DATA_SRC
		FROM PDM.T88_CAP_BOND_INVST_BIZ_INFO A
    LEFT JOIN PDM.T05_BOND_INVST_EVT T2
        ON a.Agmt_Id = T2.Bond_Agmt_Id
		AND t2.statt_dt = ${TX_DATE}
    LEFT JOIN ods.FDS_SYS_ORGAN T3
        ON T2.Core_Org_Id = T3.ORGID
		AND CAST (T3.SDATE AS DATE) = ${TX_DATE}
    WHERE A.STATT_DT = ${TX_DATE}
    GROUP BY A.Recvbl_Int_Subj_Id,A.CUR_CD,COALESCE(t3.COREORGID,''6001'')
		UNION  ALL
		SELECT 
			a.FVTPL_Subj AS SUBJ_ID -- 公允价值变动科目
			,COALESCE(t3.COREORGID,''6001'') as Org_Id
			,CUR_CD
			,abs(SUM(a.FVTPL_Amt)) AS CURR_BAL
			,\'债券投资\' AS DATA_SRC
		FROM PDM.T88_CAP_BOND_INVST_BIZ_INFO A
    LEFT JOIN PDM.T05_BOND_INVST_EVT T2
        ON a.Agmt_Id = T2.Bond_Agmt_Id
		AND t2.statt_dt = ${TX_DATE}
    LEFT JOIN ods.FDS_SYS_ORGAN T3
        ON T2.Core_Org_Id = T3.ORGID
		AND CAST (T3.SDATE AS DATE) = ${TX_DATE}
    WHERE A.STATT_DT = ${TX_DATE}
    GROUP BY A.FVTPL_Subj,A.CUR_CD,COALESCE(t3.COREORGID,''6001'')
		UNION  ALL
		SELECT 
			a.Int_Adj_Subj_ID AS SUBJ_ID -- 利息调整科目
			,COALESCE(t3.COREORGID,''6001'') as Org_Id
			,CUR_CD
			,abs(SUM(a.Int_Adj_Amt)) AS CURR_BAL
			,\'债券投资\' AS DATA_SRC
		FROM PDM.T88_CAP_BOND_INVST_BIZ_INFO A
    LEFT JOIN PDM.T05_BOND_INVST_EVT T2
        ON a.Agmt_Id = T2.Bond_Agmt_Id
		AND t2.statt_dt = ${TX_DATE}
    LEFT JOIN ods.FDS_SYS_ORGAN T3
        ON T2.Core_Org_Id = T3.ORGID
		AND CAST (T3.SDATE AS DATE) = ${TX_DATE}
    WHERE A.STATT_DT = ${TX_DATE}
    GROUP BY A.Int_Adj_Subj_ID,A.CUR_CD,COALESCE(t3.COREORGID,''6001'')
		UNION ALL
		-- 同业资金
		SELECT 
		A.PRIN_SUBJ_ID AS SUBJ_ID
		,B.Org_Id
 		,A.CUR_CD
		,ABS(SUM(A.CURR_BAL)) AS CURR_BAL 
		,\'同业资金\' AS DATA_SRC
		FROM PDM.T88_IBANK_CAP_BIZ_INFO A
    LEFT JOIN PDM.t03_cap_realtm_bal   B
		ON A.Agmt_ID = B.Agmt_ID
		AND B.STATT_DT = ${TX_DATE}
		AND B.Amt_Cate=\'ZJJE0001\'
		AND B.Data_Valid_Ind =\'E\' AND B.Bal<>0
	WHERE A.STATT_DT = ${TX_DATE}
		AND A.PRIN_SUBJ_ID<>\'13020101\'
		GROUP BY A.PRIN_SUBJ_ID,CUR_CD ,B.Org_Id
		) ALL_T88
		GROUP BY SUBJ_ID,CUR_CD ,Org_Id
		UNION ALL
		
		-- 债券面值
		SELECT SUBJ_ID,
			ORG_ID,
			CUR_CD,
			SUM(CURR_BAL),
			\'[本期末汇总层设计基础层取]发行债券\' AS DATA_SRC 
		FROM 
		(SELECT T.SUBJ_ID,T.Org_Id, T.CUR_CD, T.CURR_BAL, T5.SUBJ_ID SUBJ_ID_LX, ABS(T5.AMT) - ABS(T6.AMT) CURR_BAL_LX FROM
			(SELECT T1.AGMT_ID
				,T4.SUBJ_ID AS SUBJ_ID
				,t3.code as Org_Id  -- 机构  
				,T1.CUR_CD AS CUR_CD
				,T4.AMT AS CURR_BAL
				FROM PDM.T03_BOND_ISSU_H T1 
			LEFT JOIN PDM.t03_recvbl_invst_tx_subj_comp T4 
				ON T1.AGMT_ID = T4.AGMT_ID 
				AND T4.STATT_DT = ${TX_DATE} 
			LEFT JOIN pdm.T04_NEW_FIN_ORG_H t3 
				ON t1.org_id = t3.org_id
				AND t3.Statt_t = ${TX_DATE}
			WHERE T1.START_DT <= ${TX_DATE}
				AND T1.END_DT >= ${TX_DATE}
				AND  T4.AMT_CATE_CD= \'ZBZ003\'
			) T
		LEFT JOIN PDM.t03_recvbl_invst_tx_subj_comp T5 
			ON T.AGMT_ID = T5.AGMT_ID 
			AND T5.STATT_DT = ${TX_DATE} 
			AND T5.AMT_CATE_CD= \'ZBZ002\'
		LEFT JOIN PDM.t03_recvbl_invst_tx_subj_comp T6 
			ON T.AGMT_ID = T6.AGMT_ID 
			AND T6.STATT_DT = ${TX_DATE} 
			AND T6.AMT_CATE_CD= \'ZBZ004\'
		) A
		GROUP BY SUBJ_ID,CUR_CD ,ORG_ID
		
		UNION ALL
		-- 利息调整
		SELECT SUBJ_ID_LX,
			ORG_ID,
			CUR_CD,
			SUM(CURR_BAL_LX),
			\'[本期末汇总层设计基础层取]发行债券\' AS DATA_SRC
			FROM 
		(SELECT T.SUBJ_ID,T.ORG_ID, T.CUR_CD, T.CURR_BAL, T5.SUBJ_ID SUBJ_ID_LX, ABS(T5.AMT) - ABS(T6.AMT) CURR_BAL_LX FROM
			(SELECT T1.AGMT_ID
				,T4.SUBJ_ID AS SUBJ_ID
				,t3.code as Org_Id  -- 机构  
				,T1.CUR_CD AS CUR_CD
				,T4.AMT AS CURR_BAL
				FROM PDM.T03_BOND_ISSU_H T1 
			LEFT JOIN PDM.t03_recvbl_invst_tx_subj_comp T4 
				ON T1.AGMT_ID = T4.AGMT_ID 
				AND T4.STATT_DT = ${TX_DATE} 
			LEFT JOIN pdm.T04_NEW_FIN_ORG_H t3 
				ON t1.org_id = t3.org_id
				AND t3.Statt_t = ${TX_DATE}
			WHERE T1.START_DT <= ${TX_DATE}
			 AND T1.END_DT >= ${TX_DATE}
			 AND  T4.AMT_CATE_CD= \'ZBZ003\'
			) T
		LEFT JOIN PDM.t03_recvbl_invst_tx_subj_comp T5 
			ON T.AGMT_ID = T5.AGMT_ID 
			AND T5.STATT_DT = ${TX_DATE} 
			AND T5.AMT_CATE_CD= \'ZBZ002\'
		LEFT JOIN PDM.t03_recvbl_invst_tx_subj_comp T6 
			ON T.AGMT_ID = T6.AGMT_ID 
			AND T6.STATT_DT = ${TX_DATE} 
			AND T6.AMT_CATE_CD= \'ZBZ004\'
		) A
		GROUP BY SUBJ_ID_LX,CUR_CD,ORG_ID

		UNION ALL
		SELECT 
			SUBJ_ID,
			''0001'' ,-- Org_Id
			\'CNY\' AS CUR_CD,
			ABS(SUM(COST_BAL)) AS CURR_BAL,
			\'[本期末汇总层设计基础层取]应付债券-应付同业存单\' AS DATA_SRC
		FROM (
			SELECT  T1.CRET_DPST_CD,T1.CRET_DPST_SHT_NM,T3.COST_BAL,T4.SUBJ_ID,T4.SUBJ_NM,T4.AMT_CATE_CD 
			 FROM PDM.t03_ibank_cret_dpst_h T1
			LEFT JOIN (SELECT * FROM  
							(SELECT SRC_SYS_CRET_DPST_ID,COST_BAL,
									ROW_NUMBER() OVER(PARTITION BY SRC_SYS_CRET_DPST_ID ORDER BY TO_NUMBER(SEQ_NUM) DESC) RM 
							   FROM PDM.T03_IBANK_CRET_DPST_ACCTI_DTL 
							  WHERE STATT_DT=${TX_DATE}
							 ) A WHERE RM=1 
						) T3
				 ON T1.SRC_SYS_CRET_DPST_ID=T3.SRC_SYS_CRET_DPST_ID
		LEFT JOIN PDM.t03_cap_subj_comp_h T4
			ON T1.APPL_ID=T4.SRC_SYS_ACCT_NUM 
			AND T4.START_DT<=${TX_DATE} 
			AND T4.END_DT>=${TX_DATE} 
			-- AND T4.SUBJCODE=\'25020102\'
		WHERE T1.START_DT <=${TX_DATE} 
			AND T1.END_DT >=${TX_DATE}
			AND T1.VALID_IND=\'E\' 
			AND T3.COST_BAL <>0
			AND T4.AMT_CATE_CD=\'ZJJE0001\'
		) AAA GROUP BY SUBJ_ID 
		
		UNION ALL
		SELECT 
			SUBJ_ID,
			''0001'', -- Org_Id
			\'CNY\' AS CUR_CD,
			ABS(SUM(Int_Adj_Bal)) AS CURR_BAL,
			\'[本期末汇总层设计基础层取]应付债券-应付同业存单\' AS DATA_SRC
		FROM (
			SELECT  T1.CRET_DPST_CD,T1.CRET_DPST_SHT_NM,T3.COST_BAL,t3.Int_Adj_Bal,COALESCE(T4_1.SUBJ_ID,T4_2.SUBJ_ID) as SUBJ_ID
			 FROM PDM.t03_ibank_cret_dpst_h T1
			LEFT JOIN (SELECT * FROM  
							(SELECT SRC_SYS_CRET_DPST_ID,COST_BAL,Int_Adj_Bal,
									ROW_NUMBER() OVER(PARTITION BY SRC_SYS_CRET_DPST_ID ORDER BY TO_NUMBER(SEQ_NUM) DESC) RM 
							   FROM PDM.T03_IBANK_CRET_DPST_ACCTI_DTL 
							  WHERE STATT_DT = ${TX_DATE}
							 ) A WHERE RM =1 
						) T3
				 ON T1.SRC_SYS_CRET_DPST_ID=T3.SRC_SYS_CRET_DPST_ID
		LEFT JOIN PDM.t03_cap_subj_comp_h T4_1
			ON T1.APPL_ID = T4_1.SRC_SYS_ACCT_NUM 
			AND T4_1.START_DT<=${TX_DATE} 
			AND T4_1.END_DT>=${TX_DATE} 
			AND T4_1.AMT_CATE_CD=\'ZJJE0022\'
		LEFT JOIN PDM.t03_cap_subj_comp_h T4_2
			ON T1.APPL_ID = T4_2.SRC_SYS_ACCT_NUM 
			AND T4_2.START_DT<=${TX_DATE} 
			AND T4_2.END_DT>=${TX_DATE} 
			AND T4_2.AMT_CATE_CD=\'ZJJE0023\'
		WHERE T1.START_DT <=${TX_DATE} 
			AND T1.END_DT >=${TX_DATE}
			AND T1.VALID_IND =\'E\' 
			AND T3.Int_Adj_Bal <> 0
		) AAA GROUP BY SUBJ_ID
	 
		UNION ALL
		SELECT CASE          
		    WHEN t.Prod_Id IN (\'NCM10201010\', \'NCM10202010\') THEN  \'7001\'          
		    WHEN t.Prod_Id IN (\'NCM10352010\', \'NCM10352020\') THEN  \'7002\'          
		    WHEN t.Prod_Id IN (\'NCM1035101010\', \'NCM1035101020\', \'NCM1035102010\', \'NCM1035102020\') THEN \'7018\'                                                                    
		    WHEN SUBSTR(t.Prod_Id,4,9) IN (\'NCM103530\',\'NCM103540\',\'NCM103550\') THEN \'7003\'       
		  END, -- 科目
        t.Pln_Mgmt_Org_Id  as  Org_Id ,
 		    t.Cur_Cd, -- 币总
		    SUM(T1.Bal), -- 余额
			\'[本期未汇总层设计基础层取]表外业务\' AS DATA_SRC
		  FROM pdm.t03_loan_dubil_h t 
		LEFT JOIN pdm.t03_agmt_bal_h t1 
		    ON t.Agmt_Id = t1.Agmt_Id 
		   AND t.Agmt_Cate_Cd = t1.Agmt_Cate_Cd -- 0101借据
		   AND t1.Agmt_Bal_Typ_Cd = \'01\' -- 借据余额
		   AND t1.Start_Dt <= ${TX_DATE}
		   AND t1.End_dt >= ${TX_DATE}
		WHERE t.Start_Dt <= ${TX_DATE}
		  AND t.End_dt >= ${TX_DATE}
		  AND t1.Bal <> 0
		  AND (t.Prod_Id IN (\'NCM10201010\', \'NCM10202010\',\'NCM10352010\', \'NCM10352020\') or t.Prod_Id IN (\'NCM1035101010\', \'NCM1035101020\', \'NCM1035102010\', \'NCM1035102020\') or SUBSTR(t.Prod_Id, 4, 9) IN (\'NCM103530\', \'NCM103540\', \'NCM103550\'))
		GROUP BY 
		   CASE          
		    WHEN t.Prod_Id IN (\'NCM10201010\', \'NCM10202010\') THEN  \'7001\'          
		    WHEN t.Prod_Id IN (\'NCM10352010\', \'NCM10352020\') THEN  \'7002\'          
		    WHEN t.Prod_Id IN (\'NCM1035101010\', \'NCM1035101020\', \'NCM1035102010\', \'NCM1035102020\') THEN \'7018\'                                                                    
		    WHEN SUBSTR(t.Prod_Id,4,9) IN (\'NCM103530\',\'NCM103540\',\'NCM103550\') THEN \'7003\'       
		  END,
      t.Cur_Cd,t.Pln_Mgmt_Org_Id   

		UNION ALL

	    SELECT t1.Subj_Id, 
				t.Org_Id,
				t.Agmt_Cur_Cd, 
       		   SUM(t2.Bal),
               CASE WHEN t.Prod_Id IN(\'NCS01010604\',\'NCS01010605\',\'NCS01010606\',\'NCS01010607\',\'NCS01010613\',\'NCS01010614\',\'NCS01010615\',\'NCS01010616\',
                        \'NCS01010622\',\'NCS01010623\',\'NCS01010624\',\'NCS01010625\',\'NCS01010631\',\'NCS01010632\',\'NCS01010633\',\'NCS01010634\')
                         THEN \'[本期未汇总层设计基础层取]卖出回购票据\' 
                    WHEN t.Prod_Id IN(\'NCS01010513\',\'NCS01010601\',\'NCS01010602\',\'NCS01010603\',\'NCS01010608\',\'NCS01010609\',\'NCS01010610\',\'NCS01010611\',\'NCS01010612\',\'NCS01010617\',
			            \'NCS01010618\',\'NCS01010619\',\'NCS01010620\',\'NCS01010621\',\'NCS01010626\',\'NCS01010627\',\'NCS01010628\',\'NCS01010629\',\'NCS01010630\',\'NCS01010635\',\'NCS01010636\')
     	                 THEN \'[本期未汇总层设计基础层取]票据\' 
     	        end AS DATA_SRC
	 FROM pdm.t03_acct_h t
	LEFT JOIN PDM.t03_agmt_subj_rel_h t1 
  		ON t.Agmt_Id=t1.Agmt_Id
 		AND t1.Start_Dt <= ${TX_DATE} and t1.End_Dt >= ${TX_DATE}
 		AND t1.Agmt_Cate_Cd = \'0202\' -- 票据账户
	LEFT JOIN  pdm.t03_agmt_bal_h t2
  		ON t.Agmt_Id=t2.Agmt_Id  
  		AND t2.Agmt_Bal_Typ_Cd = \'06\' -- 余额bal
  		AND t2.Start_Dt <= ${TX_DATE} and t2.End_Dt >= ${TX_DATE}
	WHERE t.Start_Dt <= ${TX_DATE}  and t.End_Dt >= ${TX_DATE}
  		AND t.Main_Src_Task = \'NCS_MB_ACCT\'
  		AND t2.Bal <> 0
		AND t.Prod_Id IN(\'NCS01010604\',\'NCS01010605\',\'NCS01010606\',\'NCS01010607\',\'NCS01010613\',\'NCS01010614\',\'NCS01010615\',\'NCS01010616\',
                        \'NCS01010622\',\'NCS01010623\',\'NCS01010624\',\'NCS01010625\',\'NCS01010631\',\'NCS01010632\',\'NCS01010633\',\'NCS01010634\', -- 卖出回购票据
                        \'NCS01010513\',\'NCS01010601\',\'NCS01010602\',\'NCS01010603\',\'NCS01010608\',\'NCS01010609\',\'NCS01010610\',\'NCS01010611\',\'NCS01010612\',\'NCS01010617\',
			            \'NCS01010618\',\'NCS01010619\',\'NCS01010620\',\'NCS01010621\',\'NCS01010626\',\'NCS01010627\',\'NCS01010628\',\'NCS01010629\',\'NCS01010630\',\'NCS01010635\',\'NCS01010636\' -- 票据
                        )    
        AND t1.Subj_Id in(\'21110207\',\'20210403\') -- 科目
    GROUP BY t1.Subj_Id,  t.Agmt_Cur_Cd,t.Prod_Id ,t.Org_Id

    UNION ALL
  SELECT   q.Subj_Id,
       q.Org_Id,
        	   q.Agmt_Cur_Cd,
       	   sum(q.Bal),
       	   \'[本期未汇总层设计基础层取]同业存放\' AS DATA_SRC
   FROM
  (SELECT   t2.Subj_Id,
           t1.Org_Id,
        	   t1.Agmt_Cur_Cd,
       	   t3.Bal
  	 FROM pdm.t03_acct_h t1 
	LEFT JOIN pdm.t03_agmt_subj_rel_h t2
       ON  t1.Agmt_Id = t2.Agmt_Id  
       AND t2.Agmt_Cate_Cd = \'0307\' -- 存放同业账户
       AND t2.Start_Dt <= ${TX_DATE} 
       AND t2.End_Dt >= ${TX_DATE}
	LEFT JOIN pdm.t03_agmt_bal_h t3
  		ON t1.Agmt_Id = t3.Agmt_Id  
  	   AND t3.Agmt_Bal_Typ_Cd = \'06\' -- 余额bal
  	   AND t3.Main_Src_Task = \'NCS_MB_ACCT\'	
       AND t3.Start_Dt <= ${TX_DATE} 
       AND t3.End_Dt >= ${TX_DATE}
	WHERE t1.Start_Dt <= ${TX_DATE} 
  	   AND t1.End_Dt >= ${TX_DATE} 
       AND T1.Acct_Cate_Cd <> \'T\'
       AND t1.Accti_Stat_Cd <> \'SUS\'
  	   AND nvl(t2.Subj_Id,\'W\') not in (\'20120206\',\'20120219\')
  	   AND t3.Bal <> 0
    UNION ALL
	SELECT t2.Subj_Id,
      t1.Org_Id,
        	   t1.Agmt_Cur_Cd,
       	   t3.Bal
  	 FROM pdm.t03_acct_h t1 
	LEFT JOIN pdm.t03_agmt_subj_rel_h t2
       ON  t1.Agmt_Id = t2.Agmt_Id  
       AND t2.Agmt_Cate_Cd = \'0307\' -- 存放同业账户
       AND t2.Start_Dt <= ${TX_DATE} 
       AND t2.End_Dt >= ${TX_DATE}
	LEFT JOIN pdm.t03_agmt_bal_h t3
  		ON t1.Agmt_Id = t3.Agmt_Id  
  	   AND t3.Agmt_Bal_Typ_Cd = \'06\' -- 余额bal
  	   AND t3.Main_Src_Task = \'NCS_MB_ACCT\'
       AND t3.Start_Dt <= ${TX_DATE} 
       AND t3.End_Dt >= ${TX_DATE}
	WHERE t1.Start_Dt <= ${TX_DATE} 
  	   AND t1.End_Dt >= ${TX_DATE} 
       -- AND T1.Acct_Cate_Cd <> \'T\'
       -- AND t1.Accti_Stat_Cd <> \'SUS\'
  	   AND t2.Subj_Id in (\'20120206\',\'20120219\',\'20120201\',\'20120202\',\'20120207\')
  	   AND t3.Bal <> 0
  ) q group by q.Subj_Id, q.Agmt_Cur_Cd, q.Org_Id     -- 机构   

	UNION ALL
	SELECT T2.Subj_Id, -- 科目
      t3.code as Org_Id,
            T1.Cur_Cd,  -- 币种
           SUM(T2.AMT), -- 余额
           \'[本期未汇总层设计基础层取]应收款项投资\'
    FROM pdm.t03_bond_buy_h t1
  LEFT JOIN pdm.t03_recvbl_invst_tx_subj_comp t2
    ON T1.Agmt_Id = T2.Agmt_Id
   AND t2.Amt_Cate_Cd = \'YSKX001\'
   AND t2.Statt_Dt = ${TX_DATE}
  LEFT JOIN pdm.T04_NEW_FIN_ORG_H t3 
		ON t1.org_id = t3.org_id
	   AND t3.Statt_t = ${TX_DATE}
 WHERE t1.start_dt <= ${TX_DATE} and t1.end_dt >= ${TX_DATE}
   AND t2.Subj_Id in (\'11020101\',\'12220101\',\'15032001\')
   AND t2.amt <> 0
 GROUP BY  T2.Subj_Id, T1.Cur_Cd,t3.code    -- 机构   非核心
 
 UNION ALL
	SELECT T2.Subj_Id, -- 科目
			t3.code as Org_Id,
            T1.Cur_Cd,  -- 币种
           SUM(T2.AMT), -- 余额
           \'[本期未汇总层设计基础层取]应收款项投资\'
    FROM pdm.t03_bond_buy_h t1
  LEFT JOIN pdm.t03_recvbl_invst_tx_subj_comp t2
    ON T1.Agmt_Id = T2.Agmt_Id
   AND t2.Amt_Cate_Cd = \'YSKX004\'
   AND t2.Statt_Dt = ${TX_DATE}
  LEFT JOIN pdm.T04_NEW_FIN_ORG_H t3 
		ON t1.org_id = t3.org_id
	   AND t3.Statt_t = ${TX_DATE}
 WHERE t1.start_dt <= ${TX_DATE} and t1.end_dt >= ${TX_DATE}
   AND t2.Subj_Id in (\'12220301\')
   AND t2.amt <> 0
 GROUP BY  T2.Subj_Id, T1.Cur_Cd ,t3.code

/* 货币基金从FDS系统取分户账
    UNION ALL
 SELECT t4.Subj_Id,
 		t.cur_cd, 
       SUM(CASE WHEN t3.Bal_Drct = \'D\' THEN t3.Bal ELSE -1*T3.Bal END),
		\'[本期未汇总层设计基础层取]非标投资-货币基金投资\'
 FROM pdm.t02_fund_h t 
 LEFT JOIN pdm.t03_cap_subj_comp_h  t4 
   ON t.prod_id = t4.Agmt_Id
  AND t4.Amt_Cate_Cd = \'ZJJE0001\'
  AND t4.start_dt <= ${TX_DATE} AND t4.end_dt >=${TX_DATE}
 LEFT JOIN  pdm.t03_cap_realtm_bal t3 
  ON t.prod_id = t3.Agmt_ID
  AND t3.Statt_Dt = ${TX_DATE} 
  AND t3.Amt_Cate = \'ZJJE0001\'
  AND t3.Data_Valid_Ind = \'E\'
 WHERE t.start_dt <= ${TX_DATE}  and t.end_dt >=${TX_DATE} 
  AND t.Main_Src_Task =\'FDS_FUND_INFO\'
  AND t3.bal <>0
  AND t4.Subj_Id = \'11010301\'
 GROUP BY t.cur_cd, t4.Subj_Id 
 
   UNION ALL
 SELECT t4.Subj_Id,
 		t.cur_cd, 
       SUM(CASE WHEN t3.Bal_Drct = \'D\' THEN t3.Bal ELSE -1*T3.Bal END),
		\'[本期未汇总层设计基础层取]非标投资-货币基金投资\'
 FROM pdm.t02_fund_h t 
 LEFT JOIN pdm.t03_cap_subj_comp_h  t4 
   ON t.prod_id = t4.Agmt_Id
  AND t4.Amt_Cate_Cd = \'ZJJE0016\'
  AND t4.start_dt <= ${TX_DATE} AND t4.end_dt >=${TX_DATE}
 LEFT JOIN  pdm.t03_cap_realtm_bal t3 
  ON t.prod_id = t3.Agmt_ID
  AND t3.Statt_Dt = ${TX_DATE} 
  AND t3.Amt_Cate = \'ZJJE0016\'
  AND t3.Data_Valid_Ind = \'E\'
 WHERE t.start_dt <= ${TX_DATE}  and t.end_dt >=${TX_DATE} 
  AND t.Main_Src_Task =\'FDS_FUND_INFO\'
  AND t3.bal <>0
  AND t4.Subj_Id = \'11010303\'
 GROUP BY t.cur_cd, t4.Subj_Id 
 */
	UNION ALL
 SELECT t.Subj_ID,
		\'0000\' as org_id,
		\'CNY\',
	   sum(t1.Acct_Bal) Acct_Bal,
	   \'[本期未汇总层设计基础层取]非标投资-货币基金投资\'
 FROM pdm.t03_cap_acct_info t 
INNER JOIN pdm.t03_cap_acct_bal t1
	   ON t.Inn_Acct_Num = t1.Inn_Acct_Num
	  AND t.Acct_Num = t1.Ext_Acct_Num
	  AND t.Org_ID = t1.Org_ID
	  AND t.Cap_TX_Cate_Cd = \'FUND\'
	  AND t1.Acct_Bal > 0
	  AND t1.Statt_Dt = ${TX_DATE}
WHERE t.Statt_Dt = ${TX_DATE}
GROUP BY t.Subj_ID

 
/*	已加入T88_IBANK_CAP_BIZ_INFO
UNION ALL

 SELECT T8.Subj_Id,
 		T1.Cur_Cd,
        sum(case
              when \'D\' THEN
               -nvl(T11.Bal, 0)
              ELSE
               nvl(T11.Bal, 0)
            END) as CUR_BAL,
			\'同业资金-本金\'
   from pdm.t03_ext_acct T1
   LEFT JOIN PDM.t03_cap_subj_comp_h T8
     ON T1.Agmt_Id = T8.Agmt_Id
    AND T8.Amt_Cate_Cd = \'ZJJE0001\'
    AND T8.END_DT = \'9999-12-31\'
   LEFT JOIN PDM.t03_cap_acct_info_h T10
     ON T1.Agmt_Id = T10.Agmt_Id
    AND T10.END_DT = \'9999-12-31\'
   LEFT JOIN PDM.t03_cap_realtm_bal T11
     ON T1.Agmt_Id = T11.Agmt_Id
    AND T11.Amt_Cate = \'ZJJE0001\'
    AND T11.Data_Valid_Ind = \'E\'
    AND t11.Bal <> 0
    and t1.statt_dt = t11.statt_dt
  where t1.statt_dt = ${TX_DATE}
    AND t11.Bal <> 0
 GROUP BY  T1.Cur_Cd, T8.Subj_Id 
 */


UNION ALL

SELECT 
		T5.Form_Desc as Subj_ID, -- 科目
    T3.Form_Desc as org_id, -- 机构
 		T1.Cur_Cd, -- 币种
        ABS(SUM(T1.Ibank_Offer_Amt)) AS  cur_bal, -- 余额
       \'[本期未汇总层设计基础层取]资金-本金\'
  from (SELECT *
          FROM pdm.t03_fx_ibank_offer
         WHERE Recall_Dt = \'0001-01-01\'
           AND Ibank_Offer_Amt <> 0
           AND Chk_Ind = 1
           AND Matr_Dt > ${TX_DATE}
           and St_Int_Dt <= ${TX_DATE}
           and Statt_Dt = ${TX_DATE}) t1
  LEFT JOIN pdm.t01_opics_cust_info t2
    ON \'OPI\' || t1.Tx_Cntpty_Id = Cust_ID
   AND t2.Statt_Dt = ${TX_DATE}
  LEFT JOIN (SELECT *
               FROM pdm.t99_opisc_code
              WHERE Form_ID LIKE \'SL_ACCOUNT%\'
                AND Statt_Dt = ${TX_DATE}) t3
    ON T1.Src_Sys_Brch_Id = \'0\' || T3.Mbank_Ind
  LEFT JOIN (SELECT *
               FROM ods.OPI_EXPT
              WHERE SEQ = \'3\'
                AND cast (sdate AS DATE) =${TX_DATE}) t4
    ON T1.Src_Sys_Brch_Id = T4.BR -- 01
   AND T1.Prod_Id = \'OPI\' || T4.PRODCODE --  OPIMM
   AND T1.Prod_Cate_Cd = T4.TYPE -- BO
   AND T1.Term_Corp_Cd = T4.TENOR -- 99
   AND T4.ACCTNGTYPE = T2.Ibank_Org_Acctn_Cate_Cd
  LEFT JOIN (SELECT *
               FROM pdm.t99_opisc_code
              WHERE Form_ID LIKE \'SL_SUBJECT%\'
                AND Statt_Dt = ${TX_DATE}) t5
    ON TRIM(T4.GLNO) || \'/\' || TRIM(T1.Cost_Ctr_Cd) = TRIM(T5.Form_Val) -- 科目拼接 成本中心关联 
 WHERE T5.Form_Desc IN (\'13020101\', \'20030101\')
 GROUP BY T5.Form_Desc ,T1.Cur_Cd, T3.Form_Desc -- 机构


UNION ALL 

select 
       T5.Subj_Id ,
       t3.Org_ID  ,
        T1.Cur_Cd ,
       SUM(T3.Bal) as cur_bal,
    \'[本期未汇总层设计基础层取]资金-本金\' AS DATA_SRC
  from pdm.t03_secu_fund_ledgr_acct_info t1
  left join pdm.t03_cap_realtm_bal t3
    on t1.Agmt_ID = T3.Agmt_Id
   and t1.Statt_Dt = t3.Statt_Dt
   and T3.Amt_Cate = \'ZJJE0026\'
   and t3.Data_Valid_Ind = \'E\'
  left join pdm.t03_cap_subj_comp_h t5
    on t1.Agmt_ID = T5.Agmt_Id
   and t1.Statt_Dt <= t5.End_Dt
   and T5.Amt_Cate_Cd = \'ZJJE0026\'
 where t1.Statt_Dt =  ${TX_DATE}
 GROUP BY T5.Subj_Id, T1.Cur_Cd,t3.Org_ID

UNION ALL 

-- select 
--     \'XYK\' SUBJ_ID,
--     \'9997\' Org_ID,
--     \'CNY\' CUR_CD,
--     SUM(XFYE + QXYE+ FQYE+ FQWFT + DEFQWFT)  AS CURR_BAL,
-- 	\'信用卡汇总【130704&131001】\' AS DATA_SRC
--  from ODS.CCS_ZHMX  
--  where cast (sdate AS DATE)+1 =${TX_DATE}
--  AND COALESCE(ZHZT,\'\') not in (\'WQ\',\'W\') 

select 
    \'XYK\' SUBJ_ID,
    \'9997\' Org_ID,
    Cur_Cd,
    SUM(Od_Amt)  AS CURR_BAL,
	\'信用卡汇总【130704&131001】\' AS DATA_SRC
  from pdm.t88_ccard_acct_info  
 where Statt_Dt = ${TX_DATE} - 1
 group by Cur_Cd
 
	) S2
	ON S1.SUBJ_ID = S2.SUBJ_ID
	AND S1.CUR_CD = S2.CUR_CD
	AND S1.org_id = S2.org_id
LEFT JOIN  pdm.t04_core_org_h org 
	ON S1.org_id=org.Org_Id
	AND org.start_dt <= ${TX_DATE} 
	AND org.end_dt >= ${TX_DATE} 
	ORDER BY S1.SUBJ_ID,S1.CUR_CD
';

	CALL etl.pr_exec_sql(@RTC,'',ETL_T_TAB_ENG_NAME,ETL_STEP_NO,@SQL_STR,ETL_TX_DATE);
	IF @RTC <> 0 THEN LEAVE LABLE;END IF;
	SELECT ETL_STEP_NO + 1 INTO ETL_STEP_NO;

	-- CALL etl.pr_f5_T88_gl_check_org(@RTC,ETL_TX_DATE);

	SET OUT_RES_MSG = 'SUCCESSFUL';

END |