CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_PJ_WSIB(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期 yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_PJ_WSIB
    实现功能:核心系统-票据转贴现-补录铺底(从数据源核心系统将业务相关信息全量导入RWA票据转贴现补录铺底表中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :LISY
    编写时间:2016-06-06
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.CBS_LNU|贴现卡片帐
    源  表2 :RWA_DEV.CBS_LNM|贷款户主档
    源  表3 :RWA_DEV.CBS_ACS|帐户与客户关系资料表
    源  表4 :RWA_DEV.CMS_CUSTOMER_INFO|统一客户信息表
    源  表5 :RWA.ORG_INFO|机构信息表
    源  表6 :RWA.RWA_WS_BO_BILLREDISCOUNT|外部票据转帖现_银票补录表
    源  表7 :RWA.RWA_WS_CO_BILLREDISCOUNT|外部票据转帖现_商票补录表
    源  表8 :RWA.RWA_WS_BI_BILLREDISCOUNT|内部票据转帖现_银票补录表
    源  表9 :RWA.RWA_WP_SUPPTASKORG|补录任务机构分发配置表
    源  表10:RWA.RWA_WP_SUPPTASK|补录任务发布表
    源  表11:RWA_DEV.BL_CUSTOMER_INFO|补录客户汇总表
    目标表1 :RWA.RWA_WSIB_BO_BILLREDISCOUNT|外部票据转帖现_银票铺底表
    目标表2 :RWA.RWA_WSIB_CO_BILLREDISCOUNT|外部票据转帖现_商票铺底表
    目标表3 :RWA.RWA_WSIB_BI_BILLREDISCOUNT|内部票据转帖现_银票铺底表
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_PJ_WSIB';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    --清空票据转贴现补录铺底表
    DELETE FROM RWA.RWA_WSIB_BI_BILLREDISCOUNT WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    COMMIT;

    --2.将满足条件的数据从源表插入到目标表中
    --2.1 核心系统-内部票据转帖现_银票
    INSERT INTO RWA.RWA_WSIB_BI_BILLREDISCOUNT(
                DATADATE                               --数据日期
                ,ORGID                             	 	 --机构ID
                ,LNUCAR_NO                           	 --账号
                ,LNUCARNO                            	 --卡片号
                ,LNUCERNO                            	 --票据编号
                ,LNMAC_NAM														 --户名
                ,LNUDISAMT														 --贴现金额
                ,LNUCURBAL														 --票据余额
                ,LNUDISDAT														 --贴现日期
                ,LNUEXPDAT														 --贴现到期日期
                ,BELONGORGCODE			                 	 --业务所属机构
                ,ACCEPTOR                            	 --承兑行名称
                ,ACCEPTORGCODE                       	 --承兑行组织机构代码
                ,ACCEPTCOUNTRYCODE                   	 --承兑行注册国家代码
                ,ACCEPTINDUSTRYID              			 	 --承兑行所属行业代码
                ,CLIENTCATEGORY												 --承兑行客户类型
    )
    WITH TMP_SUPPORG AS (
								SELECT T1.ORGID AS ORGID
										   ,CASE WHEN T3.ORGLEVEL > 2 THEN T4.SORTNO ELSE T3.SORTNO END AS SORTNO
								  FROM RWA.RWA_WP_SUPPTASKORG T1
						INNER JOIN RWA.RWA_WP_SUPPTASK T2
								    ON T1.SUPPTASKID = T2.SUPPTASKID
								   AND T2.ENABLEFLAG = '01'
						 LEFT JOIN RWA.ORG_INFO T3
								    ON T1.ORGID = T3.ORGID
						 LEFT JOIN RWA.ORG_INFO T4
	                	ON T3.BELONGORGID = T4.ORGID
								 WHERE T1.SUPPTMPLID = 'M-0043'
							ORDER BY T3.SORTNO
		)
		, TMP_BL_CUST AS (
								SELECT CUSTOMERNAME, CERTID
									FROM RWA_DEV.BL_CUSTOMER_INFO
								WHERE CERTTYPE = 'Ent02'
									AND ROWID IN (SELECT MAX(ROWID)
											            FROM RWA_DEV.BL_CUSTOMER_INFO
											           WHERE CERTTYPE = 'Ent02'
											           GROUP BY CUSTOMERNAME)
		)
		SELECT
								DATADATE                               --数据日期
                ,ORGID                             	 	 --机构ID
                ,LNUCAR_NO                           	 --账号
                ,LNUCARNO                            	 --卡片号
                ,LNUCERNO                            	 --票据编号
                ,LNMAC_NAM														 --户名
                ,LNUDISAMT														 --贴现金额
                ,LNUCURBAL														 --票据余额
                ,LNUDISDAT														 --贴现日期
                ,LNUEXPDAT														 --贴现到期日期
                ,BELONGORGCODE			                 	 --业务所属机构
                ,ACCEPTOR                            	 --承兑行名称
                ,ACCEPTORGCODE                       	 --承兑行组织机构代码
                ,ACCEPTCOUNTRYCODE                   	 --承兑行注册国家代码
                ,ACCEPTINDUSTRYID              			 	 --承兑行所属行业代码
                ,CLIENTCATEGORY												 --承兑行客户类型
		FROM (
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                ,NVL(T7.ORGID,(SELECT ORGID FROM TMP_SUPPORG WHERE ROWNUM = 1))
                												                     AS ORGID										 --机构ID                     		按照补录任务分配情况，分配给金融市场部及其分部
                ,RANK() OVER(PARTITION BY T1.LNUAC_NO,T1.LNUCARNO ORDER BY LENGTH(NVL(T7.SORTNO,(SELECT SORTNO FROM TMP_SUPPORG WHERE ROWNUM = 1))) DESC)
                																						 AS RECORDNO								 --数据序号
                ,T1.LNUAC_NO                             		 AS LNUCAR_NO           		 --账号
                ,T1.LNUCARNO																 AS LNUCARNO                 --卡片号
                ,T1.LNUCERNO                          			 AS LNUCERNO               	 --票据编号
                ,T2.LNMAC_NAM														 		 AS LNMAC_NAM								 --户名
                ,ABS(T1.LNUDISAMT)											 		 AS LNUDISAMT								 --贴现金额
                ,ABS(T1.LNUCURBAL)											 		 AS LNUCURBAL								 --票据余额
                ,TO_CHAR(TO_DATE(T1.LNUDISDAT,'YYYYMMDD'),'YYYY-MM-DD')
                																				 		 AS LNUDISDAT								 --贴现日期
                ,TO_CHAR(TO_DATE(T1.LNUEXPDAT,'YYYYMMDD'),'YYYY-MM-DD')
                																				 		 AS LNUEXPDAT								 --贴现到期日期
                ,T2.LNMGACBK	                   				 		 AS BELONGORGCODE            --业务所属机构
                ,NVL(T6.ACCEPTOR,T1.LNCCERPAY)           		 AS ACCEPTOR                 --承兑行名称
                ,NVL(T6.ACCEPTORGCODE,T8.CERTID)						 AS ACCEPTORGCODE            --承兑行组织机构代码
                ,NVL(T6.ACCEPTCOUNTRYCODE,'CHN')           	 AS ACCEPTCOUNTRYCODE        --承兑行注册国家代码							默认CHN-中国
                ,NVL(T6.ACCEPTINDUSTRYID,'J66')              AS ACCEPTINDUSTRYID         --承兑行所属行业代码							默认J66-货币金融服务
                ,NVL(T6.CLIENTCATEGORY,'0202')               AS CLIENTCATEGORY	         --承兑行客户类型									默认0202-中国商业银行

    FROM				RWA_DEV.CBS_LNU T1	             		 															--贴现卡片帐
		INNER JOIN 	RWA_DEV.CBS_LNM T2																								--贷款户主档
		ON 					T1.LNUAC_NO = T2.LNMAC_NO
		AND					T2.DATANO = p_data_dt_str
		AND         T2.LNMITMNO IN ('13010501','13010505','13010511')      						--13010501-贴现资产-直贴银行承兑汇票本金、13010505-贴现资产-转贴银行承兑汇票本金、13010511-贴现资产-内转电子银行承兑汇票本金
	  LEFT	JOIN	RWA.ORG_INFO T5																										--机构信息表
	  ON					T2.LNMGACBK = T5.ORGID
	  LEFT	JOIN	(SELECT LNUCAR_NO
	  									 ,LNUCARNO
	  									 ,ACCEPTOR
	  									 ,ACCEPTORGCODE
	  									 ,ACCEPTCOUNTRYCODE
	  									 ,ACCEPTINDUSTRYID
	  									 ,CLIENTCATEGORY
	  							 FROM RWA.RWA_WS_BI_BILLREDISCOUNT
	  							WHERE DATADATE = (SELECT MAX(DATADATE) FROM RWA.RWA_WS_BI_BILLREDISCOUNT WHERE DATADATE < TO_DATE(p_data_dt_str,'YYYYMMDD'))
	  						) T6																															--取最近一期补录数据铺底
	  ON					T1.LNUAC_NO = T6.LNUCAR_NO
	  AND					T1.LNUCARNO = T6.LNUCARNO
	  LEFT	JOIN  TMP_SUPPORG T7
    ON          T5.SORTNO LIKE T7.SORTNO || '%'
    LEFT	JOIN	TMP_BL_CUST T8
    ON					T1.LNCCERPAY = T8.CUSTOMERNAME
		WHERE				T1.LNURVSFLG = '0' 																								--抹账标识为正常
		AND 				T1.LNUCURBAL <> 0																									--余额不等于0
		AND					T1.DATANO = p_data_dt_str
		)
		WHERE RECORDNO = 1
		ORDER BY 		LNUCAR_NO,TO_NUMBER(LNUCARNO)
		;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA',tabname => 'RWA_WSIB_BI_BILLREDISCOUNT',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA.RWA_WSIB_BI_BILLREDISCOUNT WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA.RWA_WSIB_BI_BILLREDISCOUNT表当前插入的核心系统-外部票据转帖现_银票铺底数据记录为: ' || v_count1 || ' 条');


    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功' || '-' || v_count;
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '票据转贴现业务补录数据铺底('|| v_pro_name ||')处理失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_PJ_WSIB;
/

