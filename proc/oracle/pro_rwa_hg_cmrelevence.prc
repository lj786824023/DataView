CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_HG_CMRELEVENCE(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期 yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_HG_CMRELEVENCE
    实现功能:核心系统-回购-合同缓释物关联(从数据源核心系统将回购类相关信息全量导入RWA回购接口表合同缓释物关联表中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :LISY
    编写时间:2016-04-21
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.BRD_REPO_PORTFOLIO|债券回购质押信息
    源  表2 :RWA_DEV.BRD_SECURITY_POSI|债券头寸
    源  表3 :RWA_DEV.BRD_BOND|债券信息
    源  表4 :RWA_DEV.BRD_REPO|债券回购
    源  表5 :RWA_DEV.BRD_BILL_REPO_PORTF|票据回购质押信息
    源  表5 :RWA_DEV.BRD_BILL_REPO|票据回购信息 
    源  表5 :RWA_DEV.BRD_BILL|票据信息 
    
    目标表  :RWA_DEV.RWA_HG_CMRELEVENCE|核心系统回购类合同缓释物关联表
    变更记录(修改人|修改时间|修改内容):
    pxl 2019/04/15 去除补录、老核心系统表
    
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_HG_CMRELEVENCE';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_HG_CMRELEVENCE';

    --3.将满足条件的数据从源表插入到目标表中
    --3.1 核心系统-买入返售债券回购-质押式
    INSERT INTO RWA_DEV.RWA_HG_CMRELEVENCE(
                DATADATE                               --数据日期
                ,DATANO                                --数据流水号
                ,CONTRACTID                       	 	 --合同ID
                ,MITIGATIONID                     	 	 --缓释物ID
                ,MITIGCATEGORY                    	 	 --缓释物类型
                ,SGUARCONTRACTID                  	 	 --源担保合同ID
                ,GROUPID                          	 	 --分组编号
    )
    SELECT
        TO_DATE(p_data_dt_str,'YYYYMMDD')	, --数据日期
        p_data_dt_str	, --数据流水号
        'MRFSZQ' || T3.ACCT_NO	, --合同ID
        T1.REPO_REFERENCE || T1.SECURITY_REFERENCE	, --缓释物ID
        '03'	, --缓释物类型
        'MRFSZQ' || T3.ACCT_NO	, --源担保合同ID
        '' --分组编号
    FROM  BRD_REPO_PORTFOLIO T1 --债券回购质押信息
    INNER JOIN BRD_REPO T3 --债券回购
            ON T1.REPO_REFERENCE = T3.ACCT_NO --回购交易编号
            AND T1.DATANO=T3.DATANO
    LEFT JOIN BRD_BOND T4 --债券信息
           ON T1.SECURITY_REFERENCE = T4.BOND_ID
           AND T1.DATANO=T4.DATANO
    WHERE  T3.CASH_NOMINAL <> 0
         --AND T3.CLIENT_PROPRIETARY = 'F'  --质押式  是否可以再质押 源系统字段全部为空  且只有质押式业务
         AND T3.REPO_TYPE IN ( '4', 'RB')  --买入返售
         AND T3.PRINCIPAL_GLNO LIKE '111103%'  --买入返售债券回购-质押式-债券
         AND T4.ISSUER_CODE IS NOT NULL    
         AND T3.ACCT_NO IS NOT NULL   
         AND T1.DATANO=p_data_dt_str
    ;

    COMMIT;

		--3.2 核心系统-买入返售票据回购-质押式
    INSERT INTO RWA_DEV.RWA_HG_CMRELEVENCE(
                DATADATE                               --数据日期
                ,DATANO                                --数据流水号
                ,CONTRACTID                       	 	 --合同ID
                ,MITIGATIONID                     	 	 --缓释物ID
                ,MITIGCATEGORY                    	 	 --缓释物类型
                ,SGUARCONTRACTID                  	 	 --源担保合同ID
                ,GROUPID                          	 	 --分组编号
    )
    SELECT TO_DATE(p_data_dt_str, 'YYYYMMDD'), --数据日期
           p_data_dt_str, --数据流水号
           'MRFSPJ' || T2.ACCT_NO, --合同ID
           T1.REPO_REFERENCE, --缓释物ID
           '03', --缓释物类型
           'MRFSPJ' || T2.ACCT_NO, --源担保合同ID
           '' --分组编号
      FROM BRD_BILL_REPO_PORTF T1
      LEFT JOIN BRD_BILL_REPO T2
        ON T1.REPO_REFERENCE = T2.ACCT_NO
        AND T1.DATANO=T2.DATANO
       AND T1.SECURITY_REFERENCE = T2.SECURITY_REFERENCE
      LEFT JOIN BRD_BILL T3
        ON T1.REPO_REFERENCE = T3.ACCT_NO
        AND T1.DATANO=T3.DATANO
       AND T1.SECURITY_REFERENCE = T3.BILL_NO
     WHERE --T2.CLIENT_PROPRIETARY = 'Y'  --质押式 是否可以再质押 源系统字段全部为空  且只有质押式业务
     T2.REPO_TYPE IN ('4', 'RB') --买入返售
     AND T2.CASH_NOMINAL <> 0 --过滤无效数据
     AND T2.PRINCIPAL_GLNO IS NOT NULL --经ALM集市反馈  科目为空的数据不计帐为历史数据
     AND SUBSTR(T2.PRINCIPAL_GLNO, 1, 6) = '111102' --买入返售金融资产-买入返售票据
     AND (T2.CLIENT_PROPRIETARY <> 'N' OR T2.CLIENT_PROPRIETARY IS NULL) --是否可以再质押 N为买断式  非N质押式
     AND T1.DATANO = p_data_dt_str
    ;

    COMMIT;
    
    --3.2 核心系统-买入返售票据回购-交易对手不是银行，那票据作为缓释
    INSERT INTO RWA_DEV.RWA_HG_CMRELEVENCE(
                DATADATE                               --数据日期
                ,DATANO                                --数据流水号
                ,CONTRACTID                            --合同ID
                ,MITIGATIONID                          --缓释物ID
                ,MITIGCATEGORY                         --缓释物类型
                ,SGUARCONTRACTID                       --源担保合同ID
                ,GROUPID                               --分组编号
    )
    SELECT TO_DATE(p_data_dt_str, 'YYYYMMDD'), --数据日期
           p_data_dt_str, --数据流水号
           'MRFSPJ' || T1.ACCT_NO, --合同ID
           T1.SECURITY_REFERENCE, --缓释物ID
           '03', --缓释物类型
           'MRFSPJ' || T1.ACCT_NO, --源担保合同ID
           '' --分组编号
      FROM RWA_DEV.BRD_BILL_REPO T1 --票据回购            
      LEFT JOIN RWA_DEV.NCM_CUSTOMER_INFO T3
        ON T1.CUST_NO = T3.MFCUSTOMERID
        AND T1.DATANO=T3.DATANO
       AND T3.CUSTOMERTYPE NOT LIKE '03%' --对公客户                   
     WHERE T1.CASH_NOMINAL <> 0 --过滤无效数据
       AND T1.PRINCIPAL_GLNO IS NOT NULL --经ALM集市反馈  科目为空的数据不计帐为历史数据
       AND SUBSTR(T1.PRINCIPAL_GLNO, 1, 6) = '111102' --买入返售金融资产-买入返售票据
       AND (T1.CLIENT_PROPRIETARY <> 'N' OR T1.CLIENT_PROPRIETARY IS NULL) --是否可以再质押 N为买断式  非N质押式
       AND T3.CUSTOMERNAME NOT LIKE '%银行%' --交易对手不是银行才需要那票据做缓释
       AND T1.DATANO = p_data_dt_str
    ;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_HG_CMRELEVENCE',cascade => true);

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_HG_CMRELEVENCE;
    --Dbms_output.Put_line('RWA_DEV.RWA_HG_CMRELEVENCE表当前插入的核心系统-回购数据记录为: ' || v_count || ' 条');



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
END PRO_RWA_HG_CMRELEVENCE;
/

