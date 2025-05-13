CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_TX_WSIB(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期 yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_TX_WSIB
    实现功能:信贷系统-票据贴现-补录铺底(从数据源信贷系统将业务相关信息全量导入RWA票据贴现补录铺底表中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :LISY
    编写时间:2016-06-20
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :NCM_BILL_INFO|票据信息表(不使用)
    源  表2 :NCM_BUSINESS_DUEBILL|授信业务借据信息表
    目标表1 :RWA.RWA_WSIB_BILLDISCOUNT|票据贴现补录铺底表
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_TX_WSIB';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    --清空票据贴现铺底表
    --EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA.RWA_WSIB_BILLDISCOUNT';
    DELETE FROM RWA.RWA_WSIB_BILLDISCOUNT WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    COMMIT;

    --2.将满足条件的数据从源表插入到目标表中
    --信贷系统-票据贴现业务
    INSERT INTO RWA.RWA_WSIB_BILLDISCOUNT(
                DATADATE                               --数据日期
                ,ORGID                             	 	 --机构ID
                ,BDSERIALNO                         	 --借据编号
                ,BILLNO           										 --票据编号
                ,BILLTYPE                           	 --票据类型
                ,ACCEPTORID                         	 --承兑行/承兑企业ID
                ,ACCEPTOR                           	 --承兑行/承兑企业名称
                ,ACCEPTORGCODE                      	 --承兑行/承兑企业组织机构代码
                ,ACCEPTCOUNTRYCODE                  	 --承兑行/承兑企业注册国家代码
                ,ACCEPTINDUSTRYID 										 --承兑行/承兑企业所属行业代码
                ,ACCEPTSCOPE      										 --承兑企业规模
    )
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --数据日期
                ,'01370000'			                 				 		 AS ORGID                		 --机构ID              按照补录任务分配情况，默认为总行金融同业管理部(01370000)
                ,T1.SERIALNO                             		 AS BDSERIALNO               --借据编号
                ,T1.BILLNO																	 AS BILLNO           				 --票据编号
                ,CASE WHEN T1.BUSINESSTYPE = '104010' THEN '10'
                 ELSE '20'
                 END																				 AS BILLTYPE                 --票据类型
                ,p_data_dt_str || 'TX' ||lpad(rownum, 10, '0')
                						                           			 AS ACCEPTORID               --承兑行/承兑企业ID
                ,''			                         				 		 AS ACCEPTOR                 --承兑行/承兑企业名称
                ,''                              	 					 AS ACCEPTORGCODE            --承兑行/承兑企业组织机构代码
                ,''				                              		 AS ACCEPTCOUNTRYCODE        --承兑行/承兑企业注册国家代码
                ,''																					 AS ACCEPTINDUSTRYID 				 --承兑行/承兑企业所属行业代码
                ,''																					 AS ACCEPTSCOPE      				 --承兑企业规模

    FROM				RWA_DEV.NCM_BUSINESS_DUEBILL T1	             		 					--授信业务借据信息表
		/*INNER JOIN 	RWA_DEV.NCM_BILL_INFO T2																	--票据信息表,可能在做业务时没有录入票据承兑信息，在借据中的票据可能在票据表中没有
		ON 					REPLACE(T1.BILLNO,' ','') = REPLACE(T2.BILLNO,' ','')
		AND 				T1.RELATIVESERIALNO2 = T2.OBJECTNO
		AND 				T2.OBJECTTYPE = 'BusinessContract'
		AND					T2.DATANO = p_data_dt_str*/
		WHERE 			T1.BALANCE > 0																						--余额大于0
    AND 				(T1.FINISHDATE IS NULL OR  T1.FINISHDATE = '')						--未结清的有效借据
    AND 				T1.BUSINESSTYPE IN ('104010','104020')										--104010=银行承兑汇票贴现；104020=商业承兑汇票贴现
    AND 				T1.DATANO = p_data_dt_str
		;

    COMMIT;

    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA.RWA_WSIB_BILLDISCOUNT WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA.RWA_WSIB_BILLDISCOUNT表当前插入的信贷系统-票据贴现铺底数据记录为: ' || v_count || ' 条');



    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功' || '-' || v_count;
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '票据贴现补录数据铺底('|| v_pro_name ||')处理失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_TX_WSIB;
/

