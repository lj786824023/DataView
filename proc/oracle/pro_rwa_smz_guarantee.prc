CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_SMZ_GUARANTEE(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:PRO_RWA_SMZ_GUARANTEE
    实现功能:私募债-保证,表结构为保证表
    数据口径:全量
    跑批频率:月初
    版  本  :V1.0.0
    编写人  :SHUXD
    编写时间:2015-06-29
    单  位	:上海安硕信息技术股份有限公司
    源  表1	:RWA_WS_PRIVATE_BOND|私募债业务补录模板
    目标表	:RWA_SMZ_GUARANTEE|私募债-保证表
    变更记录(修改人|修改时间|修改内容):
  */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'PRO_RWA_SMZ_GUARANTEE';
  --定义判断值变量
  v_count INTEGER;
  --定义异常变量
  v_raise EXCEPTION;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*如果是全量数据加载需清空目标表*/
    --1.清除目标表中的原有记录
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_SMZ_GUARANTEE';

    --将有效借据下对应合同的保证插入到目标表中
    INSERT INTO RWA_SMZ_GUARANTEE(
         				 DATADATE          												  --数据日期
								,DATANO                                     --数据流水号
								,GUARANTEEID                                --保证ID
								,SSYSID                                     --源系统ID
								,GUARANTEECONID                             --保证合同ID
								,GUARANTORID                                --保证人ID
								,CREDITRISKDATATYPE                         --信用风险数据类型
								,GUARANTEEWAY                            		--担保方式
								,QUALFLAGSTD                            		--权重法合格标识
								,QUALFLAGFIRB                               --内评初级法合格标识
								,GUARANTEETYPESTD                           --权重法保证类型
								,GUARANTORSDVSSTD                           --权重法保证人细分
								,GUARANTEETYPEIRB                           --内评法保证类型
								,GUARANTEEAMOUNT                            --保证总额
								,CURRENCY                                   --币种
								,STARTDATE                                  --起始日期
								,DUEDATE                                    --到期日期
								,ORIGINALMATURITY                           --原始期限
								,RESIDUALM                                  --剩余期限
								,GUARANTORIRATING                           --保证人内部评级
								,GUARANTORPD                                --保证人违约概率
								,GROUPID                                    --分组编号
    )
    SELECT
         				 TO_DATE(p_data_dt_str,'YYYYMMDD')							  AS  DATADATE          	 --数据日期
         				,p_data_dt_str																 		AS	DATANO               --数据流水号
         				,T1.DBID		                                      AS	GUARANTEEID          --保证ID
								,'SMZ'																						AS	SSYSID               --源系统ID
								,T1.DBID																		      AS	GUARANTEECONID       --保证合同ID
								,T1.CUSTID2																		    AS	GUARANTORID          --保证人ID
								,'06'																							AS	CREDITRISKDATATYPE   --信用风险数据类型
								,T1.DBLX																	        AS	GUARANTEEWAY       	 --担保方式
								,''																								AS	QUALFLAGSTD          --权重法合格标识
								,''																								AS	QUALFLAGFIRB         --内评初级法合格标识
								,''																								AS	GUARANTEETYPESTD     --权重法保证类型
								,''																								AS	GUARANTORSDVSSTD     --权重法保证人细分
								,''																								AS	GUARANTEETYPEIRB     --内评法保证类型
								,ROUND(TO_NUMBER(T1.DBJZ),6)							        AS	GUARANTEEAMOUNT      --保证总额
								,NVL(T1.DBBZDM,'CNY')										  				AS	CURRENCY             --币种
								,REPLACE(T1.DBQSR,'-','')													AS	STARTDATE            --起始日期
								,REPLACE(T1.DBDQR,'-','')													AS	DUEDATE              --到期日期
								,CASE WHEN (to_date(T1.DBDQR,'yyyy-mm-dd') - to_date(T1.DBQSR,'yyyy-mm-dd')) / 365<0
                      THEN 0
                      ELSE (to_date(T1.DBDQR,'yyyy-mm-dd') - to_date(T1.DBQSR,'yyyy-mm-dd')) / 365
                END																					      AS ORIGINALMATURITY				 --原始期限									单位：年
								,CASE WHEN (to_date(T1.DBDQR,'yyyy-mm-dd') - to_date(p_data_dt_str,'yyyymmdd')) / 365<0
								      THEN 0
								      ELSE (to_date(T1.DBDQR,'yyyy-mm-dd') - to_date(p_data_dt_str,'yyyymmdd')) / 365
								END																					      AS  RESIDUALM								 --剩余期限
								,''																								AS	GUARANTORIRATING     --保证人内部评级
								,''																								AS	GUARANTORPD          --保证人违约概率
								,''																								AS	GROUPID              --分组编号
    FROM  RWA.RWA_WS_PRIVATE_BOND T1
    INNER JOIN  RWA.RWA_WP_DATASUPPLEMENT T2                    --数据补录表
    ON          T1.SUPPORGID=T2.ORGID
    AND         T2.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
    AND         T2.SUPPTMPLID='M-0110'
    AND         T2.SUBMITFLAG='1'
    WHERE T1.DBLX IN ('030010','030020','030030','020080','020090')
    AND   T1.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;

    /*目标表数据统计*/
    --统计插入的记录
    SELECT COUNT(1) INTO v_count FROM RWA_SMZ_GUARANTEE;
    --Dbms_output.Put_line('RWA_SMZ_GUARANTEE表当前插入的数据记录为:' || v_count1 || '条');

		--Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

		p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功'||'-'||v_count;
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '私募债-保证(PRO_RWA_SMZ_GUARANTEE)ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_SMZ_GUARANTEE;
/

