CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_GUARANTEE(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_EI_GUARANTEE
    实现功能:汇总保证表,插入所有保证信息
    数据口径:全量
    跑批频率:月初
    版  本  :V1.0.0
    编写人  :SHUXD
    编写时间:2015-06-01
    单  位	:上海安硕信息技术股份有限公司
    源  表1	:RWA_DEV.RWA_XD_GUARANTEE|信贷保证表
    源  表2	:RWA_DEV.RWA_LC_GUARANTEE|理财保证表
    源  表3	:RWA_DEV.RWA_TZ_GUARANTEE|投资保证表
    源  表4	:RWA_DEV.RWA_ABS_ISSURE_GUARANTEE|资产证券化发行机构保证表
    目标表1	:RWA_DEV.RWA_EI_GUARANTEE|汇总保证表
    变更记录(修改人|修改时间|修改内容):
  */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_XD_GUARANTEE';
  --定义判断值变量
  v_count INTEGER;
  --定义异常变量
  v_raise EXCEPTION;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    BEGIN
    --删除不存在的分区，如果此存储过程是第一次跑抛出异常，可以忽略
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_GUARANTEE DROP PARTITION GUARANTEE' || p_data_dt_str;

    COMMIT;

    EXCEPTION
      WHEN OTHERS THEN
        IF (SQLCODE <> '-2149') THEN
          --首次分区truncate会出现2149异常
         p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '汇总保证表('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
        END IF;
    END;

    --新增一个当前日期下的分区
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_GUARANTEE ADD PARTITION GUARANTEE' || p_data_dt_str || ' VALUES(TO_DATE('|| p_data_dt_str || ',''YYYYMMDD''))';

    COMMIT;

    /*插入信贷的保证信息*/
    INSERT INTO RWA_DEV.RWA_EI_GUARANTEE(
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
         				 DATADATE          										AS  DATADATE          		 --数据日期
         				,DATANO            						 		    AS	DATANO               --数据流水号
         				,GUARANTEEID       										AS	GUARANTEEID          --保证ID
								,SSYSID            									  AS	SSYSID               --源系统ID
								,GUARANTEECONID    										AS	GUARANTEECONID       --保证合同ID
								,GUARANTORID       										AS	GUARANTORID          --保证人ID
								,CREDITRISKDATATYPE								    AS	CREDITRISKDATATYPE   --信用风险数据类型
								,GUARANTEEWAY      										AS	GUARANTEEWAY       	 --担保方式
								,QUALFLAGSTD       										AS	QUALFLAGSTD          --权重法合格标识
								,QUALFLAGFIRB      										AS	QUALFLAGFIRB         --内评初级法合格标识
								,GUARANTEETYPESTD  										AS	GUARANTEETYPESTD     --权重法保证类型
								,GUARANTORSDVSSTD  										AS	GUARANTORSDVSSTD     --权重法保证人细分
								,GUARANTEETYPEIRB  										AS	GUARANTEETYPEIRB     --内评法保证类型
								,GUARANTEEAMOUNT   										AS	GUARANTEEAMOUNT      --保证总额
								,CURRENCY          					  				AS	CURRENCY             --币种
								,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
								         										          AS	STARTDATE            --起始日期
								,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
								           										        AS	DUEDATE              --到期日期
								,ORIGINALMATURITY                     AS  ORIGINALMATURITY   	 --原始期限
								,RESIDUALM                            AS	RESIDUALM            --剩余期限
								,GUARANTORIRATING  									  AS	GUARANTORIRATING     --保证人内部评级
								,GUARANTORPD       									  AS	GUARANTORPD          --保证人违约概率
								,GROUPID           										AS	GROUPID              --分组编号
    FROM 				RWA_DEV.RWA_XD_GUARANTEE
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;


    /*插入理财的保证信息*/
    INSERT INTO RWA_DEV.RWA_EI_GUARANTEE(
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
         				 DATADATE          										AS  DATADATE          		 --数据日期
         				,DATANO            						 		    AS	DATANO               --数据流水号
         				,GUARANTEEID       										AS	GUARANTEEID          --保证ID
								,SSYSID            									  AS	SSYSID               --源系统ID
								,GUARANTEECONID    										AS	GUARANTEECONID       --保证合同ID
								,GUARANTORID       										AS	GUARANTORID          --保证人ID
								,CREDITRISKDATATYPE								    AS	CREDITRISKDATATYPE   --信用风险数据类型
								,GUARANTEEWAY      										AS	GUARANTEEWAY       	 --担保方式
								,QUALFLAGSTD       										AS	QUALFLAGSTD          --权重法合格标识
								,QUALFLAGFIRB      										AS	QUALFLAGFIRB         --内评初级法合格标识
								,GUARANTEETYPESTD  										AS	GUARANTEETYPESTD     --权重法保证类型
								,GUARANTORSDVSSTD  										AS	GUARANTORSDVSSTD     --权重法保证人细分
								,GUARANTEETYPEIRB  										AS	GUARANTEETYPEIRB     --内评法保证类型
								,GUARANTEEAMOUNT   										AS	GUARANTEEAMOUNT      --保证总额
								,CURRENCY          					  				AS	CURRENCY             --币种
								,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
								         										          AS	STARTDATE            --起始日期
								,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
								           										        AS	DUEDATE              --到期日期
								,ORIGINALMATURITY                     AS  ORIGINALMATURITY   	 --原始期限
								,RESIDUALM                            AS	RESIDUALM            --剩余期限
								,GUARANTORIRATING  									  AS	GUARANTORIRATING     --保证人内部评级
								,GUARANTORPD       									  AS	GUARANTORPD          --保证人违约概率
								,GROUPID           										AS	GROUPID              --分组编号
    FROM 				RWA_DEV.RWA_LC_GUARANTEE
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;


    /*插入投资的保证信息*/
    INSERT INTO RWA_DEV.RWA_EI_GUARANTEE(
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
         				 DATADATE          										AS  DATADATE          		 --数据日期
         				,DATANO            						 		    AS	DATANO               --数据流水号
         				,GUARANTEEID       										AS	GUARANTEEID          --保证ID
								,SSYSID            									  AS	SSYSID               --源系统ID
								,GUARANTEECONID    										AS	GUARANTEECONID       --保证合同ID
								,GUARANTORID       										AS	GUARANTORID          --保证人ID
								,CREDITRISKDATATYPE								    AS	CREDITRISKDATATYPE   --信用风险数据类型
								,GUARANTEEWAY      										AS	GUARANTEEWAY       	 --担保方式
								,QUALFLAGSTD       										AS	QUALFLAGSTD          --权重法合格标识
								,QUALFLAGFIRB      										AS	QUALFLAGFIRB         --内评初级法合格标识
								,GUARANTEETYPESTD  										AS	GUARANTEETYPESTD     --权重法保证类型
								,GUARANTORSDVSSTD  										AS	GUARANTORSDVSSTD     --权重法保证人细分
								,GUARANTEETYPEIRB  										AS	GUARANTEETYPEIRB     --内评法保证类型
								,GUARANTEEAMOUNT   										AS	GUARANTEEAMOUNT      --保证总额
								,CURRENCY          					  				AS	CURRENCY             --币种
								,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
								         										          AS	STARTDATE            --起始日期
								,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
								           										        AS	DUEDATE              --到期日期
								,ORIGINALMATURITY                     AS  ORIGINALMATURITY   	 --原始期限
								,RESIDUALM                            AS	RESIDUALM            --剩余期限
								,GUARANTORIRATING  									  AS	GUARANTORIRATING     --保证人内部评级
								,GUARANTORPD       									  AS	GUARANTORPD          --保证人违约概率
								,GROUPID           										AS	GROUPID              --分组编号
    FROM 				RWA_DEV.RWA_TZ_GUARANTEE
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;


    /*插入资产证券化发行机构的保证信息*/
    INSERT INTO RWA_DEV.RWA_EI_GUARANTEE(
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
         				 DATADATE          										AS  DATADATE          		 --数据日期
         				,DATANO            						 		    AS	DATANO               --数据流水号
         				,GUARANTEEID       										AS	GUARANTEEID          --保证ID
								,SSYSID            									  AS	SSYSID               --源系统ID
								,GUARANTEECONID    										AS	GUARANTEECONID       --保证合同ID
								,GUARANTORID       										AS	GUARANTORID          --保证人ID
								,CREDITRISKDATATYPE								    AS	CREDITRISKDATATYPE   --信用风险数据类型
								,GUARANTEEWAY      										AS	GUARANTEEWAY       	 --担保方式
								,QUALFLAGSTD       										AS	QUALFLAGSTD          --权重法合格标识
								,QUALFLAGFIRB      										AS	QUALFLAGFIRB         --内评初级法合格标识
								,GUARANTEETYPESTD  										AS	GUARANTEETYPESTD     --权重法保证类型
								,GUARANTORSDVSSTD  										AS	GUARANTORSDVSSTD     --权重法保证人细分
								,GUARANTEETYPEIRB  										AS	GUARANTEETYPEIRB     --内评法保证类型
								,GUARANTEEAMOUNT   										AS	GUARANTEEAMOUNT      --保证总额
								,CURRENCY          					  				AS	CURRENCY             --币种
								,TO_CHAR(TO_DATE(STARTDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
								         										          AS	STARTDATE            --起始日期
								,TO_CHAR(TO_DATE(DUEDATE,'YYYY-MM-DD'),'YYYY-MM-DD')
								           										        AS	DUEDATE              --到期日期
								,ORIGINALMATURITY                     AS  ORIGINALMATURITY   	 --原始期限
								,RESIDUALM                            AS	RESIDUALM            --剩余期限
								,GUARANTORIRATING  									  AS	GUARANTORIRATING     --保证人内部评级
								,GUARANTORPD       									  AS	GUARANTORPD          --保证人违约概率
								,GROUPID           										AS	GROUPID              --分组编号
    FROM 				RWA_DEV.RWA_ABS_ISSURE_GUARANTEE
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;
    
    
    
    ----------外币企业债 徽商银行担保 by zbwang----
    insert into RWA_EI_GUARANTEE
      (DATADATE,
       DATANO,
       GUARANTEEID,
       SSYSID,
       GUARANTEECONID,
       GUARANTORID,
       CREDITRISKDATATYPE,
       GUARANTEEWAY,
       QUALFLAGSTD,
       QUALFLAGFIRB,
       GUARANTEETYPESTD,
       GUARANTORSDVSSTD,
       GUARANTEETYPEIRB,
       GUARANTEEAMOUNT,
       CURRENCY,
       STARTDATE,
       DUEDATE,
       ORIGINALMATURITY,
       RESIDUALM,
       GUARANTORIRATING,
       GUARANTORPD,
       GROUPID)
    select 
      to_date(p_data_dt_str,'yyyymmdd'),
       p_data_dt_str,
       'B201801105279',
       'TZ',
       'B201801105279',
       'ty2017011200000051', --保证人ID
       '01',
       '010',    ---担保方式 保证
       '1',      --权重法合格标示
       '1',      --内评合格标示
       '020101', --权重法保证类型
       '06',
       '020201', --内评法保证类型
       a.Onsheetbalance, ---本金金额 因为本金余额已经加上公允价值，应急利息参与勾稽
       a.currency,
       a.startdate,
       a.duedate,
       a.originalmaturity,
       a.residualm,
       null,
       null,
       null
       from rwa_ei_exposure a where exposureid='B201801105279' 
       and datano=p_data_dt_str;
       
      
       ------外币企业债  徽商银行保证
       insert into RWA_EI_CMRELEVENCE
       (DATADATE,
        DATANO,
        CONTRACTID,
        MITIGATIONID,
        MITIGCATEGORY,
        SGUARCONTRACTID,
        GROUPID)
        values
           (to_date(p_data_dt_str, 'yyyymmdd'),
            p_data_dt_str,
           'B201801105279',
           'B201801105279',
           '02',
           'B201801105279',
            NULL);
       commit;


       
    --整理表信息
    --dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_GUARANTEE',cascade => true);
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_GUARANTEE',partname => 'GUARANTEE'||p_data_dt_str,granularity => 'PARTITION',cascade => true);


    /*目标表数据统计*/
    --统计插入的记录
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_GUARANTEE WHERE DATANO = p_data_dt_str;
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_GUARANTEE表当前插入的数据记录为:' || v_count1 || '条');


		--Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

		p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功'||'-'||v_count;
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '汇总保证(RWA_DEV.PRO_RWA_EI_GUARANTEE)ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_EI_GUARANTEE;
/

