CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_FESPOTPOSITION(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期 yyyyMMdd
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_EI_FESPOTPOSITION
    实现功能:汇总外汇现货头寸表，插入所有外汇现货头寸信息
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :SHUXD
    编写时间:2016-07-07
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.RWA_WH_FESPOTPOSITION|国结系统外汇现货头寸表
    目标表  :RWA_DEV.RWA_EI_FESPOTPOSITION|汇总外汇现货头寸表
    变更记录(修改人|修改时间|修改内容):
    */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_EI_FESPOTPOSITION';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    BEGIN
    --删除不存在的分区，如果此存储过程是第一次跑抛出异常，可以忽略
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_FESPOTPOSITION DROP PARTITION FESPOTPOSITION' || p_data_dt_str;

    COMMIT;

    EXCEPTION
      WHEN OTHERS THEN
        IF (SQLCODE <> '-2149') THEN
          --首次分区truncate会出现2149异常
         p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '汇总外汇现货头寸表('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
        END IF;
    END;

    --新增一个当前日期下的分区
    EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_FESPOTPOSITION ADD PARTITION FESPOTPOSITION' || p_data_dt_str || ' VALUES(TO_DATE(' || p_data_dt_str || ',''YYYYMMDD''))';

    COMMIT;

    /*插入外汇现货头寸信息*/
    INSERT INTO RWA_DEV.RWA_EI_FESPOTPOSITION(
                DATADATE                               --数据日期
                ,POSITIONID                            --头寸ID
                ,ACCORGID                              --账务机构ID
                ,INSTRUMENTSTYPE                       --金融工具类型
                ,ACCSUBJECTS                           --会计科目
                ,BOOKTYPE                              --账户类别
                ,STRUCTURALEXPOFLAG                    --是否结构性敞口
                ,CURRENCY                              --币种
                ,POSITION                              --头寸
                ,POSITIONTYPE                          --头寸属性

    )
    SELECT
                DATADATE                                     AS DATADATE                 --数据日期
                ,POSITIONID													         AS POSITIONID               --头寸ID
                ,ACCORGID                   				 		     AS ACCORGID                 --账务机构ID    										补录
                ,INSTRUMENTSTYPE			                       AS INSTRUMENTSTYPE          --金融工具类型											默认为 ‘0501’ （外汇现货）
                ,ACCSUBJECTS    	                           AS ACCSUBJECTS              --会计科目      										补录
                ,BOOKTYPE					                           AS BOOKTYPE                 --账户类别      										通过会计科目映射，若科目号为1101-交易性金融资产为交易账户(02)，其他默认为银行账户(01)
                ,STRUCTURALEXPOFLAG			 						         AS STRUCTURALEXPOFLAG       --是否结构性敞口										通过会计科目映射，若科目号为4001-股本、科目10030102-存放中央银行款项-存放中央银行法定准备金为结构性敞口(1)，其他默认为否(0)
                ,CURRENCY			 						               		 AS CURRENCY                 --币种          										买入金额非空则用买入币种，否则用卖出币种
                ,POSITION                            				 AS POSITION                 --头寸          										买入金额非空则用买入金额，否则用卖出金额
                ,POSITIONTYPE		                             AS POSITIONTYPE             --头寸属性      										买入金额非空则为多头(01)，否则为空头(02)

    FROM				RWA_DEV.RWA_WH_FESPOTPOSITION
    WHERE				DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
	  ;

    COMMIT;

    --整理表信息
    --dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_FESPOTPOSITION',cascade => true);
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_FESPOTPOSITION',partname => 'FESPOTPOSITION'||p_data_dt_str,granularity => 'PARTITION',cascade => true);


    /*目标表数据统计*/
    --统计插入的记录数
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_FESPOTPOSITION WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_FESPOTPOSITION表当前插入的国结系统-外汇现货(市场风险)-外货现货头寸记录为: ' || v_count || ' 条');



    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
	  p_po_rtnmsg  := '成功'||'-'||v_count;
		--定义异常
		EXCEPTION
    WHEN OTHERS THEN
				 --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
       	 ROLLBACK;
   			 p_po_rtncode := sqlcode;
   			 p_po_rtnmsg  := '外汇现货头寸('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_EI_FESPOTPOSITION;
/

