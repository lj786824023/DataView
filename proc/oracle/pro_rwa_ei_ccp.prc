CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_CCP(
			 											p_data_dt_str	IN	VARCHAR2,		--数据日期
       											p_po_rtncode	OUT	VARCHAR2,		--返回编号 1 成功,0 失败
														p_po_rtnmsg		OUT	VARCHAR2		--返回描述
				)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_EI_CCP
    实现功能:中央交易对手表,插入中央交易对手表
    数据口径:全量
    跑批频率:月初
    版  本  :V1.0.0
    编写人  :CHENGANG
    编写时间:2018-04-19
    单  位	:上海安硕信息技术股份有限公司
    源  表1	:RWA_DEV.RWA_YSP_CCP|中央交易对手表
    目标表  :RWA_DEV.RWA_EI_CCP|中央交易对手表
    辅助表  :无
    变更记录(修改人|修改时间|修改内容)：
    PXL  2019/05/08  该表无分区  不需新增分区sql
    
  	*/
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_EI_CCP';
  --定义判断值变量
  v_count INTEGER;
  --定义异常变量
  v_raise EXCEPTION;

  BEGIN
    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --清除历史数据
    DELETE FROM RWA_DEV.RWA_EI_CCP WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYY/MM/DD');
    
    COMMIT;


    /*插入中央交易对手表*/
    INSERT INTO RWA_DEV.RWA_EI_CCP(
            DATADATE,          --数据日期
            DATANO,          --数据流水号
            CCPID,          --中央交易对手ID
            CCPNAME,          --中央交易对手名称
            QUALCCPFLAG,          --是否合格中央交易对手
            ORGSORTNO,          --机构排序号
            ORGID,          --所属机构ID
            ORGNAME,          --所属机构名称
            INDUSTRYID,          --所属行业代码
            INDUSTRYNAME,          --所属行业名称
            BUSINESSLINE,          --条线
            ASSETTYPE,          --资产大类
            ASSETSUBTYPE,          --资产小类
            DEFAULTFUND          --违约基金
                          )
            SELECT DATADATE     AS DATADATE, --数据日期
                   DATANO       AS DATANO, --数据流水号
                   CCPID        AS CCPID, --中央交易对手ID
                   CCPNAME      AS CCPNAME, --中央交易对手名称
                   QUALCCPFLAG  AS QUALCCPFLAG, --是否合格中央交易对手
                   ORGSORTNO    AS ORGSORTNO, --机构排序号
                   ORGID        AS ORGID, --所属机构ID
                   ORGNAME      AS ORGNAME, --所属机构名称
                   INDUSTRYID   AS INDUSTRYID, --所属行业代码
                   INDUSTRYNAME AS INDUSTRYNAME, --所属行业名称
                   BUSINESSLINE AS BUSINESSLINE, --条线
                   ASSETTYPE    AS ASSETTYPE, --资产大类
                   ASSETSUBTYPE AS ASSETSUBTYPE, --资产小类
                   DEFAULTFUND  AS DEFAULTFUND --违约基金
              FROM RWA_DEV.RWA_YSP_CCP
             WHERE DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD');
   
    COMMIT;


    /*目标表数据统计*/
    --统计插入的记录
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_EI_CCP WHERE DATANO = p_data_dt_str;
    --Dbms_output.Put_line('RWA_DEV.RWA_EI_CCP表当前插入的数据记录为:' || v_count1 || '条');

    --Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功'||'-'||v_count;
    --定义异常
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '中央交易对手表('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_EI_CCP;
/

