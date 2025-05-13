CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_OTOCPREMIUM(p_data_dt_str  IN  VARCHAR2, --数据日期
                                                      p_po_rtncode   OUT VARCHAR2, --返回编号
                                                      p_po_rtnmsg    OUT VARCHAR2  --返回描述
)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_EI_OTOCPREMIUM
    实现功能:将总账余额表(加工表)信息导入其他一级资本工具及其溢价表中
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :LISY
    编写时间:2018-01-25
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.FNS_GL_BALANCE|总账余额表(加工表)
    源  表2 :RWA.CODE_LIBRARY|代码表
    目标表1 :RWA_DEV.RWA_EI_OTOCPREMIUM|其他一级资本工具及其溢价表
    辅助表  :无
    注释    ：目前需要如下科目：
                          44010101 其他一级资本-优先股及其溢价
    变更记录(修改人|修改时间|修改内容):

    */

  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_EI_OTOCPREMIUM';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前遍历的记录数
  v_count INTEGER :=0;
  --定义当前插入的记录数
  v_count1 INTEGER;

  BEGIN

    --DBMS_OUTPUT.PUT_LINE('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --清除目标表中的原有记录
    --EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_EI_OTOCPREMIUM';
    DELETE FROM RWA_DEV.RWA_EI_OTOCPREMIUM WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');


    --DBMS_OUTPUT.PUT_LINE('开始：导入【科目取数表】' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));


	INSERT INTO RWA_DEV.RWA_EI_OTOCPREMIUM(
				DATADATE          		--数据日期
				,DATANO            		--数据期次
				,SERIALNO          		--流水号
				,PREFERREDPREMIUM  		--优先股及其溢价
				,OTHERTOOLSPREMIUM 		--其他工具及其溢价
				,INPUTUSERID       		--登记人ID
				,INPUTORGID        		--登记机构ID
				,INPUTTIME         		--登记时间
				,UPDATEUSERID      		--更新人ID
				,UPDATEORGID       		--更新机构ID
				,UPDATETIME        		--更新时间
	)
    WITH TMP_44010101 AS (
		SELECT SUM(BALANCE_C-BALANCE_D) AS BAL FROM RWA_DEV.FNS_GL_BALANCE WHERE SUBJECT_NO = '44010101' AND CURRENCY_CODE = 'RMB' AND DATANO = p_data_dt_str
	)
    SELECT
				TO_DATE(p_data_dt_str,'YYYYMMDD')				AS DATADATE          		--数据日期
				,p_data_dt_str									AS DATANO            		--数据期次
				,p_data_dt_str || 'OTO01'						AS SERIALNO          		--流水号
				,T1.BAL											AS PREFERREDPREMIUM  		--优先股及其溢价
				,0												AS OTHERTOOLSPREMIUM 		--其他工具及其溢价
				,'SYSTEM'										AS INPUTUSERID       		--登记人ID				默认 SYSTEM
				,'01000000'										AS INPUTORGID        		--登记机构ID			默认 01000000-重庆银行/总行部室
				,TO_CHAR(SYSDATE,'YYYY/MM/DD HH24:MI:SS')		AS INPUTTIME         		--登记时间
				,''												AS UPDATEUSERID      		--更新人ID
				,''												AS UPDATEORGID       		--更新机构ID
				,''												AS UPDATETIME        		--更新时间
	FROM		TMP_44010101 T1
	;

    COMMIT;
    --DBMS_OUTPUT.PUT_LINE('结束：导入【科目取数表】' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*目标表数据统计*/
    --统计插入的记录
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_EI_OTOCPREMIUM WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --DBMS_OUTPUT.PUT_LINE('RWA_DEV.RWA_EI_OTOCPREMIUM表，中插入数量为：' || v_count1 || '条');

    --DBMS_OUTPUT.PUT_LINE('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功'||'-'||v_count1;

    --定义异常
    EXCEPTION WHEN OTHERS THEN
        --DBMS_OUTPUT.PUT_LINE('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
        ROLLBACK;
        p_po_rtncode := sqlcode;
        p_po_rtnmsg  := '导入【其他一级资本工具及其溢价表】('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;

    RETURN;

END PRO_RWA_EI_OTOCPREMIUM;
/

