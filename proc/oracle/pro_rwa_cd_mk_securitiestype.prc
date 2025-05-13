CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_CD_MK_SECURITIESTYPE(
														p_data_dt_str IN  VARCHAR2, --数据日期
                            p_po_rtncode  OUT VARCHAR2, --返回编号
                            p_po_rtnmsg   OUT VARCHAR2  --返回描述
)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_CD_MK_SECURITIESTYPE
    实现功能:理财系统-市场风险-汇总标准法暴露表-加工证券类别字段
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :QHJIANG
    编写时间:2016-04-14
    单  位  :上海安硕信息技术股份有限公司
    源  表1  :RWA_DEV.RWA_EI_MARKETEXPOSURESTD|汇总标准法暴露表
    源  表2  :RWA_DEV.RWA_EI_ISSUERRATING|发行人评级信息汇总表
    目标表1 :RWA_DEV.RWA_EI_MARKETEXPOSURESTD|汇总标准法暴露表
    辅助表  :无
    变更记录(修改人|修改时间|修改内容):
    pxl 20190910 调整合格债券判断逻辑
    */

  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_CD_MK_SECURITIESTYPE';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前插入的记录数
  v_count INTEGER;

  BEGIN

    --DBMS_OUTPUT.PUT_LINE('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    --DBMS_OUTPUT.PUT_LINE('开始：加工【市场风险标准法风险暴露表-证券类别字段】' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    /**
     * 加工逻辑：
     * 1．01 政府证券：包含各国中央政府和中央银行发行的各类债券和短期融资工具。
     *    即:RWA_EI_MARKETEXPOSURESTD.ISSUERSUBTYPE in  0101	中国中央政府
     *                                                  0102	其他主权国家或经济实体区域的中央政府
     *                                                  0103	中国中央银行
     *                                                  0104	境外中央银行
     * 2．02 合格证券：包括
     *   （1）多边开发银行、国际清算银行和国际货币基金组织发行的债券。
     *   即:RWA_EI_MARKETEXPOSURESTD.ISSUERSUBTYPE in(0207	多边开发银行、国际清算银行和国际货币基金组织)
     *   （2）我国公共部门实体和商业银行 发行的债券。
     *   即:RWA_EI_MARKETEXPOSURESTD.ISSUERSUBTYPE in(0105 中国公共部门 0202 中国商业银行 0106 中国地方政府)
     *   （3）被至少两家合格外部评级机构评为投资级别（BB+以上）的发行主体发行的债券。
     *        当前日期 早于 7.31，评级取评级发布日期为“去年/01/01 - 当前日期”；
     *        当前日期 晚于 7.31，评级取评级发布日期为“今年/01/01 - 当前日期”
     *   即: RWA_EI_ISSUERRATING.RatingResult>BB+
     * 3 09 其他证券 剩下的默认
     */
	UPDATE RWA_DEV.RWA_EI_MARKETEXPOSURESTD T
	SET    T.SECURITIESTYPE = (CASE WHEN T.ISSUERSUBTYPE IN('0101','0102','0103','0104')
	                                THEN '01'
	                                WHEN T.ISSUERSUBTYPE='0207'
	                                THEN '02'
	                                WHEN T.ISSUERSUBTYPE IN('0105','0106','0202')
	                                THEN '02'
	                                WHEN (SELECT COUNT(DISTINCT T1.RATINGORG)
	                                      FROM RWA_DEV.RWA_EI_ISSUERRATING T1
	                                      WHERE T1.ISSUERID=T.ISSUERID
	                                      AND T1.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
                                        --先弃用如下逻辑  后续与业务讨论调整 pxl  20190910
	                                      /*AND (TO_DATE(p_data_dt_str,'YYYYMMDD')<TO_DATE(SUBSTR(p_data_dt_str,0,4)||'0731','YYYYMMDD')              --早于7月31日
	                                           AND TO_DATE(RATINGDATE,'YYYY-MM-DD')<=TO_DATE(p_data_dt_str,'YYYYMMDD')              								--评级日期在“去年/01/01 - 当前日期”
	                                           AND (TO_DATE(RATINGDATE,'YYYY-MM-DD')>=add_months(TO_DATE(SUBSTR(p_data_dt_str,0,4)||'0101','YYYYMMDD'),-12)
	                                           OR
	                                           TO_DATE(p_data_dt_str,'YYYYMMDD')>=TO_DATE(SUBSTR(p_data_dt_str,0,4)||'0731','YYYYMMDD'))             --晚于7月31日
	                                           AND TO_DATE(RATINGDATE,'YYYY-MM-DD')<=TO_DATE(p_data_dt_str,'YYYYMMDD')                              --评级日期在“今年/01/01 - 当前日期”
	                                           AND TO_DATE(RATINGDATE,'YYYY-MM-DD')>=TO_DATE(SUBSTR(p_data_dt_str,0,4)||'0101','YYYYMMDD')
	                                          )
	                                      AND T1.RATINGRESULT<'0111'*/)>=2
	                                THEN '02'
	                                ELSE '09' END)
	WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
	;
	COMMIT;

    --DBMS_OUTPUT.PUT_LINE('结束：加工【市场风险标准法风险暴露表-证券类别字段】' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    --DBMS_OUTPUT.PUT_LINE('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    SELECT COUNT(1)  INTO v_count FROM RWA_DEV.RWA_EI_MARKETEXPOSURESTD WHERE SECURITIESTYPE IS NOT NULL AND DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功' || '-' || v_count;

    --定义异常
    EXCEPTION WHEN OTHERS THEN
        --DBMS_OUTPUT.PUT_LINE('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
        ROLLBACK;
        p_po_rtncode := sqlcode;
        p_po_rtnmsg  := '加工市场风险标准法风险暴露表-证券类别字段('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;

    RETURN;

END PRO_RWA_CD_MK_SECURITIESTYPE;
/

