CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EL_RISK(
                                            p_data_dt_str   IN    VARCHAR2,   --数据日期
                                            p_po_rtncode    OUT   VARCHAR2,   --返回编号
                                            p_po_rtnmsg     OUT   VARCHAR2    --返回描述
                                           )
  /*
    存储过程名称:rwa_dev.pro_rwa_el_risk
    实现功能:汇总表-风险数据记录表,表结构为风险数据记录表
    数据口径:增量
    跑批频率:月末
    版  本  :V1.0.0
    编写人  :qpzhong
    编写时间:2016-10-11
    单  位   :上海安硕信息技术股份有限公司
    源  表   :无
    目标表   :rwa.rwa_el_risk|风险数据记录表
    辅助表   :无
    变更记录(修改人|修改时间|修改内容)：
  */
  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'rwa_dev.pro_rwa_el_risk';
  --定义判断值变量
  v_count INTEGER;
  --定义异常变量
  v_raise EXCEPTION;

  v_cnt integer;

  BEGIN
    Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));


    SELECT COUNT(1) INTO v_cnt from RWA.rwa_el_risk where datadate = TO_DATE(p_data_dt_str,'YYYY-MM-DD');

    IF V_CNT = 0 THEN

    --2.将满足条件的数据从源表插入到目标表中
    /*插入目标表*/
    INSERT INTO rwa.rwa_el_risk(
                       datadate                                    --数据日期
                       ,datano                                     --数据流水号
                       ,creditvalidatestate                        --信用风险校验状态
                       ,creditexpodeterflag                        --信用风险暴露认定标识
                       ,creditexpodeteruserid                      --信用风险暴露认定人ID
                       ,creditexpodeterorgid                       --信用风险暴露认定机构ID
                       ,creditexpodetertime                        --信用风险暴露认定时间
                       ,creditconfirmflag                          --信用风险确认标识
                       ,creditconfirmuserid                        --信用风险确认人ID
                       ,creditconfirmorgid                         --信用风险确认机构ID
                       ,creditconfirmtime                          --信用风险确认时间
                       ,creditgroupflag                            --信用风险分组标识
                       ,marketvalidatestate                        --市场风险校验状态
                       ,marketconfirmflag                          --市场风险确认标识
                       ,marketconfirmuserid                        --市场风险确认人ID
                       ,marketconfirmorgid                         --市场风险确认机构ID
                       ,marketconfirmtime                          --市场风险确认时间
                       ,operatevalidatestate                       --操作风险校验状态
                       ,operateconfirmflag                         --操作风险确认标识
                       ,operateconfirmuserid                       --操作风险确认人ID
                       ,operateconfirmorgid                        --操作风险确认机构ID
                       ,operateconfirmtime                         --操作风险确认时间
                       ,capitalconfirmflag                         --可用资本确认标识
                       ,capitalconfirmuserid                       --可用资本确认人ID
                       ,capitalconfirmorgid                        --可用资本确认机构ID
                       ,capitalconfirmtime                         --可用资本确认时间
                       ,consolidateconfirmflag                     --子公司报表上传确认
                       ,consolidateconfirmuserid                   --子公司报表上传确认人ID
                       ,consolidateconfirmorgid                    --子公司报表上传确认机构
                       ,consolidateconfirmtime                     --子公司报表上传确认时间
      )
      SELECT
                       TO_DATE(p_data_dt_str,'YYYYMMDD')                      AS datadate                                             --数据日期
                       ,p_data_dt_str                                         AS datano                                     --数据流水号
                       ,''                                                    AS creditvalidatestate                        --信用风险校验状态
                       ,'0'                                                   AS creditexpodeterflag                        --信用风险暴露认定标识               (默认为否,1是0否)
                       ,''                                                    AS creditexpodeteruserid                      --信用风险暴露认定人ID
                       ,''                                                    AS creditexpodeterorgid                       --信用风险暴露认定机构ID
                       ,''                                                    AS creditexpodetertime                        --信用风险暴露认定时间
                       ,'0'                                                   AS creditconfirmflag                          --信用风险确认标识                     (默认为否,1是0否)
                       ,''                                                    AS creditconfirmuserid                        --信用风险确认人ID
                       ,''                                                    AS creditconfirmorgid                         --信用风险确认机构ID
                       ,''                                                    AS creditconfirmtime                          --信用风险确认时间
                       ,''                                                    AS creditgroupflag                            --信用风险分组标识
                       ,''                                                    AS marketvalidatestate                        --市场风险校验状态
                       ,'0'                                                   AS marketconfirmflag                          --市场风险确认标识                     (默认为否,1是0否)
                       ,''                                                    AS marketconfirmuserid                        --市场风险确认人ID
                       ,''                                                    AS marketconfirmorgid                         --市场风险确认机构ID
                       ,''                                                    AS marketconfirmtime                          --市场风险确认时间
                       ,''                                                    AS operatevalidatestate                       --操作风险校验状态
                       ,'0'                                                   AS operateconfirmflag                         --操作风险确认标识                     (默认为否,1是0否)
                       ,''                                                    AS operateconfirmuserid                       --操作风险确认人ID
                       ,''                                                    AS operateconfirmorgid                        --操作风险确认机构ID
                       ,''                                                    AS operateconfirmtime                         --操作风险确认时间
                       ,'0'                                                   AS capitalconfirmflag                         --可用资本确认标识                     (默认为否,1是0否)
                       ,''                                                    AS capitalconfirmuserid                       --可用资本确认人ID
                       ,''                                                    AS capitalconfirmorgid                        --可用资本确认机构ID
                       ,''                                                    AS capitalconfirmtime                         --可用资本确认时间
                       ,'0'                                                   AS consolidateconfirmflag                     --子公司报表上传确认
                       ,''                                                    AS consolidateconfirmuserid                   --子公司报表上传确认人ID
                       ,''                                                    AS consolidateconfirmorgid                    --子公司报表上传确认机构
                       ,''                                                    AS consolidateconfirmtime                     --子公司报表上传确认时间
      FROM DUAL
      ;
      COMMIT;

      END IF;
    /*目标表数据统计*/
    --统计插入的记录
    SELECT COUNT(1) INTO v_count FROM rwa.rwa_el_risk   WHERE   datadate = TO_DATE(p_data_dt_str,'YYYYMMDD');
    Dbms_output.Put_line('rwa.rwa_el_risk表当前插入的数据记录为:' || v_count || '条');
    Dbms_output.Put_line('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

      p_po_rtncode := '1';
      p_po_rtnmsg  := '成功';
      --定义异常
      EXCEPTION
      WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
      ROLLBACK;
      p_po_rtncode := sqlcode;
      p_po_rtnmsg  := '汇总表-风险数据记录表(RWA_DEV.PRO_RWA_EL_RISK)ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;
      RETURN;

END pro_rwa_el_risk;
/

