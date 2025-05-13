CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_ACCSUBJECTDATA(p_data_dt_str  IN  VARCHAR2, --数据日期
                                                      p_po_rtncode   OUT VARCHAR2, --返回编号
                                                      p_po_rtnmsg    OUT VARCHAR2  --返回描述
)
  /*
    存储过程名称:RWA_DEV.PRO_RWA_EI_ACCSUBJECTDATA
    实现功能:将总账余额表(加工表)信息导入科目取数表中)
    数据口径:全量
    跑批频率:月初运行
    版  本  :V1.0.0
    编写人  :QHJIANG
    编写时间:2016-06-28
    单  位  :上海安硕信息技术股份有限公司
    源  表1 :RWA_DEV.FNS_GL_BALANCE|总账余额表(加工表)
    源  表2 :RWA.CODE_LIBRARY|代码表
    目标表1 :RWA_DEV.RWA_EI_ACCSUBJECTDATA|科目取数表
    辅助表  :无
    注释    ：目前需要如下科目：
                          4001 股本 4201 库存股 4002 资本公积 4101 盈余公积
                          4102 一般风险准备 4103 本年利润 4104 利润分配 6011 利息收入
                          6021 手续费收入 6051 其他业务收入 6061 汇兑损益 6101 公允价值变动损益
                          6111 投资收益 6301 营业外收入 6402 其他业务支出 6403 营业税金及附加
                          6411 利息支出 6421 手续费支出 6602 管理费用 6701 资产减值损失
                          6711 营业外支出 6801 所得税 6901 以前年度损益调整 1811 递延所得税资产
                          1304 贷款损失准备
                          新增6112 资产处置损益-固定资产-运输设备 6113其他收益-政府补助  
                          4003其他综合收益（排除 40030102--其他综合收益-贴现资产减值准备）
                         
    变更记录(修改人|修改时间|修改内容):
    
    */

  AS
  --创建一个自治事务
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*变量定义*/
  --定义存储过程名称并赋值
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_EI_ACCSUBJECTDATA';
  --定义异常变量
  v_raise EXCEPTION;
  --定义当前遍历的记录数
  v_count INTEGER :=0;
  --定义当前插入的记录数
  v_count1 INTEGER;

  BEGIN

    --DBMS_OUTPUT.PUT_LINE('【执行 ' || v_pro_name || ' 存储过程开始 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --清除目标表中的原有记录
    --EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_EI_ACCSUBJECTDATA';
    DELETE FROM RWA_DEV.RWA_EI_ACCSUBJECTDATA WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');


    --DBMS_OUTPUT.PUT_LINE('开始：导入【科目取数表】' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    
    DECLARE
        v_insert_sql VARCHAR2(300);
    --同过游标读取需要的判断条件
    CURSOR cur_cursor IS
        SELECT ITEMNO,ITEMNAME
          FROM RWA.CODE_LIBRARY 
         WHERE CODENO='AccSubjectData' AND ISINUSE='1';
        c_cursor cur_cursor%rowtype; 
    BEGIN
    --开启游标
    OPEN cur_cursor;
    --通过循环来遍历检索游标
    --DBMS_OUTPUT.PUT_LINE('>>>>>>Insert语句开始执行中>>>>>>>');
    LOOP
        FETCH cur_cursor INTO c_cursor;
        --档游标检索完成后退出游标
        EXIT WHEN cur_cursor%notfound;
        v_count := v_count + 1;
        --生成sql
        v_insert_sql:='INSERT INTO RWA_DEV.RWA_EI_ACCSUBJECTDATA(DATADATE,DATANO,SUBJECTCODE,SUBJECTNAME) VALUES(TO_DATE('||p_data_dt_str||',''yyyyMMdd''),'||p_data_dt_str||','''||c_cursor.ITEMNO||''','''||c_cursor.ITEMNAME||''')';
        --DBMS_OUTPUT.PUT_LINE(v_insert_sql);
        --执行sql
        EXECUTE IMMEDIATE v_insert_sql;
        --COMMIT;
        --结束循环
    END LOOP;
    --DBMS_OUTPUT.PUT_LINE('遍历的总记录为：'|| v_count);
    --DBMS_OUTPUT.PUT_LINE('Insert语句已经执行结束！！！');
    --关闭游标
    CLOSE cur_cursor;
    END;
    
    --G4A 中的1.7特殊处理
    UPDATE RWA_DEV.RWA_EI_ACCSUBJECTDATA REA
       SET REA.SUBJECTBALANCE = NVL((SELECT SUM(CASE WHEN CL.ATTRIBUTE8='D-C' THEN FGB.BALANCE_D-FGB.BALANCE_C
                                                     WHEN CL.ATTRIBUTE8='C-D' THEN FGB.BALANCE_C-FGB.BALANCE_D
                                                     ELSE FGB.BALANCE_D-FGB.BALANCE_C END) AS SUBJECTBALANCE --科目余额
                                       FROM RWA_DEV.FNS_GL_BALANCE FGB
                                       LEFT JOIN RWA.CODE_LIBRARY CL 
                                         ON CL.CODENO='NewSubject'
                                        AND FGB.SUBJECT_NO=CL.ITEMNO
                                        AND CL.ISINUSE='1'
                                      WHERE FGB.DATANO = p_data_dt_str
                                        AND FGB.CURRENCY_CODE = 'RMB'
                                        AND FGB.Subject_No<>'40030102'--其他综合收益-贴现资产减值准备
                                        AND FGB.SUBJECT_NO LIKE REA.SUBJECTCODE||'%'),0)
     WHERE REA.DATANO = p_data_dt_str;
    COMMIT;
    
    --G4A-1中1 需要加上上面排除的
    UPDATE RWA_DEV.RWA_EI_ACCSUBJECTDATA REA
       SET REA.SUBJECTBALANCE=REA.SUBJECTBALANCE+(SELECT SUM(FGB.BALANCE_C-FGB.BALANCE_D)
                                                  FROM RWA_DEV.FNS_GL_BALANCE FGB
                                                  WHERE FGB.DATANO = p_data_dt_str
                                                  AND FGB.CURRENCY_CODE = 'RMB'
                                                  AND FGB.Subject_No='40030102'
                                                         )
    WHERE REA.DATANO=p_data_dt_str AND REA.subjectcode='1304';
    COMMIT;
    --DBMS_OUTPUT.PUT_LINE('结束：导入【科目取数表】' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*目标表数据统计*/
    --统计插入的记录
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_EI_ACCSUBJECTDATA;
    --DBMS_OUTPUT.PUT_LINE('RWA_DEV.RWA_EI_ACCSUBJECTDATA-科目取数表，中插入数量为：' || v_count1 || '条');

    --DBMS_OUTPUT.PUT_LINE('【执行 ' || v_pro_name || ' 存储过程结束 】:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    p_po_rtncode := '1';
    p_po_rtnmsg  := '成功'||'-'||v_count1;

    --定义异常
    EXCEPTION WHEN OTHERS THEN
        --DBMS_OUTPUT.PUT_LINE('出错了,错误代码为:'||sqlcode||';错误信息为:'||sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace);
        ROLLBACK;
        p_po_rtncode := sqlcode;
        p_po_rtnmsg  := '导入【科目取数表】('|| v_pro_name ||')ETL转换失败！'|| sqlerrm||';错误行数为:'||dbms_utility.format_error_backtrace;

    RETURN;

END PRO_RWA_EI_ACCSUBJECTDATA;
/

