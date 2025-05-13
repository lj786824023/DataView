CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_CURCHANGE_MARKET(
                             p_data_dt_str  IN  VARCHAR2,    --�������� yyyyMMdd
                             p_po_rtncode  OUT  VARCHAR2,    --���ر�� 1 �ɹ�,0 ʧ��
                            p_po_rtnmsg    OUT  VARCHAR2    --��������
        )
  /*
    �洢��������:RWA_DEV.PRO_RWA_EI_CURCHANGE_MARKET
    ʵ�ֹ���:�г�������ر������ת��
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :QHJIANG
    ��дʱ��:2016-09-28
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.TMP_CURRENCY_CHANGE|���ʱ�
    Ŀ���1 :RWA_DEV.RWA_EI_BONDINFO|����ծȯ��Ϣ��
    Ŀ���2 :RWA_DEV.RWA_EI_FESPOTPOSITION|��������ֻ�ͷ���
    Ŀ���3 :RWA_DEV.RWA_EI_MARKETEXPOSURESTD|���ܱ�׼����¶��
    Ŀ���4 :RWA_DEV.RWA_EI_TRADBONDPOSITION|����ծȯͷ����ܱ�
    Ŀ���5 :RWA_DEV.RWA_EI_FEFORWARDSSWAP|���Զ�ڵ��ڻ��ܱ�
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    XLP  20191206  ����RWA_EI_FESPOTPOSITION����ֻ�ͷ��� ���е������Ŀ 6��ͷ�������Ǳ����ۺ������������  ����������������
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_EI_CURCHANGE_MARKET';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER := 1;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    --����ծȯ��Ϣ��
    UPDATE RWA_DEV.RWA_EI_BONDINFO TA
             --���
    SET      TA.DENOMINATION = TA.DENOMINATION*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                    FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                    WHERE  NPQ.DATANO = TO_CHAR(TA.DATADATE,'yyyyMMdd')
                                                    AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
    WHERE   TA.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;
    
    --��������ֻ�ͷ���
    UPDATE RWA_DEV.RWA_EI_FESPOTPOSITION TA
             --ͷ��
    SET      TA.POSITION = TA.POSITION*NVL((SELECT NPQ.MIDDLEPRICE/100
                                            FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                            WHERE  NPQ.DATANO = TO_CHAR(TA.DATADATE,'yyyyMMdd')
                                            AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
    WHERE   TA.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
      AND   TA.ACCSUBJECTS NOT LIKE '6%' --�������������Ŀ
    ;
    COMMIT;
    
    --���ܱ�׼����¶��
    UPDATE RWA_DEV.RWA_EI_MARKETEXPOSURESTD TA
             --ͷ��
    SET      TA.POSITION = TA.POSITION*NVL((SELECT NPQ.MIDDLEPRICE/100
                                            FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                            WHERE  NPQ.DATANO = TO_CHAR(TA.DATADATE,'yyyyMMdd')
                                            AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
    WHERE   TA.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;
    
    --����ծȯͷ����ܱ�
    UPDATE RWA_DEV.RWA_EI_TRADBONDPOSITION TA
             --���
    SET      TA.DENOMINATION = TA.DENOMINATION*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                    FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                    WHERE  NPQ.DATANO = TO_CHAR(TA.DATADATE,'yyyyMMdd')
                                                    AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
            --��ֵ
            ,TA.MARKETVALUE = TA.MARKETVALUE*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                  FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                  WHERE  NPQ.DATANO = TO_CHAR(TA.DATADATE,'yyyyMMdd')
                                                  AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
            --�����
            ,TA.DISCOUNTPREMIUM = TA.DISCOUNTPREMIUM*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                          FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                          WHERE  NPQ.DATANO = TO_CHAR(TA.DATADATE,'yyyyMMdd')
                                                          AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
            --���ʼ�ֵ�䶯
            ,TA.FAIRVALUECHANGE = TA.FAIRVALUECHANGE*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                          FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                          WHERE  NPQ.DATANO = TO_CHAR(TA.DATADATE,'yyyyMMdd')
                                                          AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
            --�������
            ,TA.BOOKBALANCE = TA.BOOKBALANCE*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                  FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                  WHERE  NPQ.DATANO = TO_CHAR(TA.DATADATE,'yyyyMMdd')
                                                  AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
    WHERE   TA.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;
    
    --Զ�ڵ��ڻ��ܱ�
    UPDATE RWA_DEV.RWA_EI_FEFORWARDSSWAP TA
             --������
    SET      TA.BUYAMOUNT = TA.BUYAMOUNT*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                    FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                    WHERE  NPQ.DATANO = TO_CHAR(TA.DATADATE,'yyyyMMdd')
                                                    AND    NPQ.CURRENCYCODE = TA.BUYCURRENCY),1)
            --�������
            ,TA.SELLAMOUNT = TA.SELLAMOUNT*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                  FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                  WHERE  NPQ.DATANO = TO_CHAR(TA.DATADATE,'yyyyMMdd')
                                                  AND    NPQ.CURRENCYCODE = TA.SELLCURRENCY),1)

    WHERE   TA.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�'||'-'||v_count;
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '�г�������ر������ת��('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_EI_CURCHANGE_MARKET;
/

