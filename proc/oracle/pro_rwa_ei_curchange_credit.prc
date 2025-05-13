CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_CURCHANGE_CREDIT(
                             p_data_dt_str  IN  VARCHAR2,    --�������� yyyyMMdd
                             p_po_rtncode  OUT  VARCHAR2,    --���ر�� 1 �ɹ�,0 ʧ��
                            p_po_rtnmsg    OUT  VARCHAR2    --��������
        )
  /*
    �洢��������:RWA_DEV.PRO_RWA_EI_CURCHANGE_CREDIT
    ʵ�ֹ���:���÷�����ر������ת��
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :QHJIANG
    ��дʱ��:2016-09-28
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.TMP_CURRENCY_CHANGE|���ʱ�
    Ŀ���1 :RWA_DEV.RWA_EI_ABSEXPOSURE|���ձ�¶���ܱ�
    Ŀ���2 :RWA_DEV.RWA_EI_COLLATERAL|���ܵ���ѺƷ��
    Ŀ���3 :RWA_DEV.RWA_EI_CONTRACT|��ͬ���ܱ�
    Ŀ���4 :RWA_DEV.RWA_EI_EXPOSURE|���ձ�¶���ܱ�
    Ŀ���5 :RWA_DEV.RWA_EI_GUARANTEE|���ܱ�֤��
    Ŀ���6 :RWA_DEV.RWA_EI_SFTDETAIL|������ϻع���
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'PRO_RWA_EI_CURCHANGE_CREDIT';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER := 1;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    --���ձ�¶���ܱ�
    UPDATE RWA_DEV.RWA_EI_ABSEXPOSURE TA
             --�ʲ����
    SET      TA.ASSETBALANCE = TA.ASSETBALANCE*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                    FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                    WHERE  NPQ.DATANO = TA.DATANO
                                                    AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
            --��ֵ׼��
           /* ,TA.PROVISIONS = TA.PROVISIONS*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                WHERE  NPQ.DATANO = TA.DATANO
                                                AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)*/
    WHERE   TA.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;
    --���ܵ���ѺƷ��
    UPDATE RWA_DEV.RWA_EI_COLLATERAL TA
             --��Ѻ�ܶ�
    SET      TA.COLLATERALAMOUNT = TA.COLLATERALAMOUNT*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                            FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                            WHERE  NPQ.DATANO = TA.DATANO
                                                            AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
    WHERE   TA.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;
    --��ͬ���ܱ�
    UPDATE RWA_DEV.RWA_EI_CONTRACT TA
             --��ͬ�ܽ��
    SET      TA.CONTRACTAMOUNT = TA.CONTRACTAMOUNT*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                        FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                        WHERE  NPQ.DATANO = TA.DATANO
                                                        AND    NPQ.CURRENCYCODE = TA.SETTLEMENTCURRENCY),1)
            --��ͬδ��ȡ����
            ,TA.NOTEXTRACTPART = TA.NOTEXTRACTPART*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                        FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                        WHERE  NPQ.DATANO = TA.DATANO
                                                        AND    NPQ.CURRENCYCODE = TA.SETTLEMENTCURRENCY),1)
    WHERE   TA.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;
    --���ձ�¶���ܱ�
    UPDATE RWA_DEV.RWA_EI_EXPOSURE TA
             --�����������
    SET      TA.NORMALPRINCIPAL = TA.NORMALPRINCIPAL*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                          FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                          WHERE  NPQ.DATANO = TA.DATANO
                                                          AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
            --�������
            ,TA.OVERDUEBALANCE = TA.OVERDUEBALANCE*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                        FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                        WHERE  NPQ.DATANO = TA.DATANO
                                                        AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
            --��Ӧ�����
            ,TA.NONACCRUALBALANCE = TA.NONACCRUALBALANCE*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                              FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                              WHERE  NPQ.DATANO = TA.DATANO
                                                              AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
            --�������
            ,TA.ONSHEETBALANCE = TA.ONSHEETBALANCE*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                        FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                        WHERE  NPQ.DATANO = TA.DATANO
                                                        AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
            --������Ϣ
            ,TA.NORMALINTEREST = TA.NORMALINTEREST*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                        FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                        WHERE  NPQ.DATANO = TA.DATANO
                                                        AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
            --����ǷϢ
            ,TA.ONDEBITINTEREST = TA.ONDEBITINTEREST*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                          FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                          WHERE  NPQ.DATANO = TA.DATANO
                                                          AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
            --����ǷϢ
            ,TA.OFFDEBITINTEREST = TA.OFFDEBITINTEREST*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                            FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                            WHERE  NPQ.DATANO = TA.DATANO
                                                            AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
            --Ӧ�շ���
            ,TA.EXPENSERECEIVABLE = TA.EXPENSERECEIVABLE*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                              FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                              WHERE  NPQ.DATANO = TA.DATANO
                                                              AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
            --�ʲ����
            ,TA.ASSETBALANCE = TA.ASSETBALANCE*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                    FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                    WHERE  NPQ.DATANO = TA.DATANO
                                                    AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
            --ר��׼����
           /* ,TA.SPECIALPROVISION = TA.SPECIALPROVISION*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                            FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                            WHERE  NPQ.DATANO = TA.DATANO
                                                            AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
            --һ��׼����
            ,TA.GENERALPROVISION = TA.GENERALPROVISION*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                            FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                            WHERE  NPQ.DATANO = TA.DATANO
                                                            AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
            --�ر�׼����
            ,TA.ESPECIALPROVISION = TA.ESPECIALPROVISION*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                              FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                              WHERE  NPQ.DATANO = TA.DATANO
                                                              AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)*/
            --�Ѻ������
            ,TA.WRITTENOFFAMOUNT = TA.WRITTENOFFAMOUNT*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                            FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                            WHERE  NPQ.DATANO = TA.DATANO
                                                            AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
            --��δ֧������
            ,TA.NONPAYMENTFEES = TA.NONPAYMENTFEES*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                        FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                        WHERE  NPQ.DATANO = TA.DATANO
                                                        AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
    WHERE   TA.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;
    --���ܱ�֤��
    UPDATE RWA_DEV.RWA_EI_GUARANTEE TA
             --��֤�ܶ�
    SET      TA.GUARANTEEAMOUNT = TA.GUARANTEEAMOUNT*NVL((SELECT NPQ.MIDDLEPRICE/100
                                                          FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                                          WHERE  NPQ.DATANO = TA.DATANO
                                                          AND    NPQ.CURRENCYCODE = TA.CURRENCY),1)
    WHERE   TA.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
    ;
    COMMIT;
    --������ϻع���
    UPDATE RWA_DEV.RWA_EI_SFTDETAIL TA
             --�ʲ����
    SET      TA.ASSETBALANCE = TA.ASSETBALANCE*NVL((SELECT NPQ.MIDDLEPRICE/100
                                            FROM   RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                                            WHERE  NPQ.DATANO = TA.DATANO
                                            AND    NPQ.CURRENCYCODE = TA.ASSETCURRENCY),1)
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
          p_po_rtnmsg  := '���÷�����ر������ת��('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_EI_CURCHANGE_CREDIT;
/

