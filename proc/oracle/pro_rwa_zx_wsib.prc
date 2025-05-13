CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ZX_WSIB(
                            p_data_dt_str IN  VARCHAR2,   --�������� yyyyMMdd
                            p_po_rtncode  OUT VARCHAR2,   --���ر�� 1 �ɹ�,0 ʧ��
                            p_po_rtnmsg   OUT VARCHAR2    --��������
        )
  /*
    �洢��������:RWA_DEV.PRO_RWA_ZX_WSIB
    ʵ�ֹ���:����ϵͳ-ֱ�����е��-��¼�̵�(������Դ����ϵͳ��ҵ�������Ϣȫ������RWAֱ�����е�¼�̵ױ���)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2016-06-06
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.CBS_IAC|ͨ�÷ֻ���
    Դ  ��2 :RWA.RWA_WS_DSBANK_ADV|ֱ�����е�¼��
    Դ  ��3 :RWA.RWA_WP_SUPPTASKORG|��¼��������ַ����ñ�
    Դ  ��4 :RWA.RWA_WP_SUPPTASK|��¼���񷢲���
    Ŀ���  :RWA.RWA_WSIB_DSBANK_ADV|ֱ�����е���̵ױ�
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ZX_WSIB';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    --���ֱ�����е���̵ױ�
    --EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA.RWA_WSIB_DSBANK_ADV';
    DELETE FROM RWA.RWA_WSIB_DSBANK_ADV WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    COMMIT;

    --2.���������������ݴ�Դ����뵽Ŀ�����
    INSERT INTO RWA.RWA_WSIB_DSBANK_ADV(
                DATADATE                               --��������
                ,ORGID                                 --����ID
                ,IACAC_NO                              --�˺�
                ,IACCURBAL                             --���
                ,IACCRTDAT                             --��ʼ��
                ,IACDLTDAT                             --������
                ,BELONGORGCODE                         --ҵ����������
                ,CLIENTNAME                            --���׶�������
                ,ORGANIZATIONCODE                      --���׶�����֯��������
                ,COUNTRYCODE                           --���׶���ע����Ҵ���
                ,INDUSTRYID                            --���׶���������ҵ����
                ,CLIENTCATEGORY                        --���׶��ֿͻ�����
    )
    WITH TMP_SUPPORG AS (
                SELECT T1.ORGID AS ORGID
                       ,CASE WHEN T3.ORGLEVEL > 2 THEN T4.SORTNO ELSE T3.SORTNO END AS SORTNO
                  FROM RWA.RWA_WP_SUPPTASKORG T1
            INNER JOIN RWA.RWA_WP_SUPPTASK T2
                    ON T1.SUPPTASKID = T2.SUPPTASKID
                   AND T2.ENABLEFLAG = '01'
             LEFT JOIN RWA.ORG_INFO T3
                    ON T1.ORGID = T3.ORGID
             LEFT JOIN RWA.ORG_INFO T4
                    ON T3.BELONGORGID = T4.ORGID
                 WHERE T1.SUPPTMPLID = 'M-0190'
              ORDER BY T3.SORTNO
    )
    SELECT
                DATADATE                               --��������
                ,ORGID                                 --����ID
                ,IACAC_NO                              --�˺�
                ,IACCURBAL                             --���
                ,IACCRTDAT                             --��ʼ��
                ,IACDLTDAT                             --������
                ,BELONGORGCODE                         --ҵ����������
                ,CLIENTNAME                            --���׶�������
                ,ORGANIZATIONCODE                      --���׶�����֯��������
                ,COUNTRYCODE                           --���׶���ע����Ҵ���
                ,INDUSTRYID                            --���׶���������ҵ����
                ,CLIENTCATEGORY                        --���׶��ֿͻ�����
    FROM (
    SELECT
                TO_DATE(p_data_dt_str,'YYYYMMDD')            AS DATADATE                 --��������
                ,NVL(T4.ORGID,(SELECT ORGID FROM TMP_SUPPORG WHERE ROWNUM = 1))
                                                             AS ORGID                    --����ID                     ���ղ�¼������������Ĭ��Ϊ���и������в�(01280000)
                ,RANK() OVER(PARTITION BY T1.IACAC_NO ORDER BY LENGTH(NVL(T4.SORTNO,(SELECT SORTNO FROM TMP_SUPPORG WHERE ROWNUM = 1))) DESC)
                                                             AS RECORDNO                 --�������
                ,T1.IACAC_NO                                 AS IACAC_NO                 --�˺�
                ,T1.IACCURBAL                                AS IACCURBAL                --���
                ,NVL(T2.IACCRTDAT,TO_CHAR(TO_DATE(T1.IACCRTDAT,'YYYYMMDD'),'YYYY-MM-DD'))
                                                             AS IACCRTDAT                --��ʼ��
                ,NVL(T2.IACDLTDAT,TO_CHAR(TO_DATE(T1.IACDLTDAT,'YYYYMMDD'),'YYYY-MM-DD'))
                                                             AS IACDLTDAT                --������
                ,T1.IACGACBR                                 AS BELONGORGCODE            --ҵ����������
                ,T2.CLIENTNAME                               AS CLIENTNAME               --���׶�������
                ,T2.ORGANIZATIONCODE                         AS ORGANIZATIONCODE         --���׶�����֯��������
                ,NVL(T2.COUNTRYCODE,'CHN')                   AS COUNTRYCODE              --���׶���ע����Ҵ���         Ĭ��CHN-�й�
                ,NVL(T2.INDUSTRYID,'J66')                    AS INDUSTRYID               --���׶���������ҵ����         Ĭ��J66-���ҽ��ڷ���
                ,NVL(T2.CLIENTCATEGORY,'0202')               AS CLIENTCATEGORY           --�ͻ�����                     Ĭ��0202-�й���ҵ����

    FROM        RWA_DEV.CBS_IAC T1
    LEFT JOIN   (SELECT IACAC_NO
                       ,CLIENTNAME
                       ,ORGANIZATIONCODE
                       ,COUNTRYCODE
                       ,INDUSTRYID
                       ,IACCRTDAT
                       ,IACDLTDAT
                       ,CLIENTCATEGORY
                   FROM RWA.RWA_WS_DSBANK_ADV
                  WHERE DATADATE = (SELECT MAX(DATADATE) FROM RWA.RWA_WS_DSBANK_ADV WHERE DATADATE < TO_DATE(p_data_dt_str,'YYYYMMDD'))
                ) T2                                   --ȡ���һ�ڲ�¼�����̵�
    ON          T1.IACAC_NO = T2.IACAC_NO
    LEFT JOIN   RWA.ORG_INFO T3
    ON          T1.IACGACBR = T3.ORGID
    LEFT  JOIN  TMP_SUPPORG T4
    ON          T3.SORTNO LIKE T4.SORTNO || '%'
    WHERE       T1.IACITMNO = '13070800'               --ֱ�����е���Ŀ��
    AND         T1.IACCURBAL <> 0                      --��Ϊ0
    AND         T1.DATANO = p_data_dt_str
    )
    WHERE RECORDNO = 1
    ORDER BY    IACAC_NO
    ;

    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA',tabname => 'RWA_WSIB_DSBANK_ADV',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA.RWA_WSIB_DSBANK_ADV WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA.RWA_WSIB_DSBANK_ADV��ǰ����ĺ���ϵͳ-ֱ�����е���̵����ݼ�¼Ϊ: ' || v_count || ' ��');



    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
         ROLLBACK;
         p_po_rtncode := sqlcode;
         p_po_rtnmsg  := 'ֱ�����е�¼�����̵�('|| v_pro_name ||')����ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_ZX_WSIB;
/

