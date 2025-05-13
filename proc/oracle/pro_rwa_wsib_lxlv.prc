CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_WSIB_LXLV(
                            p_data_dt_str IN  VARCHAR2,   --�������� yyyyMMdd
                            p_po_rtncode  OUT VARCHAR2,   --���ر�� 1 �ɹ�,0 ʧ��
                            p_po_rtnmsg   OUT VARCHAR2    --��������
        )
  /*
    �洢��������:RWA_DEV.PRO_RWA_WSIB_LXLV
    ʵ�ֹ���:
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :
    ��дʱ��:2020-01-06
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_WSIB_LXLV';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    --EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA.RWA_WSIB_LXLV';
    DELETE FROM RWA.RWA_WSIB_LXLV WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');

    COMMIT;

    --2.���������������ݴ�Դ����뵽Ŀ�����

    INSERT INTO RWA.RWA_WSIB_LXLV
    (
    DATADATE                               --��������
   ,ORGID                                  --����ID
   ,CCY                                    --����
   ,BRPROCDATE                             --��ʼ����
   ,MATDATE                                --��������
   ,MTY                                    --����
   ,LXLV                                   --��Ϣ����
    )
  select
       TO_DATE(p_data_dt_str, 'YYYYMMDD')AS DATADATE --��������
       ,'9998'                           AS ORGID   --����ID
      ,T.CCY                             AS CCY --����
      , to_char(T.BRPROCDATE,'yyyy/mm/dd')  AS BRPROCDATE  --��ʼ����
      , to_char(T.MATDATE,'yyyy/mm/dd')      AS MATDATE --��������
      ,T.MTY                             AS  MTY --����
      ,K2.LXLV                           AS LXLV   --��Ϣ����
       FROM OPI_DDFT T
        INNER JOIN (SELECT BR,
                           CCY,
                           YIELDCURVE,
                           SHIFTSEQ,
                           MAX(BRPROCDATE) BRPROCDATE,
                           MAX(MATDATE) MATDATE,
                           QUOTETYPE,
                           MTY
                      FROM OPI_DDFT
                     WHERE OPI_DDFT.BR = '01'
                       AND OPI_DDFT.QUOTETYPE = 'M'
                       AND OPI_DDFT.YIELDCURVE = 'DISCOUNT'
                       AND OPI_DDFT.Datano=p_data_dt_str
                     GROUP BY BR, CCY, YIELDCURVE, SHIFTSEQ, QUOTETYPE, MTY) OPI_DDFT2
           ON T.CCY = OPI_DDFT2.CCY
          AND T.BRPROCDATE = OPI_DDFT2.BRPROCDATE
          AND T.MATDATE = OPI_DDFT2.MATDATE
      LEFT JOIN(SELECT   K1.CCY,
              K1.BRPROCDATE,
              K1.MATDATE,
              K1.MTY,
              K1.LXLV
           FROM RWA.RWA_WS_LXLV_BL K1
          WHERE DATADATE =
          (SELECT MAX(DATADATE)
          FROM RWA.RWA_WS_LXLV_BL
          WHERE DATADATE < TO_DATE(p_data_dt_str, 'YYYYMMDD'))) K2---ȡ��һ�ڵĲ�¼������Ϣ�̵�
           ON     T.CCY=K2.CCY
           AND   T.BRPROCDATE=K2.BRPROCDATE
           AND   T.MATDATE=K2.MATDATE
           AND   T.MTY=K2.MTY
          where T.Datano=p_data_dt_str
          order by T.CCY,T.MTY   ;
     
           commit;

    dbms_stats.gather_table_stats(ownname => 'RWA',tabname => 'RWA_WSIB_LXLV',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA.RWA_WSIB_LXLV WHERE DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD');
    --Dbms_output.Put_line('RWA.RWA_WSIB_LXLV��ǰ����ĺ���ϵͳ-ֱ�����е���̵����ݼ�¼Ϊ: ' || v_count || ' ��');



    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
         ROLLBACK;
         p_po_rtncode := sqlcode;
         p_po_rtnmsg  := '�ͻ�������Ϣ��¼�����̵�('|| v_pro_name ||')����ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_WSIB_LXLV;
/

