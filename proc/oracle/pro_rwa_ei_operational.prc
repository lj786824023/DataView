CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_EI_OPERATIONAL(p_data_dt_str  IN  VARCHAR2, --��������
                                                   p_po_rtncode   OUT VARCHAR2, --���ر��
                                                   p_po_rtnmsg    OUT VARCHAR2  --��������
)
  /*
    �洢��������:RWA_DEV.PRO_RWA_EI_OPERATIONAL
    ʵ�ֹ���:����������(�ӹ���)��Ϣ�ӹ�����������ձ�¶��)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :QHJIANG
    ��дʱ��:2016-06-28
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.FNS_GL_BALANCE|��������(�ӹ���)
    Դ  ��2 :RWA.CODE_LIBRARY|�����
    Դ  ��3 :RWA_DEV.TMP_CURRENCY_CHANGE|���ʱ�
    Դ  ��4 :RWA.RWA_CD_OPERATIONAL_STAND_MODEL|�������տ�Ŀ��-��׼��-����ģ��
    Դ  ��5 :RWA.ORG_INFO|������
    Ŀ���1 :RWA_DEV.RWA_EI_OPERATIONALEXPOSURE|�������ձ�¶��
    Ŀ���2 :RWA_DEV.RWA_EI_OPERATIONALACCOUNT|�������տ�Ŀ��
    ������  :��
    �����¼(�޸���|�޸�ʱ��|�޸�����):

    pxl   2019/12/05  ʹ�ò�������RMB�������ݼ�����������   ��Ϊ<>RMB����6��ͷ�����Ŀ�ı��Ҵ������⣨����ϵͳ���⣩

    */

  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_EI_OPERATIONAL';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ�����ļ�¼��
  v_count INTEGER :=1;
  --���嵱ǰ����ļ�¼��
  v_count1 INTEGER;
  --��ǰ��������
  v_data_dt_str VARCHAR2(20);
  --�ӹ����������
  v_run_count INTEGER :=0;
  --��Ӧ����Ƿ���Ҫ�ӹ�
  v_run_flag INTEGER :=0;
  --����Ϣ����
  v_INTERESTINCOME NUMBER(24,6) := 0;
  --����Ϣ֧��
  v_INTERESTEXPENSE NUMBER(24,6) := 0;

  BEGIN
    
    
    --���⴦��  ����ǰ�ſ�
    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    --��ĩ�ӹ�����
    IF TO_CHAR(TO_DATE(p_data_dt_str,'YYYYMMDD'),'YYYY')||'1231'=p_data_dt_str  THEN
      v_run_count := 1;
      --ǰ1��
      SELECT count(1) INTO v_run_flag FROM RWA_DEV.RWA_EI_OPERATIONALEXPOSURE WHERE DATADATE=ADD_MONTHS(TO_DATE(TO_CHAR(TO_DATE(p_data_dt_str,'YYYYMMDD'),'YYYY')||'1231','YYYYMMDD'),-12*1);
      IF v_run_flag=0 THEN v_run_count := v_run_count+1; END IF;
      v_run_flag := 0;
      --ǰ2��
      SELECT count(1) INTO v_run_flag FROM RWA_DEV.RWA_EI_OPERATIONALEXPOSURE WHERE DATADATE=ADD_MONTHS(TO_DATE(TO_CHAR(TO_DATE(p_data_dt_str,'YYYYMMDD'),'YYYY')||'1231','YYYYMMDD'),-12*2);
      IF v_run_flag=0 THEN v_run_count := v_run_count+1; END IF;
      v_run_flag := 0;
    END IF;

    WHILE v_count<=v_run_count LOOP
        v_data_dt_str := TO_CHAR(ADD_MONTHS(TO_DATE(p_data_dt_str,'YYYYMMDD'),-(v_count-1)*12),'YYYYMMDD');

        BEGIN
           --ɾ�������ڵķ���������˴洢�����ǵ�һ�����׳��쳣�����Ժ���
           EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_OPERATIONALACCOUNT DROP PARTITION OPERATIONAL' || v_data_dt_str;

           COMMIT;
           EXCEPTION
            WHEN OTHERS THEN
               IF (SQLCODE <> '-2149') THEN
                  --�״η���truncate�����2149�쳣
                  p_po_rtncode := sqlcode;
                  p_po_rtnmsg  := '�������տ�Ŀ��('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
               RETURN;
            END IF;
        END;

        --����һ����ǰ�����µķ���
        EXECUTE IMMEDIATE 'ALTER TABLE RWA_DEV.RWA_EI_OPERATIONALACCOUNT ADD PARTITION OPERATIONAL' || v_data_dt_str || ' VALUES(TO_DATE('|| v_data_dt_str || ',''YYYYMMDD''))';

        --DBMS_OUTPUT.PUT_LINE('��ʼ�����롾�������տ�Ŀ��' || v_data_dt_str);
        --�������տ�Ŀ��
        --DBMS_OUTPUT.PUT_LINE('��ʼ�����롾�������տ�Ŀ��' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
         INSERT INTO RWA_DEV.RWA_EI_OPERATIONALACCOUNT(
             DATADATE                --��������
            ,DATANO                  --������ˮ��
            ,ORGSORTNO               --�������������
            ,ORGID                   --��������ID
            ,ORGNAME                 --������������
            ,ACCOUNTCODE             --��Ŀ����
            ,ACCOUNTNAME             --��Ŀ����
            ,ACCOUNTBALANCE          --��Ŀ���
            ,CURRENCY                --����
        )
        SELECT
                TEMP.DATADATE                       AS DATADATE              --��������
               ,TEMP.DATANO                         AS DATANO                --������ˮ��
               ,OI.SORTNO                           AS ORGSORTNO             --�������������
               ,TEMP.ORGID                          AS ORGID                 --��������ID
               ,OI.ORGNAME                          AS ORGNAME               --������������
               ,ACCOUNTCODE                         AS ACCOUNTCODE           --��Ŀ����
               ,ACCOUNTNAME                         AS ACCOUNTNAME           --��Ŀ����
               ,ACCOUNTBALANCE                      AS ACCOUNTBALANCE        --��Ŀ���
               ,CURRENCY                            AS CURRENCY              --����
        FROM
            (SELECT
                     TO_DATE(FGB.DATANO,'YYYYMMDD')              AS DATADATE                --��������
                    ,FGB.DATANO                                  AS DATANO                  --������ˮ��
                    ,FGB.ORG_ID                                  AS ORGID                   --��������ID
                    ,FGB.SUBJECT_NO                              AS ACCOUNTCODE             --��Ŀ����
                    ,''                                          AS ACCOUNTNAME             --��Ŀ����
                    ,SUM(FGB.BALANCE)                            AS ACCOUNTBALANCE          --��Ŀ���
                    ,FGB.CURRENCY_CODE                           AS CURRENCY                --����
             FROM   (SELECT  --NPQ.MIDDLEPRICE
                             100
                            ,T.DATANO
                            ,'9998' AS ORG_ID
                            --,T.CURRENCY_CODE
                            ,'CNY' AS CURRENCY_CODE
                            ,T.SUBJECT_NO
                            ,CL.ITEMNAME
                            ,CASE
                               --WHEN OSM.DCFX = 'D-C' THEN  (T.BALANCE_D - T.BALANCE_C)*NVL(NPQ.MIDDLEPRICE, 100)/100 
                               WHEN OSM.DCFX = 'D-C' THEN  (T.BALANCE_D - T.BALANCE_C) 
                               --WHEN OSM.DCFX = 'C-D' THEN  (T.BALANCE_C - T.BALANCE_D)*NVL(NPQ.MIDDLEPRICE, 100)/100 
                               WHEN OSM.DCFX = 'C-D' THEN  (T.BALANCE_C - T.BALANCE_D)
                               --ELSE (T.BALANCE_D - T.BALANCE_C)*NVL(NPQ.MIDDLEPRICE, 100)/100
                               ELSE (T.BALANCE_D - T.BALANCE_C)
                             END  AS BALANCE
                     FROM RWA_DEV.FNS_GL_BALANCE T
                     LEFT JOIN RWA.CODE_LIBRARY CL
                     ON     CL.CODENO='NewSubject'
                     AND    T.SUBJECT_NO=CL.ITEMNO
                     AND    CL.ISINUSE='1'
                     --LEFT JOIN RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                     --ON     NPQ.DATANO = T.DATANO
                     --AND    NPQ.CURRENCYCODE = T.CURRENCY_CODE
                     LEFT JOIN RWA.RWA_CD_OPERATIONAL_STAND_MODEL OSM
                       ON T.SUBJECT_NO = OSM.SUBJECT_NO
                     WHERE  T.DATANO = v_data_dt_str
                     --AND    T.CURRENCY_CODE <> 'RMB'
                     AND    T.CURRENCY_CODE = 'RMB'
               ) FGB
            INNER JOIN RWA.RWA_CD_OPERATIONAL_STAND_MODEL OSM
              ON  FGB.SUBJECT_NO = OSM.SUBJECT_NO                 
            GROUP BY FGB.DATANO, FGB.ORG_ID, FGB.SUBJECT_NO, FGB.CURRENCY_CODE
            
       ) TEMP
        LEFT JOIN RWA.ORG_INFO OI
        ON     TEMP.ORGID=OI.ORGID
        AND    OI.STATUS='1'
        ;

        COMMIT;

        --���Ŀ����е�ԭ�м�¼
        EXECUTE IMMEDIATE 'DELETE FROM RWA_DEV.RWA_EI_OPERATIONALEXPOSURE WHERE DATADATE = TO_DATE('||v_data_dt_str||',''YYYYMMDD'')';

        COMMIT;

        --�������ձ�¶��
        --DBMS_OUTPUT.PUT_LINE('��ʼ�����롾�������ձ�¶��' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
        --���롾�������ձ�¶��������
        INSERT INTO RWA_DEV.RWA_EI_OPERATIONALEXPOSURE(
             DATADATE                --��������
            ,DATANO                  --������ˮ��
            ,BUSINESSLINE            --ҵ������
            ,ORGSORTNO               --�������������
            ,ORGID                   --��������ID
            ,ORGNAME                 --������������
            ,INTERESTINCOME          --��Ϣ����
            ,INTERESTEXPENSE         --��Ϣ֧��
            ,NETFEECOMMINCOME        --�����Ѻ�Ӷ������
            ,NETTRADPROFITLOSS       --����������
            ,NETINVESECUPROFITLOSS   --֤ȯͶ�ʾ�����
            ,OTHEROPERATINGINCOME    --����Ӫҵ����
        )
        SELECT
                TEMP.DATADATE                       AS DATADATE              --��������
               ,TEMP.DATANO                         AS DATANO                --������ˮ��
               ,TEMP.BUSINESSLINE                   AS BUSINESSLINE          --ҵ������
               ,OI.SORTNO                           AS ORGSORTNO             --�������������
               ,TEMP.ORGID                          AS ORGID                 --��������ID
               ,OI.ORGNAME                          AS ORGNAME               --������������
               ,TEMP.BALANCE010                     AS INTERESTINCOME        --��Ϣ����
               ,TEMP.BALANCE020                AS INTERESTEXPENSE       --��Ϣ֧��
               ,TEMP.BALANCE050-TEMP.BALANCE060     AS NETFEECOMMINCOME      --�����Ѻ�Ӷ������=050�����Ѽ�Ӷ������-060 �����Ѽ�Ӷ��֧��
               ,TEMP.BALANCE031+TEMP.BALANCE032     AS NETTRADPROFITLOSS     --����������=031 ��������-032 ������ʧ
               ,TEMP.BALANCE070                     AS NETINVESECUPROFITLOSS --֤ȯͶ�ʾ�����
               ,TEMP.BALANCE040                     AS OTHEROPERATINGINCOME  --����Ӫҵ����
        FROM
            (SELECT
                     TO_DATE(FGB.DATANO,'YYYYMMDD')                                                         AS DATADATE     --��������
                    ,FGB.DATANO                                                                             AS DATANO       --������ˮ��
                    ,OSM.BUSINESSLINE                                                                       AS BUSINESSLINE --ҵ������
                    ,FGB.ORG_ID                                                                             AS ORGID        --��������ID
                    ,SUM(DECODE(OSM.SUMTYPE,'010',FGB.BALANCE,0)*NVL(FGB.MIDRATE, 100)/100)                   AS BALANCE010   --010 ��Ϣ����
                    ,SUM(DECODE(OSM.SUMTYPE,'020',FGB.BALANCE,0)*NVL(FGB.MIDRATE, 100)/100)                   AS BALANCE020   --020 ��Ϣ֧��
                    ,SUM(DECODE(OSM.SUMTYPE,'031',FGB.BALANCE,0)*NVL(FGB.MIDRATE, 100)/100)                   AS BALANCE031   --031 ��������
                    ,SUM(DECODE(OSM.SUMTYPE,'032',FGB.BALANCE,0)*NVL(FGB.MIDRATE, 100)/100)                   AS BALANCE032   --032 ������ʧ
                    ,SUM(DECODE(OSM.SUMTYPE,'040',FGB.BALANCE,0)*NVL(FGB.MIDRATE, 100)/100)                   AS BALANCE040   --040 ����Ӫҵ����
                    ,SUM(DECODE(OSM.SUMTYPE,'050',FGB.BALANCE,0)*NVL(FGB.MIDRATE, 100)/100)                   AS BALANCE050   --050 �����Ѽ�Ӷ������
                    ,SUM(DECODE(OSM.SUMTYPE,'060',FGB.BALANCE,0)*NVL(FGB.MIDRATE, 100)/100)                   AS BALANCE060   --060 �����Ѽ�Ӷ��֧��
                    ,SUM(DECODE(OSM.SUMTYPE,'070',FGB.BALANCE,0)*NVL(FGB.MIDRATE, 100)/100)                   AS BALANCE070   --070 ֤ȯͶ�ʾ�����
               FROM   (
                      SELECT  --NPQ.MIDDLEPRICE
                             100 MIDRATE
                            ,T.DATANO
                            ,'9998' AS ORG_ID
                            --,T.CURRENCY_CODE
                            ,'CNY' AS CURRENCY_CODE
                            ,T.SUBJECT_NO
                            ,CL.ITEMNAME
                            ,CASE
                               --WHEN OSM.DCFX = 'D-C' THEN  (T.BALANCE_D - T.BALANCE_C)*NVL(NPQ.MIDDLEPRICE, 100)/100 
                               WHEN OSM.DCFX = 'D-C' THEN  (T.BALANCE_D - T.BALANCE_C) 
                               --WHEN OSM.DCFX = 'C-D' THEN  (T.BALANCE_C - T.BALANCE_D)*NVL(NPQ.MIDDLEPRICE, 100)/100 
                               WHEN OSM.DCFX = 'C-D' THEN  (T.BALANCE_C - T.BALANCE_D)
                               --ELSE (T.BALANCE_D - T.BALANCE_C)*NVL(NPQ.MIDDLEPRICE, 100)/100
                               ELSE (T.BALANCE_D - T.BALANCE_C)
                             END  AS BALANCE
                     FROM RWA_DEV.FNS_GL_BALANCE T
                     LEFT JOIN RWA.CODE_LIBRARY CL
                     ON     CL.CODENO='NewSubject'
                     AND    T.SUBJECT_NO=CL.ITEMNO
                     AND    CL.ISINUSE='1'
                     --LEFT JOIN RWA_DEV.TMP_CURRENCY_CHANGE NPQ
                     --ON     NPQ.DATANO = T.DATANO
                     --AND    NPQ.CURRENCYCODE = T.CURRENCY_CODE
                     LEFT JOIN RWA.RWA_CD_OPERATIONAL_STAND_MODEL OSM
                       ON T.SUBJECT_NO = OSM.SUBJECT_NO
                     WHERE  T.DATANO = v_data_dt_str
                     --AND    T.CURRENCY_CODE <> 'RMB'
                     AND    T.CURRENCY_CODE = 'RMB'
                    ) FGB
                    INNER JOIN RWA.RWA_CD_OPERATIONAL_STAND_MODEL OSM
                          ON  FGB.SUBJECT_NO = OSM.SUBJECT_NO     
             GROUP BY FGB.DATANO,OSM.BUSINESSLINE,FGB.ORG_ID) TEMP
        LEFT JOIN RWA.ORG_INFO OI
        ON     TEMP.ORGID=OI.ORGID
        AND    OI.STATUS='1'
        ;

				COMMIT;



        /***************     ��ҵ���м���ҵ�����߾���Ϣ����ʱ, Ӧ����ҵ�����ߵ��ʽ�ռ�ñ�����̯��Ϣ�ɱ���     ***********/

        --1. ��Ϣ�������          
        SELECT SUM(T1.INTERESTINCOME) INTO v_INTERESTINCOME --��Ϣ�����Ŀ����
          FROM RWA_DEV.RWA_EI_OPERATIONALEXPOSURE T1
         WHERE T1.DATADATE = TO_DATE(p_data_dt_str, 'YYYYMMDD')
        ;
        
        --2. ��Ϣ֧������  
        SELECT SUM(T1.INTERESTEXPENSE) INTO v_INTERESTEXPENSE  --��Ϣ֧����Ŀ����
        FROM   RWA_DEV.RWA_EI_OPERATIONALEXPOSURE T1
        WHERE  T1.DATADATE = TO_DATE(p_data_dt_str,'YYYYMMDD')
        ;
        
                
        --3.�������ߵ�ֵ=X * �������ߵ�ռ��        
        MERGE INTO RWA_DEV.RWA_EI_OPERATIONALEXPOSURE T1 
        USING (
                    
            SELECT T2.BUSINESSLINE,
                   v_INTERESTEXPENSE * (SUM(T2.INTERESTINCOME) / v_INTERESTINCOME)  INTERESTEXPENSE-- X �Ǿ���Ϣֵ
              FROM RWA_DEV.RWA_EI_OPERATIONALEXPOSURE T2
             WHERE T2.DATANO = p_data_dt_str
             GROUP BY T2.BUSINESSLINE
        ) T2 ON (T2.BUSINESSLINE = T1.BUSINESSLINE AND T1.DATANO = p_data_dt_str)
        WHEN MATCHED THEN 
           UPDATE SET T1.INTERESTEXPENSE = T2.INTERESTEXPENSE
        ;


        /*  ����һ���߼�
        SELECT SUM(TEMP2.INTERESTINCOME) INTO v_INTERESTINCOME
        FROM   RWA_DEV.RWA_EI_OPERATIONALEXPOSURE TEMP2
        WHERE  TEMP2.DATADATE = TO_DATE(v_data_dt_str,'YYYYMMDD')
        ;

        UPDATE RWA_DEV.RWA_EI_OPERATIONALEXPOSURE TEMP1
        SET    INTERESTEXPENSE=(CASE WHEN NVL(v_INTERESTINCOME,0)=0
                                     THEN 0
                                     ELSE (NVL(TEMP1.INTERESTEXPENSE,0)*NVL(TEMP1.INTERESTINCOME,0)/NVL(v_INTERESTINCOME,0)) END)
        WHERE  TEMP1.DATANO = v_data_dt_str
        ;

				COMMIT;
        */

        v_INTERESTINCOME :=0;

        --����Ϣ����[NETINTERESTINCOME]=��Ϣ����[INTERESTINCOME]-��Ϣ֧��[INTERESTEXPENSE]
        --������Ϣ����[NETNONINTERESTINCOME]=�����Ѻ�Ӷ������[NETFEECOMMINCOME]+����������[NETTRADPROFITLOSS]+֤ȯͶ�ʾ�����[NETINVESECUPROFITLOSS]+����Ӫҵ����[OTHEROPERATINGINCOME]
        --������[GROSSINCOME]=����Ϣ����[NETINTERESTINCOME]+������Ϣ����[NETNONINTERESTINCOME]
        UPDATE RWA_DEV.RWA_EI_OPERATIONALEXPOSURE OES
        SET     OES.NETINTERESTINCOME=(NVL(OES.INTERESTINCOME,0)-NVL(OES.INTERESTEXPENSE,0))
               ,OES.NETNONINTERESTINCOME=(NVL(OES.NETFEECOMMINCOME,0)+NVL(OES.NETTRADPROFITLOSS,0)+NVL(OES.NETINVESECUPROFITLOSS,0)+NVL(OES.OTHEROPERATINGINCOME,0))
               ,OES.GROSSINCOME=(NVL(OES.INTERESTINCOME,0)-NVL(OES.INTERESTEXPENSE,0))+(NVL(OES.NETFEECOMMINCOME,0)+NVL(OES.NETTRADPROFITLOSS,0)+NVL(OES.NETINVESECUPROFITLOSS,0)+NVL(OES.OTHEROPERATINGINCOME,0))
        WHERE  OES.DATANO = v_data_dt_str;

        COMMIT;

        v_count := v_count+1;
    END LOOP;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_OPERATIONALACCOUNT',cascade => true);
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_EI_OPERATIONALEXPOSURE',cascade => true);

    --DBMS_OUTPUT.PUT_LINE('���������롾�������ձ�¶��' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_EI_OPERATIONALEXPOSURE;
    --DBMS_OUTPUT.PUT_LINE('RWA_DEV.RWA_EI_OPERATIONALEXPOSURE-�������ձ�¶���в�������Ϊ��' || v_count1 || '��');

    --DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));
    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�';

    --�����쳣
    EXCEPTION WHEN OTHERS THEN
        --DBMS_OUTPUT.PUT_LINE('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
        ROLLBACK;
        p_po_rtncode := sqlcode;
        p_po_rtnmsg  := '���롾�������ձ�¶��('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;


    RETURN;

END PRO_RWA_EI_OPERATIONAL;
/

