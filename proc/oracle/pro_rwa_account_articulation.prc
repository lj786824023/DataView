CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ACCOUNT_ARTICULATION(p_data_dt_str in VARCHAR2, p_po_rtncode OUT VARCHAR2, p_po_rtnmsg OUT VARCHAR2)
  /*
    �洢��������:RWA_DEV.pro_rwa_account_articulation
    ʵ�ֹ���:���ն�����Ŀ�������ͱ�������ά�ȣ�����RWA�ķ��ձ�¶����Ƿ������������Ӧ����ĩ����ܹ�������
           ��ṹΪ���ձ�¶��RWA_DEV.RWA_EI_EXPOSURE
    ���ݿھ�:ȫ��
    ����Ƶ��:��ĩ
    ��  ��  :V1.0.0
    ��д��  :qpzhong
    ��дʱ��:20161016
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.FNS_GL_BALANCE                   |���˱�
    Դ  ��2 :RWA_DEV.NSS_PA_QUOTEPRICE                |����ת����
    Դ  ��3 :RWA_DEV.RWA_EI_EXPOSURE                  |����-���÷��ձ�¶��
    Դ  ��4 :RWA_DEV.RWA_ARTICULATION_PARAM           |���˹���������
    Դ  ��5 :RWA.CODE_LIBRARY                         |�����
    Դ  ��6 :RWA.ORG_INFO OI                          |������
    Դ  ��7 :RWA_DEV.RWA_ARTICULATION_TOLERANCE       |���˹������̶����ñ�
    Դ  ��8 :RWA_DEV.RWA_TMP_DERIVATION_SUBJECT       |���˹���������Ŀ��ʱ��
    Դ  ��9 :RWA_DEV.RWA_TMP_GLBALANCE02              |���˹������������ʱ���
    Դ  ��10:RWA_DEV.RWA_TMP_GLBALANCE                |���˹�����Ŀ�����ʱ��
    Դ  ��11:RWA_DEV.RWA_TMP_DERIVATION_SUBJECT2      |���˹���������Ŀ��ʱ���
    Դ  ��12:RWA_DEV.RWA_TMP_DERIVATION_BALANCE01     |���˹���������Ŀ�����ʱ��һ
    Դ  ��13:RWA_DEV.RWA_TMP_DERIVATION_BALANCE02     |���˹���������Ŀ�����ʱ���
    Դ  ��14:RWA_DEV.RWA_TMP_DERIVATION_BALANCE03     |���˹���������Ŀ�����ʱ����
    Դ  ��15:RWA_DEV.RWA_TMP_EXPOBALANCE              |���˹�����¶�����ʱ��
    Ŀ���1 :RWA_DEV.RWA_ARTICULATION_RESULT          |���˹��������
    Ŀ���2 :RWA_DEV.RWA_EI_CLIENT                    |���������
    Ŀ���3 :RWA_DEV.RWA_EI_CONTRACT                  |��ͬ��
    Ŀ���4 :RWA_DEV.RWA_EI_EXPOSURE                  |��¶��
    Ŀ���5 :RWA_DEV.RWA_TMP_GLBALANCE                |���˹�����Ŀ�����ʱ��
    Ŀ���6 :RWA_DEV.RWA_TMP_GLBALANCE02              |���˹������������ʱ���
    Ŀ���7 :RWA_DEV.RWA_TMP_DERIVATION_SUBJECT2      |���˹���������Ŀ��ʱ���
    Ŀ���8 :RWA_DEV.RWA_TMP_DERIVATION_BALANCE01     |���˹���������Ŀ�����ʱ��һ
    Ŀ���9 :RWA_DEV.RWA_TMP_DERIVATION_BALANCE02     |���˹���������Ŀ�����ʱ���
    Ŀ���10:RWA_DEV.RWA_TMP_DERIVATION_BALANCE03     |���˹���������Ŀ�����ʱ����
    Ŀ���11:RWA_DEV.RWA_TMP_EXPOBALANCE              |���˹�����¶�����ʱ��
    ������  :��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  V_BWXTZBJ NUMBER(24,6); ----ϵͳ�����I9��ȡ�����м�ֵ
  V_BWZZZBJ NUMBER(24,6); ----���˰��տ�Ŀ��ȡ������ Ԥ�Ƹ�ծ-�����ʲ���ֵ׼��
  V_BWXHZBJ NUMBER(24,6); ----���˰��տ�Ŀ��ȡ������ Ԥ�Ƹ�ծ-�����ʲ���ֵ׼��
  V_BWCHA   NUMBER(24,6); ----�������˺�ϵͳ��ȡI9�ļ�ֵ�Ĳ��죬�����ÿ�������죬��̯���������ÿ�ÿһ�¶
  V_BNXTZBJ NUMBER(24,6); ----ϵͳ���ڴ�����ÿ���I9��ȡ�����м�ֵ
  V_BNZZZBJ NUMBER(24,6); ----���˰��տ�Ŀ��ȡ��ϵͳ���� ������ʧ׼��
  V_BNCHA   NUMBER(24,6); ----���ڴ�����ʧ׼�����˺�ϵͳȡ�ļ�ֵ�Ĳ��죬�����ÿ��Ĳ��죬��̯��010803
  
  v_pro_name VARCHAR2(200) := 'RWA_DEV.pro_rwa_account_articulation';
  v_datadate date := TO_DATE(p_data_dt_str,'yyyy/mm/dd');   --��������
  v_datano VARCHAR2(8) := TO_CHAR(v_datadate, 'yyyymmdd');  --������ˮ��
  v_startdate VARCHAR2(10) := TO_CHAR(v_datadate,'yyyy-mm-dd'); --��ʼ����

  v_count NUMBER := 0;

  v_intolerance NUMBER(24,6) := 0.001; --�������̶� Ĭ��0.1%
  v_outtolerance NUMBER(24,6) := 0.01; --�������̶� Ĭ��1%

  --��ǰ������Ŀ�Ļ�����Ŀ����ʽ ''''a'',''b'',''c''''
  --v_subject_str VARCHAR2(1000) := '';

  --�����ʲ��ۼ���
  V_ILDDEBT number(24,6):=0;

  --����������˹������̶�������Ϣ�α�
  CURSOR cursor_type_tolerance IS
    SELECT tolerance_type, tolerance FROM RWA_DEV.rwa_articulation_tolerance;

  cursor_tolerance cursor_type_tolerance%ROWTYPE;

  --����洢������Ŀ��table
  TYPE table_derivation_subject
  IS TABLE OF RWA_DEV.rwa_tmp_derivation_subject.subject_no%TYPE INDEX BY BINARY_INTEGER;

  v_tds table_derivation_subject;

  --�������������Ŀ��Ϣ�α�
  CURSOR cursor_derivation_subject
  IS SELECT subject_no FROM RWA_DEV.rwa_tmp_derivation_subject;

  cursor_ds cursor_derivation_subject%ROWTYPE;

  BEGIN
 
 DBMS_OUTPUT.PUT_LINE('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --���Ŀ����е�ԭ�м�¼
  DELETE FROM RWA_DEV.rwa_articulation_result WHERE datadate = v_datadate;
  DELETE FROM RWA_DEV.rwa_ei_client WHERE datadate = v_datadate AND ssysid = 'GC';
  DELETE FROM RWA_DEV.rwa_ei_contract WHERE datadate = v_datadate AND ssysid = 'GC';
  DELETE FROM RWA_DEV.rwa_ei_exposure WHERE datadate = v_datadate AND ssysid = 'GC';
  COMMIT;

  EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TMP_GLBALANCE';
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TMP_GLBALANCE02';
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TMP_DERIVATION_SUBJECT';
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TMP_DERIVATION_SUBJECT2';
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TMP_DERIVATION_BALANCE01';
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TMP_DERIVATION_BALANCE02';
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TMP_DERIVATION_BALANCE03';
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TMP_EXPOBALANCE';
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_TMP_ABSBOND';


 --��ȡ���˽����Ϣ���л�������
  INSERT INTO RWA_DEV.RWA_TMP_GLBALANCE(
              SUBJECT_NO
              ,ORGID
              ,CURRENCY
              ,ACCOUNT_BALANCE
  )
  SELECT FGB.SUBJECT_NO,
         FGB.ORG_ID AS ORGID,
         FGB.CURRENCY_CODE AS CURRENCY,
         CASE WHEN CL.ATTRIBUTE8 = 'C-D' 
           /*���˽���Ƿ�ת�����ʽ��*/
           THEN SUM(FGB.BALANCE_C * NVL(NPQ.MIDDLEPRICE/100, 1) - FGB.BALANCE_D * NVL(NPQ.MIDDLEPRICE/100, 1))
             ELSE SUM(FGB.BALANCE_D * NVL(NPQ.MIDDLEPRICE/100, 1) - FGB.BALANCE_C * NVL(NPQ.MIDDLEPRICE/100, 1))
           END AS ACCOUNT_BALANCE --��Ŀ���
    FROM RWA_DEV.FNS_GL_BALANCE FGB
    LEFT JOIN RWA_DEV.TMP_CURRENCY_CHANGE NPQ --�����м�۱�
      ON NPQ.DATANO = FGB.DATANO
     AND NPQ.CURRENCYCODE = FGB.CURRENCY_CODE
    LEFT JOIN RWA.CODE_LIBRARY CL
      ON CL.CODENO = 'NewSubject'
     AND CL.ITEMNO = FGB.SUBJECT_NO
   WHERE FGB.DATANO = v_datano
     AND FGB.CURRENCY_CODE <> 'RMB'
   GROUP BY FGB.SUBJECT_NO, FGB.ORG_ID, CURRENCY_CODE,CL.ATTRIBUTE8;

   COMMIT;

  --��ȡ������Ŀ��Ϣ
  INSERT INTO RWA_DEV.RWA_TMP_DERIVATION_SUBJECT (SUBJECT_NO, ARTICULATERELATION)
    SELECT T.THIRD_SUBJECT_NO AS SUBJECT_NO, T.ARTICULATERELATION
      FROM RWA_DEV.RWA_ARTICULATION_PARAM T
     WHERE ARTICULATERELATION IS NOT NULL
        AND T.ISINUSE = '1'--1:������
        AND T.ARTICULATETYPE = '01' --01:�����ɱ�
  ;
  COMMIT;

  --��ȡ���˹�����������ARTICULATERELATION������ϵ������Ŀ�Ŀ��
  INSERT INTO RWA_DEV.RWA_TMP_DERIVATION_SUBJECT2
  WITH TEMP_SUBJECT1 AS  --��ȡ�򵥼Ӽ��Ŀ�Ŀ
       (
       SELECT DISTINCT DS.ARTICULATERELATION --ԭʼ��Ŀ��
                       ,REGEXP_SUBSTR(DS.ARTICULATERELATION,'[^-+]+', 1,LEVEL, 'i') AS REL_SUBJECT_NO --������Ŀ
          FROM (
              SELECT * FROM RWA_DEV.RWA_ARTICULATION_PARAM  T
              WHERE UPPER(ARTICULATERELATION) NOT LIKE 'MAX%'
               AND ISCALCULATE = '1' --�Ƿ�RWA���� 0�� 1��
               AND ISINUSE = '1' --����״̬ 1���� 0ͣ��
               AND ARTICULATETYPE = '01' --�������� 01:�����ɱ�
               AND ARTICULATERELATION IS NOT NULL
           ) DS
           CONNECT BY LEVEL <= LENGTH(REGEXP_REPLACE(DS.ARTICULATERELATION, '[^-+]+', '')) + 1)
        ,
        TEMP_SUbJECT2 AS  --��ȡ���ӿ�Ŀ(ȥ��ARTICULATERELATION�ֶ�MAX(,0))
       (SELECT DISTINCT ARTICULATERELATION--ԭʼ��Ŀ��
                       ,REGEXP_SUBSTR(ARTICULATERELATION2,'[^-+]+', 1,LEVEL, 'i') AS REL_SUBJECT_NO --������Ŀ
          FROM(
         SELECT ARTICULATERELATION,
                --ȥ��ARTICULATERELATION�ֶ�MAX(,0)
                CASE WHEN instr(DS.ARTICULATERELATION,',')>6 THEN SUBSTR(DS.ARTICULATERELATION,5,instr(DS.ARTICULATERELATION,',')-5)
                     ELSE SUBSTR(DS.ARTICULATERELATION,7,LENGTH(DS.ARTICULATERELATION)-7) END as ARTICULATERELATION2
          FROM RWA_DEV.RWA_ARTICULATION_PARAM DS
         WHERE UPPER(DS.ARTICULATERELATION) LIKE 'MAX%'
       AND DS.ISCALCULATE = '1' --�Ƿ�RWA���� 0�� 1��
           AND DS.ISINUSE = '1' --����״̬ 1���� 0ͣ��
           AND DS.ARTICULATETYPE = '01' --�������� 01:�����ɱ�
           AND DS.ARTICULATERELATION IS NOT NULL
           )
           CONNECT BY LEVEL <= LENGTH(REGEXP_REPLACE(ARTICULATERELATION2, '[^-+]+', '')) + 1)
      SELECT DISTINCT RAP.THIRD_SUBJECT_NO AS SUBJECT_NO, TS.REL_SUBJECT_NO
        FROM RWA_DEV.RWA_ARTICULATION_PARAM RAP
       INNER JOIN (SELECT * FROM TEMP_SUBJECT1
                   UNION
                   SELECT * FROM TEMP_SUBJECT2
                  ) TS
          ON TS.ARTICULATERELATION = RAP.ARTICULATERELATION
       ORDER BY RAP.THIRD_SUBJECT_NO, TS.REL_SUBJECT_NO ASC;
    COMMIT;

  --��ȡ������Ŀ���
  INSERT INTO RWA_DEV.RWA_TMP_DERIVATION_BALANCE01
    (SUBJECT_NO, ORGID, CURRENCY, ARTICULATERELATION, ACCOUNT_BALANCE)
    SELECT GLT.SUBJECT_NO,
           GLT.ORGID,
           GLT.CURRENCY,
           DS.ARTICULATERELATION,
           GLT.ACCOUNT_BALANCE
      FROM RWA_DEV.RWA_TMP_GLBALANCE GLT
     INNER JOIN RWA_DEV.RWA_TMP_DERIVATION_SUBJECT DS
        ON GLT.SUBJECT_NO = DS.SUBJECT_NO;
  COMMIT;

  --��ȡ������Ŀ������ĿARTICULATERELATION������Ŀ�Ŀ��Ӧ���
  INSERT INTO RWA_DEV.RWA_TMP_DERIVATION_BALANCE02 (SUBJECT_NO, ORGID, CURRENCY, ACCOUNT_BALANCE)
  SELECT GLT.SUBJECT_NO, GLT.ORGID, GLT.CURRENCY, SUM(GLT.ACCOUNT_BALANCE )
    FROM RWA_DEV.RWA_TMP_GLBALANCE GLT
   WHERE EXISTS (
    SELECT 1 FROM RWA_DEV.RWA_TMP_DERIVATION_SUBJECT2 DS2 WHERE GLT.SUBJECT_NO = DS2.rel_subject_no
   ) 
  /* WHERE GLT.SUBJECT_NO IN (
  SELECT DISTINCT REGEXP_SUBSTR(DS.ARTICULATERELATION,'[^-+]+',1,LEVEL,'i') AS subject_no
                           FROM (SELECT CASE WHEN INSTR(ARTICULATERELATION,',')=0 THEN ARTICULATERELATION
                                             WHEN INSTR(ARTICULATERELATION,',')>6 THEN SUBSTR(ARTICULATERELATION,5,INSTR(ARTICULATERELATION,',')-5)
                                              ELSE SUBSTR(ARTICULATERELATION,7,LENGTH(ARTICULATERELATION)-7) END AS ARTICULATERELATION
                                   FROM RWA_DEV.RWA_TMP_DERIVATION_SUBJECT
                                ) DS
                           CONNECT BY LEVEL <= LENGTH(REGEXP_REPLACE(DS.ARTICULATERELATION,'[^-+]+',''))+1
                          )
      */        
  GROUP BY GLT.SUBJECT_NO,GLT.ORGID,GLT.CURRENCY
  ;

  COMMIT;

  --��������Ŀ��Ϣ���������
  --ͨ���α��ȡ������Ŀ��Ϣ
  V_COUNT := 1;
  IF CURSOR_DERIVATION_SUBJECT%ISOPEN = FALSE THEN
     OPEN CURSOR_DERIVATION_SUBJECT;
  END IF;

  LOOP
      FETCH CURSOR_DERIVATION_SUBJECT INTO CURSOR_DS;
      EXIT WHEN CURSOR_DERIVATION_SUBJECT%NOTFOUND;
      V_TDS(V_COUNT) := CURSOR_DS.SUBJECT_NO;
    V_COUNT := V_COUNT+1;
  END LOOP;

  IF CURSOR_DERIVATION_SUBJECT%ISOPEN THEN
     CLOSE CURSOR_DERIVATION_SUBJECT;
  END IF;

  v_count := 0;

  --dbms_output.put_line('������Ŀ��:' || v_tds.count);
  --������Ŀ���з���
  IF V_TDS.COUNT >0 THEN
     FOR I IN 1..V_TDS.COUNT LOOP
       --����V_SUBJECT_STR����δʹ�� ��ע��
     /*SELECT REGEXP_REPLACE(ARTICULATERELATION, '[^0-9]+', ',') INTO V_SUBJECT_STR
       FROM (SELECT SUBJECT_NO,CASE WHEN INSTR(ARTICULATERELATION,',')=0 then ARTICULATERELATION
                                    WHEN instr(ARTICULATERELATION,',')>6 THEN SUBSTR(ARTICULATERELATION,5,instr(ARTICULATERELATION,',')-5)
                                    ELSE SUBSTR(ARTICULATERELATION,7,LENGTH(ARTICULATERELATION)-7) END as ARTICULATERELATION
               FROM RWA_DEV.RWA_TMP_DERIVATION_SUBJECT
              WHERE ARTICULATERELATION LIKE 'MAX%'
              UNION
             SELECT SUBJECT_NO,ARTICULATERELATION
               FROM RWA_DEV.RWA_TMP_DERIVATION_SUBJECT
              WHERE ARTICULATERELATION NOT LIKE 'MAX%')
      WHERE SUBJECT_NO = V_TDS(I); */

     --dbms_output.put_line('��ǰ�ַ���:' || v_subject_str);
     INSERT INTO RWA_DEV.RWA_TMP_DERIVATION_BALANCE03(
                     SUBJECT_NO
                     ,ORGID
                     ,CURRENCY
                     ,ARTICULATERELATION
                     ,REPLACED_FUNCTION
                     ,LOGIC_VALUE
        )
        SELECT
             SUBJECT_NO
             ,ORGID
             ,CURRENCY
             ,ARTICULATERELATION
             ,SUBSTR(COMPLEX_LOGIC_FUNCTION,1,INSTR(COMPLEX_LOGIC_FUNCTION,'@',1,1)-1) AS REPLACED_FUNCTION
             ,SUBSTR(COMPLEX_LOGIC_FUNCTION,INSTR(COMPLEX_LOGIC_FUNCTION,'@',1,1)+1) AS LOGIC_VALUE
        FROM (
              WITH GL_TEMP AS (
                       SELECT DISTINCT
                              TT.SUBJECT_NO,
                              REPLACE(T1.ARTICULATERELATION, 'MAX', 'GREATEST') AS ARTICULATERELATION,
                              TT.ORGID,
                              TT.CURRENCY,
                              TT.REL_SUBJECT_NO AS SUB_SUBJECT_NO ,
                              NVL(T2.ACCOUNT_BALANCE, 0) AS SUB_ACCOUNT_BALANCE
                         FROM (SELECT DISTINCT B.SUBJECT_NO,B.REL_SUBJECT_NO,A.ORGID,A.CURRENCY
                                 FROM (SELECT SUBJECT_NO, CURRENCY, ORGID
                                         FROM RWA_DEV.RWA_TMP_DERIVATION_BALANCE02 AA
                                        WHERE EXISTS (
                                              SELECT 1 FROM RWA_DEV.RWA_ARTICULATION_PARAM BB
                                               WHERE BB.THIRD_SUBJECT_NO = V_TDS(I)
                                                 AND INSTR(BB.ARTICULATERELATION,AA.SUBJECT_NO)>0
                                                 AND BB.ISGATHER = '0' --�Ƿ���ܵ����й��� 0:��
                                                 ) 
                                       ) A,
                                      (SELECT SUBJECT_NO,REL_SUBJECT_NO
                                         FROM RWA_DEV.RWA_TMP_DERIVATION_SUBJECT2
                                        WHERE SUBJECT_NO = V_TDS(I)
                                      ) B
                                ) TT
                        LEFT JOIN RWA_DEV.RWA_TMP_DERIVATION_BALANCE01 T1
                          ON T1.SUBJECT_NO=TT.SUBJECT_NO
                        LEFT JOIN RWA_DEV.RWA_TMP_DERIVATION_BALANCE02 T2
                          ON TT.CURRENCY = T2.CURRENCY
                         AND TT.ORGID = T2.ORGID
                         AND TT.REL_SUBJECT_NO = T2.SUBJECT_NO
                       UNION
                       SELECT SUBJECT_NO,ARTICULATERELATION,ORGID,CURRENCY,SUB_SUBJECT_NO,SUM(SUB_ACCOUNT_BALANCE) AS SUB_ACCOUNT_BALANCE 
                       FROM (
                       SELECT DISTINCT TT.SUBJECT_NO,
                                       REPLACE(T1.ARTICULATERELATION, 'MAX', 'GREATEST') AS ARTICULATERELATION,
                                       '9998' AS ORGID,
                                       TT.CURRENCY,
                                       TT.REL_SUBJECT_NO AS SUB_SUBJECT_NO,
                                       NVL(T2.ACCOUNT_BALANCE, 0) AS SUB_ACCOUNT_BALANCE
                         FROM (SELECT DISTINCT B.SUBJECT_NO,
                                               B.REL_SUBJECT_NO,
                                               A.CURRENCY
                                 FROM ( SELECT DISTINCT SUBJECT_NO, CURRENCY
                                          FROM RWA_DEV.RWA_TMP_DERIVATION_BALANCE02 AA
                                         WHERE EXISTS (
                                               SELECT 1 FROM RWA_DEV.RWA_ARTICULATION_PARAM BB
                                                WHERE BB.THIRD_SUBJECT_NO = V_TDS(I)
                                                  AND INSTR(BB.ARTICULATERELATION, AA.SUBJECT_NO) > 0
                                                  AND BB.ISGATHER = '1' --�Ƿ���ܵ����й��� 1:��
                                                  )
                                        ) A,
                                       (SELECT SUBJECT_NO, REL_SUBJECT_NO
                                          FROM RWA_DEV.RWA_TMP_DERIVATION_SUBJECT2
                                         WHERE SUBJECT_NO = V_TDS(I)
                                        ) B
                               ) TT
                         LEFT JOIN (SELECT SUBJECT_NO,CURRENCY,ARTICULATERELATION,SUM(ACCOUNT_BALANCE ) AS ACCOUNT_BALANCE
                            FROM RWA_DEV.RWA_TMP_DERIVATION_BALANCE01
                            WHERE SUBJECT_NO = V_TDS(I)
                            GROUP BY SUBJECT_NO,CURRENCY,ARTICULATERELATION) T1
                           ON T1.SUBJECT_NO = TT.SUBJECT_NO
                         LEFT JOIN (SELECT SUBJECT_NO,CURRENCY,SUM(ACCOUNT_BALANCE ) AS ACCOUNT_BALANCE
                            FROM RWA_DEV.RWA_TMP_DERIVATION_BALANCE02
                            GROUP BY SUBJECT_NO,CURRENCY ) T2
                           ON TT.CURRENCY = T2.CURRENCY
                          AND TT.REL_SUBJECT_NO = T2.SUBJECT_NO
                         )
                       GROUP BY SUBJECT_NO,ARTICULATERELATION,ORGID,CURRENCY,SUB_SUBJECT_NO
               )
               SELECT
                     SUBJECT_NO
                     ,ORGID
                     ,CURRENCY
                     ,ARTICULATERELATION
                     ,FUN_DERIVATION_SUBJECT(SUB_SUBJECT_NO||'@'||ARTICULATERELATION||'@'||SUB_ACCOUNT_BALANCE) AS COMPLEX_LOGIC_FUNCTION
               FROM  GL_TEMP
               GROUP BY SUBJECT_NO ,ORGID ,CURRENCY ,ARTICULATERELATION
         );
         COMMIT;

   END LOOP;
  END IF;

  --����RWA_DEV.RWA_TMP_GLBALANCE���
   DELETE FROM RWA_DEV.RWA_TMP_GLBALANCE
    WHERE SUBJECT_NO IN (SELECT SUBJECT_NO FROM RWA_DEV.RWA_TMP_DERIVATION_BALANCE03);

   INSERT INTO RWA_DEV.RWA_TMP_GLBALANCE (SUBJECT_NO, ORGID, CURRENCY, ACCOUNT_BALANCE)
     SELECT T.SUBJECT_NO,
            T.ORGID,
            T.CURRENCY,
            T.LOGIC_VALUE AS ACCOUNT_BALANCE
       FROM RWA_DEV.RWA_TMP_DERIVATION_BALANCE03 T;
   COMMIT;

--ͨ���α��ȡ���˹������̶�������Ϣ
  IF CURSOR_TYPE_TOLERANCE%ISOPEN = FALSE THEN
     OPEN CURSOR_TYPE_TOLERANCE;
  END IF;

  LOOP
      FETCH CURSOR_TYPE_TOLERANCE INTO CURSOR_TOLERANCE;
      EXIT WHEN CURSOR_TYPE_TOLERANCE%NOTFOUND;
      IF CURSOR_TOLERANCE.TOLERANCE_TYPE = '01' THEN --����
        IF CURSOR_TOLERANCE.TOLERANCE IS NOT NULL THEN
           V_INTOLERANCE := CURSOR_TOLERANCE.TOLERANCE;
        END IF;
      ELSE
           IF CURSOR_TOLERANCE.TOLERANCE IS NOT NULL THEN
               V_OUTTOLERANCE := CURSOR_TOLERANCE.TOLERANCE;
           END IF;
      END IF;
  END LOOP;

  IF CURSOR_TYPE_TOLERANCE%ISOPEN THEN
     CLOSE CURSOR_TYPE_TOLERANCE;
  END IF;

  --�������� ��/����ܵ����й���
  INSERT INTO RWA_DEV.RWA_TMP_EXPOBALANCE (
        SUBJECT_NO
        ,ORGID
        ,CURRENCY
        ,EXPOSE_BALANCE
  )
  WITH TEMP_RWA_EI_EXPOSURE AS
   (SELECT R.ACCSUBJECT1,
           R.ORGID,
           R.CURRENCY,
           R.NORMALPRINCIPAL
      FROM RWA_DEV.RWA_EI_EXPOSURE R
     WHERE R.DATADATE = V_DATADATE
       AND R.SSYSID <> 'ABS'
       AND R.ACCSUBJECT1  NOT IN (SELECT REL_SUBJECT_NO FROM RWA_DEV.RWA_TMP_DERIVATION_SUBJECT2)
     UNION ALL
    SELECT RTDS.SUBJECT_NO,
           R.ORGID,
           R.CURRENCY,
           SUM(R.NORMALPRINCIPAL) AS  NORMALPRINCIPAL
      FROM RWA_DEV.RWA_EI_EXPOSURE R
     INNER JOIN RWA_DEV.RWA_TMP_DERIVATION_SUBJECT2 RTDS
        ON RTDS.REL_SUBJECT_NO = R.ACCSUBJECT1
     WHERE R.DATADATE = V_DATADATE
       AND R.SSYSID <> 'ABS'
     GROUP BY RTDS.SUBJECT_NO, R.ORGID, R.CURRENCY
   )
  SELECT REE.ACCSUBJECT1,
         '9998' AS ORGID,
         REE.CURRENCY,
         NVL(SUM(REE.NORMALPRINCIPAL), 0) AS EXPOSE_BALANCE --��¶���
    FROM TEMP_RWA_EI_EXPOSURE REE
   INNER JOIN RWA_DEV.RWA_ARTICULATION_PARAM RAP
      ON REE.ACCSUBJECT1 = RAP.THIRD_SUBJECT_NO
     AND RAP.ARTICULATETYPE = '01' --��������
     AND RAP.ISINUSE = '1' --1��������
     AND RAP.ISGATHER = '1' --�Ƿ���ܵ����й���
   GROUP BY REE.ACCSUBJECT1,  REE.CURRENCY
   UNION ALL
  SELECT REE.ACCSUBJECT1,
         REE.ORGID,
         REE.CURRENCY,
         NVL(SUM(REE.NORMALPRINCIPAL), 0) AS EXPOSE_BALANCE --��¶���
    FROM TEMP_RWA_EI_EXPOSURE REE
   INNER JOIN RWA_DEV.RWA_ARTICULATION_PARAM RAP
      ON REE.ACCSUBJECT1 = RAP.THIRD_SUBJECT_NO
     AND RAP.ARTICULATETYPE = '01' --��������
     AND RAP.ISINUSE = '1' --1��������
     AND RAP.ISGATHER = '0' --�Ƿ���ܵ����й���
   GROUP BY REE.ACCSUBJECT1, REE.ORGID, REE.CURRENCY
  ;

  COMMIT;

  --������Ϣ+�������� ��/����ܵ����й���
  INSERT INTO RWA_DEV.RWA_TMP_EXPOBALANCE (
        SUBJECT_NO
        ,ORGID
        ,CURRENCY
        ,EXPOSE_BALANCE
  )
  WITH TEMP_RWA_EI_EXPOSURE AS
   (SELECT R.ACCSUBJECT1,
           R.ORGID,
           R.CURRENCY,
           R.NORMALINTEREST,
           R.ONDEBITINTEREST,
           R.EXPENSERECEIVABLE
      FROM RWA_DEV.RWA_EI_EXPOSURE R
     WHERE R.DATADATE = V_DATADATE
       AND R.SSYSID <> 'ABS'
   )
  SELECT RAP4.INTERESTSUBJECT AS ACCSUBJECT1,
         '9998' AS ORGID,
         REE.CURRENCY,
         CASE WHEN RAP4.INTERESTTYPE = '01' THEN NVL(SUM(REE.NORMALINTEREST), 0)
              WHEN RAP4.INTERESTTYPE = '03' THEN  NVL(SUM(REE.NORMALINTEREST + REE.ONDEBITINTEREST), 0)
              ELSE NVL(SUM(REE.ONDEBITINTEREST), 0) END AS EXPOSE_BALANCE --��¶���
    FROM TEMP_RWA_EI_EXPOSURE REE
   INNER JOIN (SELECT DISTINCT RAP3.INTERESTSUBJECT,
                               RAP3.THIRD_SUBJECT_NO,
                               RAP3.INTERESTTYPE
                 FROM RWA_DEV.RWA_ARTICULATION_PARAM RAP2
                INNER JOIN RWA_DEV.RWA_ARTICULATION_PARAM RAP3
                   ON RAP2.THIRD_SUBJECT_NO = RAP3.INTERESTSUBJECT
                WHERE RAP2.ISGATHER = '1' --1:���ܵ����й���
                  AND RAP2.ARTICULATETYPE = '02' --������Ϣ
                  AND RAP2.ISINUSE = '1'
               ) RAP4
      ON REE.ACCSUBJECT1 = RAP4.THIRD_SUBJECT_NO
   GROUP BY RAP4.INTERESTSUBJECT, RAP4.INTERESTTYPE, REE.CURRENCY
   UNION ALL
  SELECT RAP4.INTERESTSUBJECT AS ACCSUBJECT1,
         REE.ORGID,
         REE.CURRENCY,
         CASE WHEN RAP4.INTERESTTYPE = '01' THEN NVL(SUM(REE.NORMALINTEREST), 0)
              WHEN RAP4.INTERESTTYPE = '03' THEN  NVL(SUM(REE.NORMALINTEREST + REE.ONDEBITINTEREST), 0)
              ELSE NVL(SUM(REE.ONDEBITINTEREST), 0) END AS EXPOSE_BALANCE --��¶���
    FROM TEMP_RWA_EI_EXPOSURE REE
   INNER JOIN (SELECT DISTINCT RAP3.INTERESTSUBJECT,
                               RAP3.THIRD_SUBJECT_NO,
                               RAP3.INTERESTTYPE
                 FROM RWA_DEV.RWA_ARTICULATION_PARAM RAP2
                INNER JOIN RWA_DEV.RWA_ARTICULATION_PARAM RAP3
                   ON RAP2.THIRD_SUBJECT_NO = RAP3.INTERESTSUBJECT
                WHERE RAP2.ISGATHER = '0' --0:�����ܵ����й���
                  AND RAP2.ARTICULATETYPE = '02' --������Ϣ
                  AND RAP2.ISINUSE = '1'
               ) RAP4
      ON REE.ACCSUBJECT1 = RAP4.THIRD_SUBJECT_NO
   GROUP BY RAP4.INTERESTSUBJECT,REE.ORGID, RAP4.INTERESTTYPE, REE.CURRENCY
   UNION ALL
  SELECT RAP4.EXPENSESUBJECT AS ACCSUBJECT1,
         '9998' AS ORGID,
         REE.CURRENCY,
         CASE WHEN RAP4.EXPENSETYPE = '01' THEN NVL(SUM(REE.EXPENSERECEIVABLE), 0)
              ELSE NVL(SUM(REE.EXPENSERECEIVABLE), 0) END AS EXPOSE_BALANCE --��¶���
    FROM TEMP_RWA_EI_EXPOSURE REE
   INNER JOIN (SELECT DISTINCT RAP3.EXPENSESUBJECT,
                               RAP3.THIRD_SUBJECT_NO,
                               RAP3.EXPENSETYPE
                 FROM RWA_DEV.RWA_ARTICULATION_PARAM RAP2
                INNER JOIN RWA_DEV.RWA_ARTICULATION_PARAM RAP3
                   ON RAP2.THIRD_SUBJECT_NO = RAP3.EXPENSESUBJECT
                WHERE RAP2.ISGATHER = '1' --1:���ܵ����й���
                  AND RAP2.ARTICULATETYPE = '03' --��������
                  AND RAP2.ISINUSE = '1'
               ) RAP4
      ON REE.ACCSUBJECT1 = RAP4.THIRD_SUBJECT_NO
   GROUP BY RAP4.EXPENSESUBJECT, RAP4.EXPENSETYPE, REE.CURRENCY
   UNION ALL
  SELECT RAP4.EXPENSESUBJECT AS ACCSUBJECT1,
         REE.ORGID,
         REE.CURRENCY,
         CASE WHEN RAP4.EXPENSETYPE = '01' THEN NVL(SUM(REE.EXPENSERECEIVABLE), 0)
              ELSE NVL(SUM(REE.EXPENSERECEIVABLE), 0) END AS EXPOSE_BALANCE --��¶���
    FROM TEMP_RWA_EI_EXPOSURE REE
   INNER JOIN (SELECT DISTINCT RAP3.EXPENSESUBJECT,
                               RAP3.THIRD_SUBJECT_NO,
                               RAP3.EXPENSETYPE
                 FROM RWA_DEV.RWA_ARTICULATION_PARAM RAP2
                INNER JOIN RWA_DEV.RWA_ARTICULATION_PARAM RAP3
                   ON RAP2.THIRD_SUBJECT_NO = RAP3.EXPENSESUBJECT
                WHERE RAP2.ISGATHER = '0' --0:�����ܵ����й���
                  AND RAP2.ARTICULATETYPE = '03' --��������
                  AND RAP2.ISINUSE = '1'
               ) RAP4
      ON REE.ACCSUBJECT1 = RAP4.THIRD_SUBJECT_NO
   GROUP BY RAP4.EXPENSESUBJECT,REE.ORGID, RAP4.EXPENSETYPE, REE.CURRENCY
 ;
  COMMIT;

  --��ȡ�ʲ�֤ȯ���ʲ�
  INSERT INTO RWA_TMP_ABSBOND (SUBJECTNO,BALANCE,INTEREST,CURRENCY,ORGID)
  with tmp_abs_bond as
 (select bond_id, balance, interest
    from (select T1.bond_id as bond_id
                 ,RANK() OVER(PARTITION BY T1.bond_id ORDER BY T1.sort_seq DESC) AS RECORDNO
                 --INITIAL_COST�ɱ� + INT_ADJUST��Ϣ���� + MKT_VALUE_CHANGE���ʼ�ֵ�䶯/���ʼ�ֵ�䶯���� +ACCOUNTABLE_INTӦ����Ϣ
                 ,NVL(T1.INITIAL_COST, 0) + NVL(T1.INT_ADJUST, 0) + NVL(T1.MKT_VALUE_CHANGE, 0) + NVL(T1.ACCOUNTABLE_INT, 0) as balance
                 ,NVL(T1.RECEIVABLE_INT, 0) as interest --Ӧ����Ϣ
            from rwa_dev.fns_bnd_book_b T1
           inner join (select zqnm
                        from rwa.rwa_wsib_abs_issue_exposure
                       where dataDate = V_DATADATE
                      union
                      select zqnm
                        from rwa.rwa_wsib_abs_invest_exposure
                       where dataDate = V_DATADATE) T2
              on T1.bond_id = T2.ZQNM
           where T1.Datano = V_DATANO AND T1.AS_OF_DATE <= V_DATANO)
   where RECORDNO = 1)
select CASE
         WHEN T1.ASSET_CLASS = '10' THEN
          CASE
            WHEN T1.BOND_TYPE2 IN ('30', '50') THEN
             '11012001' --����������Ͷ�ʱ���
            ELSE
             '11010101' --������ծȯͶ�ʱ���
          END
         WHEN T1.ASSET_CLASS = '20' THEN
          CASE
            WHEN T1.BOND_TYPE2 IN ('30', '50') THEN
             '15012001' --���������������ʲ�����
            ELSE
             '15010101' --����������ծȯ�ʲ�����
          END
         WHEN T1.ASSET_CLASS = '40' THEN
          CASE
            WHEN T1.BOND_TYPE2 IN ('30', '50') THEN
             '15032001' --�ɹ����������ʲ�����
            ELSE
             '15030101' --�ɹ�����ծȯ�ʲ�����
          END
       END AS subject_no,
       T2.balance as balance,
       T2.interest as interest,
       NVL(T1.CURRENCY_CODE,'CNY') as currency,
       T1.DEPARTMENT as orgid
  from rwa_dev.fns_bnd_info_b T1
 inner join tmp_abs_bond T2
    on T1.bond_id = T2.bond_id
 where (T1.ASSET_CLASS = '20' OR
       (T1.ASSET_CLASS = '40' AND T1.BOND_TYPE2 NOT IN ('30', '50')) OR
       (T1.ASSET_CLASS = '40' AND T1.BOND_TYPE2 IN ('30', '50') AND
       T1.CLOSED = '1'))
   AND T1.DATANO = V_DATANO;

  COMMIT;
  
  --�����ʲ�֤ȯ�������ı����Ŀ���
  --����
  UPDATE RWA_DEV.RWA_TMP_EXPOBALANCE T
     SET T.EXPOSE_BALANCE = T.EXPOSE_BALANCE +
                            NVL((SELECT SUM(T1.BALANCE)
                               FROM RWA_DEV.RWA_TMP_ABSBOND T1
                              WHERE T1.SUBJECTNO = T.SUBJECT_NO
                                AND T1.CURRENCY = T.CURRENCY
                                AND T1.SUBJECTNO IN (
                                SELECT B.THIRD_SUBJECT_NO FROM RWA_DEV.RWA_ARTICULATION_PARAM B WHERE B.ISGATHER = '1'
                                )),0)
   WHERE EXISTS (SELECT 1
            FROM RWA_DEV.RWA_TMP_ABSBOND T1
           WHERE T1.SUBJECTNO = T.SUBJECT_NO
             AND T1.CURRENCY = T.CURRENCY
             AND T1.SUBJECTNO IN (
                                SELECT B.THIRD_SUBJECT_NO FROM RWA_DEV.RWA_ARTICULATION_PARAM B WHERE B.ISGATHER = '1'
                                )
             );

    COMMIT;
     --������
    UPDATE RWA_DEV.RWA_TMP_EXPOBALANCE T
     SET T.EXPOSE_BALANCE = T.EXPOSE_BALANCE +
                            NVL((SELECT SUM(T1.BALANCE)
                               FROM RWA_DEV.RWA_TMP_ABSBOND T1
                              WHERE T1.SUBJECTNO = T.SUBJECT_NO
                                AND T1.CURRENCY = T.CURRENCY
                                AND T1.ORGID = T.ORGID   --������ʱ�����˻����Ź�������
                                AND T1.SUBJECTNO IN (
                                SELECT B.THIRD_SUBJECT_NO FROM RWA_DEV.RWA_ARTICULATION_PARAM B WHERE B.ISGATHER = '0'
                                )),0)
   WHERE EXISTS (SELECT 1
            FROM RWA_DEV.RWA_TMP_ABSBOND T1
           WHERE T1.SUBJECTNO = T.SUBJECT_NO
             AND T1.CURRENCY = T.CURRENCY
             AND T1.ORGID = T.ORGID --������ʱ�����˻����Ź�������
             AND T1.SUBJECTNO IN (
                                SELECT B.THIRD_SUBJECT_NO FROM RWA_DEV.RWA_ARTICULATION_PARAM B WHERE B.ISGATHER = '0'
                                ));

    COMMIT;
  --�����ʲ�֤ȯ����������Ϣ��Ŀ���
  --����
 /* UPDATE RWA_dEV.Rwa_Tmp_Expobalance T
     SET T.EXPOSE_BALANCE = T.EXPOSE_BALANCE +
                            NVL((SELECT SUM(T1.INTEREST)
                                  FROM RWA_DEV.RWA_TMP_ABSBOND T1
                                 INNER JOIN RWA_DEV.RWA_ARTICULATION_PARAM T2
                                    ON T2.THIRD_SUBJECT_NO = T1.SUBJECTNO
                                 WHERE T2.INTERESTSUBJECT = T.SUBJECT_NO
                                   AND T1.CURRENCY = T.CURRENCY
                                   AND T1.ORGID = T.ORGID
                                   AND T2.INTERESTSUBJECT IN (
                                  SELECT B.THIRD_SUBJECT_NO FROM RWA_DEV.RWA_ARTICULATION_PARAM B WHERE B.ISGATHER = '0'
                                  )),
                                0)
   WHERE EXISTS (SELECT 1
            FROM RWA_DEV.RWA_TMP_ABSBOND T1
           INNER JOIN RWA_DEV.RWA_ARTICULATION_PARAM T2
              ON T2.THIRD_SUBJECT_NO = T1.SUBJECTNO
           WHERE T2.INTERESTSUBJECT = T.SUBJECT_NO
             AND T1.CURRENCY = T.CURRENCY
             AND T1.ORGID = T.ORGID
             AND T2.INTERESTSUBJECT IN (
                                  SELECT B.THIRD_SUBJECT_NO FROM RWA_DEV.RWA_ARTICULATION_PARAM B WHERE B.ISGATHER = '0'
                                  ));
    COMMIT;
    --������
     UPDATE RWA_dEV.Rwa_Tmp_Expobalance T
     SET T.EXPOSE_BALANCE = T.EXPOSE_BALANCE +
                            NVL((SELECT SUM(T1.INTEREST)
                                  FROM RWA_DEV.RWA_TMP_ABSBOND T1
                                 INNER JOIN RWA_DEV.RWA_ARTICULATION_PARAM T2
                                    ON T2.THIRD_SUBJECT_NO = T1.SUBJECTNO
                                 WHERE T2.INTERESTSUBJECT = T.SUBJECT_NO
                                   AND T1.CURRENCY = T.CURRENCY
                                   AND T2.INTERESTSUBJECT IN (
                                  SELECT B.THIRD_SUBJECT_NO FROM RWA_DEV.RWA_ARTICULATION_PARAM B WHERE B.ISGATHER = '1'
                                  )),.
                                0)
   WHERE EXISTS (SELECT 1
            FROM RWA_DEV.RWA_TMP_ABSBOND T1
           INNER JOIN RWA_DEV.RWA_ARTICULATION_PARAM T2
              ON T2.THIRD_SUBJECT_NO = T1.SUBJECTNO
           WHERE T2.INTERESTSUBJECT = T.SUBJECT_NO
             AND T1.CURRENCY = T.CURRENCY
             AND T2.INTERESTSUBJECT IN (
                                  SELECT B.THIRD_SUBJECT_NO FROM RWA_DEV.RWA_ARTICULATION_PARAM B WHERE B.ISGATHER = '1'
                                  ));
  COMMIT;
  */

   --��ȡ�����ʲ��ۼ���
  SELECT COUNT(1) INTO V_ILDDEBT FROM RWA_dEV.Rwa_Ei_Profitdist T WHERE T.DATADATE = V_DATADATE;

    IF V_ILDDEBT > 0 THEN
      SELECT T.ILDDEBT INTO V_ILDDEBT FROM RWA_dEV.Rwa_Ei_Profitdist T WHERE T.DATADATE = V_DATADATE;
    END IF;
  
  --���������ʲ���������ؿۼ���
  UPDATE RWA_DEV.RWA_TMP_EXPOBALANCE T
     SET T.EXPOSE_BALANCE = T.EXPOSE_BALANCE + V_ILDDEBT
   WHERE T.SUBJECT_NO = '17010000'
     AND T.CURRENCY = 'CNY'
     AND T.ORGID = '9998';
  COMMIT;
  --
  INSERT INTO RWA_DEV.RWA_TMP_GLBALANCE02 (
        SUBJECT_NO
        ,ORGID
        ,CURRENCY
        ,IOFLAG
        ,RETAILFLAG
        ,ACCOUNT_BALANCE
  )
  SELECT T3.SUBJECT_NO,
         '9998' AS ORGID --���˻���
        ,T3.CURRENCY --����
        ,DECODE(REGEXP_INSTR(T3.SUBJECT_NO, '^[123456]'), 1, '01', '02') AS IOFLAG --���ڱ����ʶ
        ,RAP.RETAILFLAG --���۱�ʶ
        ,SUM(T3.ACCOUNT_BALANCE) AS ACCOUNT_BALANCE
    FROM (SELECT ORGID, CURRENCY, SUBJECT_NO, ACCOUNT_BALANCE
            FROM RWA_DEV.RWA_TMP_GLBALANCE) T3
   INNER JOIN RWA_DEV.RWA_ARTICULATION_PARAM RAP
      ON T3.SUBJECT_NO = RAP.THIRD_SUBJECT_NO
     AND RAP.ARTICULATETYPE <> '04' --�Ƿ񹴻� 04:������
     AND RAP.ISINUSE = '1' --����״̬ 1���� 0ͣ��
     AND RAP.ISGATHER = '1' --�Ƿ���ܵ����� 1:��
   GROUP BY T3.SUBJECT_NO, T3.CURRENCY, RAP.RETAILFLAG
  UNION ALL
 SELECT T3.SUBJECT_NO,
        T3.ORGID --���˻���
       ,T3.CURRENCY --����
       ,DECODE(REGEXP_INSTR(T3.SUBJECT_NO, '^[123456]'), 1, '01', '02') AS IOFLAG --���ڱ����ʶ
       ,RAP.RETAILFLAG --���۱�ʶ
       ,T3.ACCOUNT_BALANCE
   FROM (SELECT T2.ORGID, T2.CURRENCY, T2.SUBJECT_NO, T2.ACCOUNT_BALANCE
           FROM RWA_DEV.RWA_TMP_GLBALANCE T2) T3
  INNER JOIN RWA_DEV.RWA_ARTICULATION_PARAM RAP
     ON T3.SUBJECT_NO = RAP.THIRD_SUBJECT_NO
    AND RAP.ARTICULATETYPE <> '04' --�Ƿ񹴻� 04:������
    AND RAP.ISINUSE = '1' --����״̬ 1���� 0ͣ��
    AND RAP.ISGATHER = '0' --�Ƿ���ܵ����� 0:��
  ;

  COMMIT;

    /*����Ŀ���RWA_DEV.RWA_ARTICULATION_RESULT*/
  INSERT INTO RWA_DEV.RWA_ARTICULATION_RESULT(
                DATADATE                      --��������
                ,SUBJECT_NO                   --��Ŀ��
                ,ORGID                        --���������
                ,ORGNAME                      --�����������
                ,CURRENCY                     --����
                ,EXPOSE_BALANCE               --��¶���
                ,ACCOUNT_BALANCE              --���˽��
                ,MINUS_BALANCE                --������
                ,MINUS_RATE                   --������
                ,TOLERANCE                    --���̶�
                ,ISTOLERATE                   --�Ƿ����� 1������ 0��������
                ,IOFLAG                       --����/�����ʶ 01���� 02����
                ,RETAILFLAG                    --�Թ����۱�ʶ 0�Թ� 1����
                ,ORGSORTNO                    --���������
                ,ISNETTING
    )
   SELECT V_DATADATE AS DATADATE --��������
         , SUBJECT_NO AS SUBJECT_NO --��Ŀ
         , ORGID AS ORGID
         , ORGNAME AS ORGNAME
         , CURRENCY AS CURRENCY --����
         , EXPOSE_BALANCE AS EXPOSE_BALANCE --��¶���
         , ACCOUNT_BALANCE AS ACCOUNT_BALANCE --�������
         , ABS(ACCOUNT_BALANCE) - ABS(EXPOSE_BALANCE) AS MINUS_BALANCE --������
         , CASE WHEN ACCOUNT_BALANCE = 0 THEN
             CASE WHEN EXPOSE_BALANCE = 0 THEN 0 ELSE 1 END
            ELSE
             CASE WHEN EXPOSE_BALANCE = 0 THEN 1
               ELSE
                ABS(ABS(ACCOUNT_BALANCE) - ABS(EXPOSE_BALANCE)) /
                ABS(ACCOUNT_BALANCE)
             END
          END AS MINUS_RATE --������
         , TOLERANCE AS TOLERANCE --���̶�
         , CASE WHEN ACCOUNT_BALANCE = 0 THEN CASE WHEN EXPOSE_BALANCE = 0 THEN  '1' ELSE '0' END
               ELSE CASE WHEN EXPOSE_BALANCE = 0 THEN '0' ELSE
                DECODE(SIGN(ABS(ABS(ACCOUNT_BALANCE) - ABS(EXPOSE_BALANCE)) - TOLERANCE * ABS(ACCOUNT_BALANCE)), -1, '1', 0, '1', '0')
             END
          END AS ISTOLERATE --�Ƿ�����
         , IOFLAG AS IOFLAG --����/�����ʶ
         , RETAILFLAG       --�Թ����۱�ʶ
         , SORTNO
         ,'1' AS ISNETTING --�����ʶ
     FROM (
           SELECT DISTINCT
                  TEMP02.SUBJECT_NO
                  ,TEMP02.ORGID
                  ,OI.ORGNAME
                  ,OI.SORTNO
                  ,TEMP02.CURRENCY
                  ,TEMP02.IOFLAG
                  ,TEMP02.RETAILFLAG
                  ,NVL(TEMP01.EXPOSE_BALANCE, 0) AS EXPOSE_BALANCE
                  ,NVL(TEMP02.ACCOUNT_BALANCE, 0) AS ACCOUNT_BALANCE --���˽��
                  ,DECODE(TEMP02.IOFLAG, '01', V_INTOLERANCE, V_OUTTOLERANCE) AS TOLERANCE --���̶�
             FROM RWA_DEV.RWA_TMP_GLBALANCE02 TEMP02
             LEFT JOIN RWA_DEV.RWA_TMP_EXPOBALANCE TEMP01
               ON TEMP02.SUBJECT_NO = TEMP01.SUBJECT_NO
              AND TEMP02.ORGID = TEMP01.ORGID
              AND TEMP02.CURRENCY = TEMP01.CURRENCY
             LEFT JOIN RWA.ORG_INFO OI
               ON OI.ORGID = TEMP02.ORGID
          )
    WHERE  SUBJECT_NO not in ('11012002','11012003','11012004') -------20190826  BY WZB  �����Ŀ ������Ϣ���������
       AND( 
        EXPOSE_BALANCE <> 0
       OR ACCOUNT_BALANCE <> 0
       );
    COMMIT;

    --���ֱ��ֶ��˶Բ��ϵĿ�Ŀ�����ֱ��ֶ����Ƿ��ܹ�����
    MERGE INTO RWA_DEV.RWA_ARTICULATION_RESULT RAR
    USING (SELECT TEMP02.ORGID,
                  TEMP02.SUBJECT_NO,
                   CASE WHEN SUM(TEMP02.ACCOUNT_BALANCE) - SUM(TEMP01.EXPOSE_BALANCE) <> 0 AND SUM(TEMP01.EXPOSE_BALANCE) <> 0
                          THEN '0'
                        WHEN SUM(TEMP02.ACCOUNT_BALANCE) - SUM(TEMP01.EXPOSE_BALANCE) = 0
                          THEN '0'
                        ELSE '1' END AS ISNETTING
             FROM (select subject_no, orgid,
                          sum(t.account_balance) as ACCOUNT_BALANCE
                     from RWA_DEV.Rwa_Tmp_Glbalance02 t
                    group by subject_no, orgid) TEMP02
             LEFT JOIN (select subject_no, orgid,
                              sum(expose_balance) as expose_balance
                         from RWA_DEV.RWA_TMP_EXPOBALANCE
                        group by subject_no, orgid) TEMP01
               ON TEMP02.SUBJECT_NO = TEMP01.SUBJECT_NO
              AND TEMP02.ORGID = TEMP01.ORGID
            WHERE EXISTS (SELECT 1 FROM (
                        /* SELECT ORGID, SUBJECT_NO
                             FROM (SELECT T.ORGID, T.SUBJECT_NO, T.CURRENCY
                                     FROM RWA_DEV.RWA_ARTICULATION_RESULT T
                                    WHERE T.DATADATE = TO_DATE(P_DATA_DT_STR, 'yyyymmdd')
                                      AND T.MINUS_BALANCE <> 0)
                            GROUP BY ORGID, SUBJECT_NO
                           HAVING COUNT(1) > 1 */
                           SELECT T.ORGID, T.SUBJECT_NO
                               FROM RWA_DEV.RWA_ARTICULATION_RESULT T
                              WHERE T.DATADATE =v_datadate --TO_DATE('20170630', 'yyyymmdd')
                                AND T.MINUS_BALANCE <> 0
                           GROUP BY ORGID, SUBJECT_NO
                             HAVING COUNT(SUBJECT_NO) > 1
                           ) TEMP03
                    WHERE TEMP03.ORGID = TEMP02.ORGID
                      AND TEMP03.SUBJECT_NO = TEMP02.SUBJECT_NO)
            GROUP BY TEMP02.ORGID, TEMP02.SUBJECT_NO) RAR2
    ON (RAR.DATADATE = TO_DATE(P_DATA_DT_STR, 'yyyymmdd') AND RAR.SUBJECT_NO = RAR2.SUBJECT_NO AND RAR.ORGID = RAR2.ORGID)
    WHEN MATCHED THEN
      UPDATE SET RAR.ISNETTING = RAR2.ISNETTING;

    COMMIT;

    /*����Ŀ���RWA_DEV.RWA_EI_CONTRACT*/
   INSERT INTO RWA_DEV.RWA_EI_CONTRACT(
               DataDate                             --��������
              ,DataNo                               --������ˮ��
              ,ContractID                           --��ͬID
              ,SContractID                          --Դ��ͬID
              ,SSysID                               --ԴϵͳID
              ,ClientID                             --��������ID
              ,SOrgID                               --Դ����ID
              ,SOrgName                             --Դ��������
              ,OrgSortNo                            --�������������
              ,OrgID                                --��������ID
              ,OrgName                              --������������
              ,IndustryID                           --������ҵ����
              ,IndustryName                         --������ҵ����
              ,BusinessLine                         --ҵ������
              ,AssetType                            --�ʲ�����
              ,AssetSubType                         --�ʲ�С��
              ,BusinessTypeID                       --ҵ��Ʒ�ִ���
              ,BusinessTypeName                     --ҵ��Ʒ������
              ,CreditRiskDataType                   --���÷�����������
              ,StartDate                            --��ʼ����
              ,DueDate                              --��������
              ,OriginalMaturity                     --ԭʼ����
              ,ResidualM                            --ʣ������
              ,SettlementCurrency                   --�������
              ,ContractAmount                       --��ͬ�ܽ��
              ,NotExtractPart                       --��ͬδ��ȡ����
              ,UncondCancelFlag                     --�Ƿ����ʱ����������
              ,ABSUAFlag                            --�ʲ�֤ȯ�������ʲ���ʶ
              ,ABSPoolID                            --֤ȯ���ʲ���ID
              ,GroupID                              --������
              ,GUARANTEETYPE                        --��Ҫ������ʽ
              ,ABSPROPORTION                        --�ʲ�֤ȯ������
    )
    SELECT
          V_DATADATE                                       AS DATADATE
          ,V_DATANO                                        AS DATANO
          ,'GC-' || RAR.SUBJECT_NO || '-' || RAR.ORGID || '-' || RAR.CURRENCY
                                                           AS CONTRACTID
          ,'GC-' || RAR.SUBJECT_NO || '-' || RAR.ORGID || '-' || RAR.CURRENCY
                                                           AS SCONTRACTID
          ,'GC'                                            AS SSYSID
          ,'GC-' || RAR.SUBJECT_NO || '-' || RAR.ORGID || '-' || RAR.CURRENCY
                                                           AS CLIENTID
          ,RAR.ORGID                                       AS SORGID
          ,RAR.ORGNAME                                     AS SORGNAME
          ,OI.SORTNO                                       AS ORGSORTNO            --���������
          ,RAR.ORGID                                       AS ORGID
          ,RAR.ORGNAME                                     AS ORGNAME
          ,'999999'                                        AS INDUSTRYID
          ,'δ֪'                                          AS INDUSTRYNAME
          ,'0501'                                          AS BUSINESSLINE --���� 0501:����
          ,''                                              AS ASSETTYPE
          ,''                                              AS ASSETSUBTYPE
          ,'9010101010'                                    AS BUSINESSTYPEID
          ,'����ҵ��Ʒ��'                                  AS BUSINESSTYPENAME
          ,DECODE(RAP.RETAILFLAG,1,'02' ,'01' )            AS CREDITRISKDATATYPE
          ,V_STARTDATE                                     AS STARTDATE
          ,TO_CHAR(ADD_MONTHS(V_DATADATE, 6) ,'yyyy-mm-dd')AS DUEDATE
          ,0.5                                             AS ORIGINALMATURITY
          ,0.5                                             AS RESIDUALM
          ,RAR.CURRENCY                                    AS SETTLEMENTCURRENCY
          ,RAR.MINUS_BALANCE                               AS CONTRACTAMOUNT
          ,0                                               AS NOTEXTRACTPART         --��ͬδ��ȡ����     (Ĭ��Ϊ0)
          ,'0'                                             AS UNCONDCANCELFLAG       --�Ƿ����ʱ����������(Ĭ��Ϊ��0�� 1��)
          ,'0'                                             AS ABSUAFLAG
          ,''                                              AS ABSPOOLID
          ,''                                              AS GROUPID                --������         (ԴϵͳID)
          ,''                                              AS GUARANTEETYPE
          ,1                                               AS ABSPROPORTION
       FROM RWA_DEV.RWA_ARTICULATION_RESULT RAR
      INNER JOIN RWA_DEV.RWA_ARTICULATION_PARAM RAP
         ON RAR.SUBJECT_NO = RAP.THIRD_SUBJECT_NO
        AND RAP.ISNETTING = '1' --�Ƿ����� 0�� 1��
        AND RAP.ISINUSE = '1' --����״̬ 1���� 0ͣ��
       LEFT JOIN RWA.ORG_INFO OI
         ON OI.ORGID = RAR.ORGID
      WHERE RAR.DATADATE = V_DATADATE
           --AND RAR.ISTOLERATE = '0'                     --�Ƿ����� 1������ 0��������
        AND RAR.MINUS_BALANCE > 0
        AND RAR.ISNETTING = '1'
     ;

    COMMIT;

     /*����Ŀ���RWA_DEV.RWA_EI_CONTRACT*/
   INSERT INTO RWA_DEV.RWA_EI_CONTRACT(
               DataDate                             --��������
              ,DataNo                               --������ˮ��
              ,ContractID                           --��ͬID
              ,SContractID                          --Դ��ͬID
              ,SSysID                               --ԴϵͳID
              ,ClientID                             --��������ID
              ,SOrgID                               --Դ����ID
              ,SOrgName                             --Դ��������
              ,OrgSortNo                            --�������������
              ,OrgID                                --��������ID
              ,OrgName                              --������������
              ,IndustryID                           --������ҵ����
              ,IndustryName                         --������ҵ����
              ,BusinessLine                         --ҵ������
              ,AssetType                            --�ʲ�����
              ,AssetSubType                         --�ʲ�С��
              ,BusinessTypeID                       --ҵ��Ʒ�ִ���
              ,BusinessTypeName                     --ҵ��Ʒ������
              ,CreditRiskDataType                   --���÷�����������
              ,StartDate                            --��ʼ����
              ,DueDate                              --��������
              ,OriginalMaturity                     --ԭʼ����
              ,ResidualM                            --ʣ������
              ,SettlementCurrency                   --�������
              ,ContractAmount                       --��ͬ�ܽ��
              ,NotExtractPart                       --��ͬδ��ȡ����
              ,UncondCancelFlag                     --�Ƿ����ʱ����������
              ,ABSUAFlag                            --�ʲ�֤ȯ�������ʲ���ʶ
              ,ABSPoolID                            --֤ȯ���ʲ���ID
              ,GroupID                              --������
              ,GUARANTEETYPE                        --��Ҫ������ʽ
              ,ABSPROPORTION                        --�ʲ�֤ȯ������
    )
    SELECT
          V_DATADATE                                       AS DATADATE
          ,V_DATANO                                        AS DATANO
          ,'GC-' || B1.SUBJECT_NO || '-' || B1.ORGID || '-' || B1.CURRENCY
                                                           AS CONTRACTID
          ,'GC-' || B1.SUBJECT_NO || '-' || B1.ORGID || '-' || B1.CURRENCY
                                                           AS SCONTRACTID
          ,'GC'                                            AS SSYSID
          ,'GC-'||B1.SUBJECT_NO || '-' || B1.ORGID || '-' || B1.CURRENCY
                                                           AS CLIENTID
          ,OI.ORGID                                        AS SORGID
          ,OI.ORGNAME                                      AS SORGNAME
          ,OI.SORTNO                                       AS ORGSORTNO            --���������
          ,OI.ORGID                                        AS ORGID
          ,OI.ORGNAME                                      AS ORGNAME
          ,'999999'                                        AS INDUSTRYID
          ,'δ֪'                                           AS INDUSTRYNAME
          ,'0501'                                          AS BUSINESSLINE
          ,''                                              AS ASSETTYPE
          ,''                                              AS ASSETSUBTYPE
          ,'9010101010'                                    AS BUSINESSTYPEID
          ,'����ҵ��Ʒ��'                                  AS BUSINESSTYPENAME
          ,DECODE(RAP.RETAILFLAG,1,'02' ,'01' )            AS CREDITRISKDATATYPE
          ,V_STARTDATE                                     AS STARTDATE
          ,TO_CHAR(ADD_MONTHS(V_DATADATE, 6) ,'yyyy-mm-dd')AS DUEDATE
          ,0.5                                             AS ORIGINALMATURITY
          ,0.5                                             AS RESIDUALM
          ,b1.CURRENCY                                     AS SETTLEMENTCURRENCY
          ,b1.MINUS_BALANCE                                AS CONTRACTAMOUNT
          ,0                                               AS NOTEXTRACTPART         --��ͬδ��ȡ����     (Ĭ��Ϊ0)
          ,'0'                                             AS UNCONDCANCELFLAG       --�Ƿ����ʱ����������(Ĭ��Ϊ��0�� 1��)
          ,'0'                                             AS ABSUAFLAG
          ,''                                              AS ABSPOOLID
          ,''                                              AS GROUPID                --������         (ԴϵͳID)
          ,''                                              AS GUARANTEETYPE
          ,1                                               AS ABSPROPORTION
       FROM (SELECT
               TEMP02.SUBJECT_NO,
               TEMP02.ORGID,
               'CNY' as CURRENCY,
               SUM(TEMP02.ACCOUNT_BALANCE) - SUM(TEMP01.EXPOSE_BALANCE) as MINUS_BALANCE,
               CASE
                 WHEN SUM(TEMP02.ACCOUNT_BALANCE) - SUM(TEMP01.EXPOSE_BALANCE) <> 0 AND SUM(TEMP01.EXPOSE_BALANCE) <> 0
                   THEN '0'
                 WHEN SUM(TEMP02.ACCOUNT_BALANCE) - SUM(TEMP01.EXPOSE_BALANCE) = 0
                   THEN '0'
                 ELSE '1'
               END AS ISNETTING
          FROM (select subject_no,
                       orgid,
                       sum(t.account_balance) as ACCOUNT_BALANCE
                  from RWA_DEV.Rwa_Tmp_Glbalance02 t
                 group by subject_no, orgid) TEMP02
          LEFT JOIN (select subject_no,
                           orgid,
                           sum(expose_balance) as expose_balance
                      from RWA_DEV.RWA_TMP_EXPOBALANCE
                     group by subject_no, orgid) TEMP01
            ON TEMP02.SUBJECT_NO = TEMP01.SUBJECT_NO
           AND TEMP02.ORGID = TEMP01.ORGID
          LEFT JOIN RWA_ARTICULATION_PARAM RAP
            ON RAP.THIRD_SUBJECT_NO = TEMP02.SUBJECT_NO
         WHERE EXISTS (SELECT 1
                  FROM (
                  /*SELECT ORGID, SUBJECT_NO
                          FROM (SELECT T.ORGID, T.SUBJECT_NO, T.CURRENCY
                                  FROM RWA_DEV.RWA_ARTICULATION_RESULT T
                                 WHERE T.DATADATE =
                                       TO_DATE(P_DATA_DT_STR, 'yyyymmdd')
                                   AND T.MINUS_BALANCE <> 0)
                         GROUP BY ORGID, SUBJECT_NO
                        HAVING COUNT(1) > 1 */
                        SELECT T.ORGID, T.SUBJECT_NO
                               FROM RWA_DEV.RWA_ARTICULATION_RESULT T
                              WHERE T.DATADATE = TO_DATE('20170630', 'yyyymmdd')
                                AND T.MINUS_BALANCE <> 0
                           GROUP BY ORGID, SUBJECT_NO
                             HAVING COUNT(SUBJECT_NO) > 1 ) TEMP03
                 WHERE TEMP03.ORGID = TEMP02.ORGID
                   AND TEMP03.SUBJECT_NO = TEMP02.SUBJECT_NO)
         GROUP BY TEMP02.ORGID, TEMP02.SUBJECT_NO) B1
  LEFT JOIN RWA_DEV.Rwa_Articulation_Param RAP
    ON RAP.THIRD_SUBJECT_NO = B1.SUBJECT_NO
  left join rwa.org_info oi
   on oi.orgid = b1.orgid
 where B1.ISNETTING = 0
   and B1.minus_balance > 0
 ;
    COMMIT;

  /*����Ŀ���RWA_DEV.RWA_EI_EXPOSURE*/
    INSERT INTO RWA_DEV.RWA_EI_EXPOSURE(
                 DataDate                                                     --��������
                ,DataNo                                                       --������ˮ��
                ,ExposureID                                                   --���ձ�¶ID
                ,DueID                                                        --ծ��ID
                ,SSysID                                                       --ԴϵͳID
                ,ContractID                                                   --��ͬID
                ,ClientID                                                     --��������ID
                ,SOrgID                                                       --Դ����ID
                ,SOrgName                                                     --Դ��������
                ,OrgSortNo                                                    --�������������
                ,OrgID                                                        --��������ID
                ,OrgName                                                      --������������
                ,AccOrgID                                                     --�������ID
                ,AccOrgName                                                   --�����������
                ,IndustryID                                                   --������ҵ����
                ,IndustryName                                                 --������ҵ����
                ,BusinessLine                                                 --ҵ������
                ,AssetType                                                    --�ʲ�����
                ,AssetSubType                                                 --�ʲ�С��
                ,BusinessTypeID                                               --ҵ��Ʒ�ִ���
                ,BusinessTypeName                                             --ҵ��Ʒ������
                ,CreditRiskDataType                                           --���÷�����������
                ,AssetTypeOfHaircuts                                          --�ۿ�ϵ����Ӧ�ʲ����
                ,BusinessTypeSTD                                              --Ȩ�ط�ҵ������
                ,ExpoClassSTD                                                 --Ȩ�ط���¶����
                ,ExpoSubClassSTD                                              --Ȩ�ط���¶С��
                ,ExpoClassIRB                                                 --��������¶����
                ,ExpoSubClassIRB                                              --��������¶С��
                ,ExpoBelong                                                   --��¶������ʶ
                ,BookType                                                     --�˻����
                ,ReguTranType                                                 --��ܽ�������
                ,RepoTranFlag                                                 --�ع����ױ�ʶ
                ,RevaFrequency                                                --�ع�Ƶ��
                ,Currency                                                     --����
                ,NormalPrincipal                                              --�����������
                ,OverdueBalance                                               --�������
                ,NonAccrualBalance                                            --��Ӧ�����
                ,OnSheetBalance                                               --�������
                ,NormalInterest                                               --������Ϣ
                ,OnDebitInterest                                              --����ǷϢ
                ,OffDebitInterest                                             --����ǷϢ
                ,ExpenseReceivable                                            --Ӧ�շ���
                ,AssetBalance                                                 --�ʲ����
                ,AccSubject1                                                  --��Ŀһ
                ,AccSubject2                                                  --��Ŀ��
                ,AccSubject3                                                  --��Ŀ��
                ,StartDate                                                    --��ʼ����
                ,DueDate                                                      --��������
                ,OriginalMaturity                                             --ԭʼ����
                ,ResidualM                                                    --ʣ������
                ,RiskClassify                                                 --���շ���
                ,ExposureStatus                                               --���ձ�¶״̬
                ,OverdueDays                                                  --��������
                ,SpecialProvision                                             --ר��׼����
                ,GeneralProvision                                             --һ��׼����
                ,EspecialProvision                                            --�ر�׼����
                ,WrittenOffAmount                                             --�Ѻ������
                ,OffExpoSource                                                --���Ⱪ¶��Դ
                ,OffBusinessType                                              --����ҵ������
                ,OffBusinessSdvsSTD                                           --Ȩ�ط�����ҵ������ϸ��
                ,UncondCancelFlag                                             --�Ƿ����ʱ����������
                ,CCFLevel                                                     --����ת��ϵ������
                ,CCFAIRB                                                      --�߼�������ת��ϵ��
                ,ClaimsLevel                                                  --ծȨ����
                ,BondFlag                                                     --�Ƿ�Ϊծȯ
                ,BondIssueIntent                                              --ծȯ����Ŀ��
                ,NSURealPropertyFlag                                          --�Ƿ�����ò�����
                ,RepAssetTermType                                             --��ծ�ʲ���������
                ,DependOnFPOBFlag                                             --�Ƿ�����������δ��ӯ��
                ,IRating                                                      --�ڲ�����
                ,PD                                                           --ΥԼ����
                ,LGDLevel                                                     --ΥԼ��ʧ�ʼ���
                ,LGDAIRB                                                      --�߼���ΥԼ��ʧ��
                ,MAIRB                                                        --�߼�����Ч����
                ,EADAIRB                                                      --�߼���ΥԼ���ձ�¶
                ,DefaultFlag                                                  --ΥԼ��ʶ
                ,BEEL                                                         --��ΥԼ��¶Ԥ����ʧ����
                ,DefaultLGD                                                   --��ΥԼ��¶ΥԼ��ʧ��
                ,EquityExpoFlag                                               --��Ȩ��¶��ʶ
                ,EquityInvestType                                             --��ȨͶ�ʶ�������
                ,EquityInvestCause                                            --��ȨͶ���γ�ԭ��
                ,SLFlag                                                       --רҵ�����ʶ
                ,SLType                                                       --רҵ��������
                ,PFPhase                                                      --��Ŀ���ʽ׶�
                ,ReguRating                                                   --�������
                ,CBRCMPRatingFlag                                             --������϶������Ƿ��Ϊ����
                ,LargeFlucFlag                                                --�Ƿ񲨶��Խϴ�
                ,LiquExpoFlag                                                 --�Ƿ���������з��ձ�¶
                ,PaymentDealFlag                                              --�Ƿ����Ը�ģʽ
                ,DelayTradingDays                                             --�ӳٽ�������
                ,SecuritiesFlag                                               --�м�֤ȯ��ʶ
                ,SecuIssuerID                                                 --֤ȯ������ID
                ,RatingDurationType                                           --������������
                ,SecuIssueRating                                              --֤ȯ���еȼ�
                ,SecuResidualM                                                --֤ȯʣ������
                ,SecuRevaFrequency                                            --֤ȯ�ع�Ƶ��
                ,CCPTranFlag                                                  --�Ƿ����뽻�׶�����ؽ���
                ,CCPID                                                        --���뽻�׶���ID
                ,QualCCPFlag                                                  --�Ƿ�ϸ����뽻�׶���
                ,BankRole                                                     --���н�ɫ
                ,ClearingMethod                                               --���㷽ʽ
                ,BankAssetFlag                                                --�Ƿ������ύ�ʲ�
                ,MatchConditions                                              --�����������
                ,SFTFlag                                                      --֤ȯ���ʽ��ױ�ʶ
                ,MasterNetAgreeFlag                                           --���������Э���ʶ
                ,MasterNetAgreeID                                             --���������Э��ID
                ,SFTType                                                      --֤ȯ���ʽ�������
                ,SecuOwnerTransFlag                                           --֤ȯ����Ȩ�Ƿ�ת��
                ,OTCFlag                                                      --�����������߱�ʶ
                ,ValidNettingFlag                                             --��Ч�������Э���ʶ
                ,ValidNetAgreementID                                          --��Ч�������Э��ID
                ,OTCType                                                      --����������������
                ,DepositRiskPeriod                                            --��֤������ڼ�
                ,MTM                                                          --���óɱ�
                ,MTMCurrency                                                  --���óɱ�����
                ,BuyerOrSeller                                                --������
                ,QualROFlag                                                   --�ϸ�����ʲ���ʶ
                ,ROIssuerPerformFlag                                          --�����ʲ��������Ƿ�����Լ
                ,BuyerInsolvencyFlag                                          --���ñ������Ƿ��Ʋ�
                ,NonpaymentFees                                               --��δ֧������
                ,RetailExpoFlag                                               --���۱�¶��ʶ
                ,RetailClaimType                                              --����ծȨ����
                ,MortgageType                                                 --ס����Ѻ��������
                ,ExpoNumber                                                   --���ձ�¶����
                ,LTV                                                          --�����ֵ��
                ,Aging                                                        --����
                ,NewDefaultDebtFlag                                           --����ΥԼծ���ʶ
                ,PDPoolModelID                                                --PD�ֳ�ģ��ID
                ,LGDPoolModelID                                               --LGD�ֳ�ģ��ID
                ,CCFPoolModelID                                               --CCF�ֳ�ģ��ID
                ,PDPoolID                                                     --����PD��ID
                ,LGDPoolID                                                    --����LGD��ID
                ,CCFPoolID                                                    --����CCF��ID
                ,ABSUAFlag                                                    --�ʲ�֤ȯ�������ʲ���ʶ
                ,ABSPoolID                                                    --֤ȯ���ʲ���ID
                ,GroupID                                                      --������
                ,DefaultDate                                                  --ΥԼʱ��
                ,ABSPROPORTION                                                --�ʲ�֤ȯ������
                ,DEBTORNUMBER                                                 --����˸���
    )
    SELECT
                V_DATADATE                                      AS DATADATE                     --��������
                ,DATANO                                         AS DATANO                       --������ˮ��
                ,REC.CONTRACTID                                 AS EXPOSUREID                   --���ձ�¶ID
                ,REC.CONTRACTID                                 AS DUEID                        --ծ��ID
                ,REC.SSYSID                                     AS SSYSID                       --ԴϵͳID
                ,REC.CONTRACTID                                 AS CONTRACTID                   --��ͬID
                ,REC.CLIENTID                                   AS CLIENTID                     --��������ID
                ,REC.ORGID                                    AS SORGID                       --Դ����ID
                ,REC.ORGNAME                                    AS SORGNAME                     --Դ��������
                ,REC.ORGSORTNO                                 AS ORGSORTNO                    --���������
                ,REC.ORGID                                  AS ORGID                        --��������ID
                ,REC.ORGNAME                                    AS ORGNAME                      --������������
                ,REC.ORGID                                     AS ACCORGID                     --�������ID
                ,REC.ORGNAME                                AS ACCORGNAME                   --�����������
                ,REC.INDUSTRYID                                 AS INDUSTRYID                   --������ҵ����
                ,REC.INDUSTRYNAME                               AS INDUSTRYNAME                 --������ҵ����
                ,REC.BUSINESSLINE                               AS BUSSINESSLINE                --����
                ,REC.ASSETTYPE                                  AS ASSETTYPE                    --�ʲ�����
                ,REC.ASSETSUBTYPE                               AS ASSETSUBTYPE                 --�ʲ�С��
                ,'9010101010'                                   AS BUSINESSTYPEID               --ҵ��Ʒ�ִ���
                ,'����ҵ��Ʒ��'                                 AS BUSINESSTYPENAME             --ҵ��Ʒ������
                ,DECODE(RAP.RETAILFLAG,1,'02','01')             AS CREDITRISKDATATYPE           --���÷�����������(01:һ�������,02:һ������)
                ,'01'                                           AS ASSETTYPEOFHAIRCUTS          --�ۿ�ϵ����Ӧ�ʲ����
                ,DECODE(RAP.RETAILFLAG,1 ,'06' ,'07')           AS BUSINESSTYPESTD              --Ȩ�ط�ҵ������(�Թ� һ���ʲ�07 ���� ����06)
                ,''                                             AS EXPOCLASSSTD                 --Ȩ�ط���¶����(�Թ� 0106 ���� 0108)
                ,''                                             AS EXPOSUBCLASSSTD              --Ȩ�ط���¶С��(�Թ� 010601 ���� 010803)
                ,''                                             AS EXPOCLASSIRB                 --��������¶����(�Թ� 0203 ���� 0204)
                ,''                                             AS EXPOSUBCLASSIRB              --��������¶С��(�Թ� 020301 ���� 020403)
                ,DECODE(
                      REGEXP_INSTR(RAP.THIRD_SUBJECT_NO ,'^[123456]')
                      ,1
                      ,'01'
                      ,'02'
                  )                                             AS EXPOBELONG                   --��¶������ʶ((01:����;02:һ�����;03:���׶���;))
                ,'01'                                           AS BOOKTYPE                     --�˻����(�̶�ֵ"�����˻�",01:�����˻�,02:�����˻�)
                ,'03'                                           AS REGUTRANTYPE                 --��ܽ�������(�̶�ֵ"��Ѻ����",01:�ع�����;02:�����ʱ��г�����;03:��Ѻ����;)
                ,'0'                                            AS REPOTRANFLAG                 --�ع����ױ�ʶ(�̶�ֵΪ"��" 0)
                ,1                                              AS REVAFREQUENCY                --�ع�Ƶ��
                ,REC.SETTLEMENTCURRENCY                         AS CURRENCY                     --����
                ,REC.CONTRACTAMOUNT                             AS NORMALPRINCIPAL              --�����������
                ,0                                              AS OVERDUEBALANCE               --�������
                ,0                                              AS NONACCRUALBALANCE            --��Ӧ�����
                ,REC.CONTRACTAMOUNT                             AS ONSHEETBALANCE               --�������(�����������+�������+��Ӧ�����)
                ,0                                              AS NORMALINTEREST               --������Ϣ
                ,0                                              AS ONDEBITINTEREST              --����ǷϢ
                ,0                                              AS OFFDEBITINTEREST             --����ǷϢ
                ,0                                              AS EXPENSERECEIVABLE            --Ӧ�շ���
                ,REC.CONTRACTAMOUNT                             AS ASSETBALANCE                 --�ʲ����
                ,RAP.THIRD_SUBJECT_NO                           AS ACCSUBJECT1                  --��Ŀһ
                ,NULL                                           AS ACCSUBJECT2                  --��Ŀ��
                ,NULL                                           AS ACCSUBJECT3                  --��Ŀ��
                ,REC.STARTDATE                                  AS STARTDATE                    --��ʼ����
                ,REC.DUEDATE                                    AS DUEDATE                      --��������
                ,REC.ORIGINALMATURITY                           AS ORIGINALMATURITY             --ԭʼ����
                ,REC.RESIDUALM                                  AS RESIDUALM                    --ʣ������
                ,'01'                                           AS RISKCLASSIFY                 --���շ���(01����,02��ע,03�μ�,04����,05��ʧ)
                ,'01'                                           AS EXPOSURESTATUS               --���ձ�¶״̬(01����������02��������)
                ,0                                              AS OVERDUEDAYS                  --��������
                ,0                                              AS SPECIALPROVISION             --ר��׼����
                ,0                                              AS GENERALPROVISION             --һ��׼����
                ,0                                              AS ESPECIALPROVISION            --�ر�׼����
                ,0                                              AS WRITTENOFFAMOUNT             --�Ѻ������
                ,DECODE(
                        REGEXP_INSTR(RAP.THIRD_SUBJECT_NO ,'^[123456]')
                        ,1
                        ,''
                        ,'03'
                       )                                        AS OFFEXPOSOURCE                --���Ⱪ¶��Դ
                ,''                                             AS OFFBUSINESSTYPE              --����ҵ������
                ,''                                             AS OFFBUSINESSSDVSSTD           --Ȩ�ط�����ҵ������ϸ��
                ,'0'                                            AS UNCONDCANCELFLAG             --�Ƿ����ʱ����������
                ,''                                             AS CCFLEVEL                     --����ת��ϵ������
                ,''                                             AS CCFAIRB                      --�߼�������ת��ϵ��
                ,'01'                                           AS CLAIMSLEVEL                  --ծȨ����(01:�߼�ծȨ,02:�μ�ծȨ)
                ,'0'                                            AS BONDFLAG                     --�Ƿ�Ϊծȯ
                ,''                                             AS BONDISSUEINTENT              --ծȯ����Ŀ��
                ,'0'                                            AS NSUREALPROPERTYFLAG          --�Ƿ�����ò�����
                ,''                                             AS REPASSETTERMTYPE             --��ծ�ʲ���������
                ,'0'                                            AS DEPENDONFPOBFLAG             --�Ƿ�����������δ��ӯ��
                ,NULL                                           AS IRATING                      --�ڲ�����
                ,NULL                                           AS PD                           --ΥԼ����
                ,''                                             AS LGDLEVEL                     --ΥԼ��ʧ�ʼ���
                ,0                                              AS LGDAIRB                      --�߼���ΥԼ��ʧ��
                ,NULL                                           AS MAIRB                        --�߼�����Ч����
                ,0                                              AS EADAIRB                      --�߼���ΥԼ���ձ�¶
                ,'0'                                            AS DEFAULTFLAG                  --ΥԼ��ʶ
                ,0.45                                           AS BEEL                         --��ΥԼ��¶Ԥ����ʧ����
                ,0.45                                           AS DEFAULTLGD                   --��ΥԼ��¶ΥԼ��ʧ��
                ,'0'                                            AS EQUITYEXPOFLAG               --��Ȩ��¶��ʶ
                ,''                                             AS EQUITYINVESTTYPE             --��ȨͶ�ʶ�������
                ,''                                             AS EQUITYINVESTCAUSE            --��ȨͶ���γ�ԭ��
                ,'0'                                            AS SLFLAG                       --רҵ�����ʶ
                ,''                                             AS SLTYPE                       --רҵ��������
                ,''                                             AS PFPHASE                      --��Ŀ���ʽ׶�
                ,''                                             AS REGURATING                   --�������
                ,''                                             AS CBRCMPRATINGFLAG             --������϶������Ƿ��Ϊ����
                ,'0'                                            AS LARGEFLUCFLAG                --�Ƿ񲨶��Խϴ�
                ,'0'                                            AS LIQUEXPOFLAG                 --�Ƿ���������з��ձ�¶
                ,'0'                                            AS PAYMENTDEALFLAG              --�Ƿ����Ը�ģʽ
                ,0                                              AS DELAYTRADINGDAYS             --�ӳٽ�������
                ,'0'                                            AS SECURITIESFLAG               --�м�֤ȯ��ʶ
                ,''                                             AS SECUISSUERID                 --֤ȯ������ID
                ,''                                             AS RATINGDURATIONTYPE           --������������
                ,''                                             AS SECUISSUERATING              --֤ȯ���еȼ�
                ,0                                              AS SECURESIDUALM                --֤ȯʣ������
                ,1                                              AS SECUREVAFREQUENCY            --֤ȯ�ع�Ƶ��
                ,'0'                                            AS CCPTRANFLAG                  --�Ƿ����뽻�׶�����ؽ���
                ,''                                             AS CCPID                        --���뽻�׶���ID
                ,'0'                                            AS QUALCCPFLAG                  --�Ƿ�ϸ����뽻�׶���
                ,''                                             AS BANKROLE                     --���н�ɫ
                ,''                                             AS CLEARINGMETHOD               --���㷽ʽ
                ,''                                             AS BANKASSETFLAG                --�Ƿ������ύ�ʲ�
                ,''                                             AS MATCHCONDITIONS              --�����������
                ,'0'                                            AS SFTFLAG                      --֤ȯ���ʽ��ױ�ʶ
                ,'0'                                            AS MASTERNETAGREEFLAG           --���������Э���ʶ
                ,''                                             AS MASTERNETAGREEID             --���������Э��ID
                ,''                                             AS SFTTYPE                      --֤ȯ���ʽ�������
                ,'0'                                            AS SECUOWNERTRANSFLAG           --֤ȯ����Ȩ�Ƿ�ת��
                ,'0'                                            AS OTCFLAG                      --�����������߱�ʶ
                ,'0'                                            AS VALIDNETTINGFLAG             --��Ч�������Э���ʶ
                ,''                                             AS VALIDNETAGREEMENTID          --��Ч�������Э��ID
                ,''                                             AS OTCTYPE                      --����������������
                ,0                                              AS DEPOSITRISKPERIOD            --��֤������ڼ�
                ,0                                              AS MTM                          --���óɱ�
                ,''                                             AS MTMCURRENCY                  --���óɱ�����
                ,''                                             AS BUYERORSELLER                --������
                ,'0'                                            AS QUALROFLAG                   --�ϸ�����ʲ���ʶ
                ,'0'                                            AS ROISSUERPERFORMFLAG          --�����ʲ��������Ƿ�����Լ
                ,''                                             AS BUYERINSOLVENCYFLAG          --���ñ������Ƿ��Ʋ�
                ,0                                              AS NONPAYMENTFEES               --��δ֧������
                ,DECODE(RAP.RETAILFLAG,1,'1','0')               AS RETAILEXPOFLAG               --���۱�¶��ʶ
                ,DECODE(RAP.RETAILFLAG,1,'020403','')           AS RETAILCLAIMTYPE              --����ծȨ����
                ,''                                             AS MORTGAGETYPE                 --ס����Ѻ��������
                ,1                                              AS ExpoNumber                   --���ձ�¶����                Ĭ�� 1
                ,0.8                                            AS LTV                          --�����ֵ��                 Ĭ�� 0.8
                ,NULL                                           AS Aging                        --����                        Ĭ�� NULL
                ,''                                             AS NewDefaultDebtFlag           --����ΥԼծ���ʶ            Ĭ�� NULL
                ,''                                             AS PDPoolModelID                --PD�ֳ�ģ��ID                Ĭ�� NULL
                ,''                                             AS LGDPoolModelID               --LGD�ֳ�ģ��ID               Ĭ�� NULL
                ,''                                             AS CCFPoolModelID               --CCF�ֳ�ģ��ID               Ĭ�� NULL
                ,''                                             AS PDPoolID                     --����PD��ID                 Ĭ�� NULL
                ,''                                             AS LGDPoolID                    --����LGD��ID                Ĭ�� NULL
                ,''                                             AS CCFPoolID                    --����CCF��ID                Ĭ�� NULL
                ,'0'                                            AS ABSUAFlag                    --�ʲ�֤ȯ�������ʲ���ʶ     Ĭ�� ��(0)
                ,''                                             AS ABSPoolID                    --֤ȯ���ʲ���ID              Ĭ�� NULL
                ,''                                             AS GroupID                      --������                    Ĭ�� NULL
                ,NULL                                           AS DefaultDate                  --ΥԼʱ��
                ,NULL                                           AS ABSPROPORTION                --�ʲ�֤ȯ������
                ,NULL                                           AS DEBTORNUMBER                 --����˸���

    FROM         RWA_DEV.RWA_EI_CONTRACT REC
    INNER JOIN   RWA_DEV.RWA_ARTICULATION_PARAM RAP
    ON           SUBSTR(REC.CONTRACTID,4,8) = RAP.THIRD_SUBJECT_NO
    AND          RAP.ISNETTING = '1'                      --�Ƿ����� 0�� 1��
    AND          RAP.ISINUSE = '1'                        --����״̬ 1���� 0ͣ��
    WHERE        REC.DATADATE = V_DATADATE
    AND          REC.SSYSID = 'GC'
    ;

    COMMIT;

    /*����Ŀ���RWA_DEV.RWA_EI_CLIENT*/
    INSERT INTO RWA_DEV.RWA_EI_CLIENT(
                 DataDate                   --��������
                ,DataNo                     --������ˮ��
                ,ClientID                   --��������ID
                ,SourceClientID             --Դ��������ID
                ,SSysID                     --ԴϵͳID
                ,ClientName                 --������������
                ,SOrgID                     --Դ����ID
                ,SOrgName                   --Դ��������
                ,OrgSortNo                  --�������������
                ,OrgID                      --��������ID
                ,OrgName                    --������������
                ,IndustryID                 --������ҵ����
                ,IndustryName               --������ҵ����
                ,ClientType                 --�����������
                ,ClientSubType              --��������С��
                ,RegistState                --ע����һ����
                ,RCERating                  --����ע����ⲿ����
                ,RCERAgency                 --����ע����ⲿ��������
                ,OrganizationCode           --��֯��������
                ,ConsolidatedSCFlag         --�Ƿ񲢱��ӹ�˾
                ,SLClientFlag               --רҵ����ͻ���ʶ
                ,SLClientType               --רҵ����ͻ�����
                ,ExpoCategoryIRB            --��������¶���
                ,ModelID                    --ģ��ID
                ,ModelIRating               --ģ���ڲ�����
                ,ModelPD                    --ģ��ΥԼ����
                ,IRating                    --�ڲ�����
                ,PD                         --ΥԼ����
                ,DefaultFlag                --ΥԼ��ʶ
                ,NewDefaultFlag             --����ΥԼ��ʶ
                ,DefaultDate                --ΥԼʱ��
                ,ClientERating              --���������ⲿ����
                ,CCPFlag                    --���뽻�׶��ֱ�ʶ
                ,QualCCPFlag                --�Ƿ�ϸ����뽻�׶���
                ,ClearMemberFlag            --�����Ա��ʶ
                ,CompanySize                --��ҵ��ģ
                ,SSMBFlag                   --��׼С΢��ҵ��ʶ
                ,AnnualSale                 --��˾�ͻ������۶�
                ,CountryCode                --ע����Ҵ���
                ,MSMBFlag                   --���Ų�΢С��ҵ��ʶ
    )
    SELECT
                V_DATADATE                AS DATADATE             --��������
                ,V_DATANO                 AS DATANO               --������ˮ��
                ,REE.CLIENTID             AS CLIENTID             --�����������
                ,REE.CLIENTID             AS SOURCECLIENTID       --Դ�����������
                ,REE.SSYSID               AS SSYSID               --Դϵͳ����
                ,'��������ͻ�'           AS CLIENTNAME           --������������
                ,REE.ORGID                AS SORGID               --Դ��������
                ,REE.ORGNAME              AS SORGNAME             --Դ��������
                ,REE.ORGSORTNO            AS ORGSORTNO            --���������
                ,REE.ORGID                AS ORGID                --������������
                ,REE.ORGNAME              AS ORGNAME              --������������
                ,REE.INDUSTRYID           AS INDUSTRYID           --������ҵ����
                ,REE.INDUSTRYNAME         AS INDUSTRYNAME         --������ҵ����
                ,DECODE(RAP.RETAILFLAG,1,'04','03')
                                          AS CLIENTTYPE           --�����������
                ,DECODE(RAP.RETAILFLAG,1,'0401','0301')
                                          AS CLIENTSUBTYPE        --��������С��
                ,'01'                     AS REGISTSTATE          --ע����һ����
                ,NULL                     AS RCERATING            --����ע����ⲿ����
                ,NULL                     AS RCERAGENCY           --����ע����ⲿ��������
                ,NULL                     AS ORGANIZATIONCODE     --��֯��������
                ,'0'                      AS CONSOLIDATEDSCFLAG   --�Ƿ񲢱��ӹ�˾
                ,'0'                      AS SLCLIENTFLAG         --רҵ����ͻ���ʶ
                ,NULL                     AS SLCLIENTTYPE         --רҵ����ͻ�����
                ,'020701'                 AS EXPOCATEGORYIRB      --��������¶���
                ,NULL                     AS ModelID              --ģ��ID
                ,NULL                     AS ModelIRating         --ģ���ڲ�����
                ,NULL                     AS ModelPD              --ģ��ΥԼ����
                ,NULL                     AS IRating              --�ڲ�����
                ,NULL                     AS PD                   --ΥԼ����
                ,'0'                      AS DefaultFlag          --ΥԼ��ʶ
                ,'0'                      AS NewDefaultFlag       --����ΥԼ��ʶ
                ,NULL                     AS DefaultDate          --ΥԼʱ��
                ,''                       AS CLIENTERATING        --���������ⲿ����
                ,'0'                      AS CCPFLAG              --���뽻�׶��ֱ�ʶ
                ,'0'                      AS QUALCCPFLAG          --�Ƿ�ϸ����뽻�׶���
                ,'0'                      AS CLEARMEMBERFLAG      --�����Ա��ʶ
                ,'01'                     AS CompanySize          --��ҵ��ģ
                ,'0'                      AS SSMBFLAG             --��׼С΢��ҵ��ʶ
                ,null                     AS ANNUALSALE           --��˾�ͻ������۶�
                ,'CHN'                    AS COUNTRYCODE          --ע����Ҵ���
                ,'0'                      AS MSMBFLAG             --���Ų�΢С��ҵ��ʶ
    FROM  RWA_DEV.RWA_EI_EXPOSURE REE
    LEFT JOIN RWA_DEV.RWA_ARTICULATION_PARAM RAP
    ON    RAP.THIRD_SUBJECT_NO = REE.ACCSUBJECT1
    WHERE DATADATE = V_DATADATE
    AND   SSYSID = 'GC'
    ;
    COMMIT;




--1  Ӧ����Ϣ��ֵ׼���չ����µ�һ���̶��Ŀ�Ŀ�ϣ���ĿֻҪ��1132��ͷ�Ŀ�Ŀ, ���ճ���Ŀ���Ҫ���ڼ�ֵ���
  UPDATE RWA_DEV.RWA_EI_EXPOSURE T1 
         SET T1.GENERALPROVISION=(SELECT SUM(T.BALANCE_C-T.BALANCE_D) AS PROVISION FROM RWA_DEV.FNS_GL_BALANCE T
                                  WHERE T.DATANO=p_data_dt_str
                                  AND T.SUBJECT_NO='12310100'
                                  AND T.CURRENCY_CODE<>'RMB' )
  WHERE T1.DATANO=p_data_dt_str AND T1.ACCSUBJECT1='11320901';
  COMMIT;
  
  --1  �������˼�ֵ׼���չ����µ�һ���̶��Ŀ�Ŀ�ϣ���ĿֻҪ��1132��ͷ�Ŀ�Ŀ���ɣ�, ���ճ���Ŀ���Ҫ���ڼ�ֵ���
  UPDATE RWA_DEV.RWA_EI_EXPOSURE T1 
         SET T1.GENERALPROVISION=(SELECT SUM(T.BALANCE_C-T.BALANCE_D) AS PROVISION FROM RWA_DEV.FNS_GL_BALANCE T
                                  WHERE T.DATANO=p_data_dt_str
                                  AND T.SUBJECT_NO='12312000'
                                  AND T.CURRENCY_CODE<>'RMB' )
  WHERE T1.DATANO=p_data_dt_str AND T1.ACCSUBJECT1='11320601' AND T1.CURRENCY='CNY';
  COMMIT;

--2  ������ֵ��Ŀ,�����뵽���������
--����������ʱ������֮ǰ����ձ�
EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.TEMP_GENERALPROVISION';
COMMIT;

--2.1�����ֵ׼����ϸ����ʱ��
INSERT INTO RWA_DEV.TEMP_GENERALPROVISION(
               EXPOSUREID  
              ,ACCSUBJECTNO
              ,NORMALBAL
              ,PROVISIONSUBJECTNO
              ,PROVISIONSUBJECTNAME
              ,GENERALPROVISIONBAL
  )
  SELECT
               T1.EXPOSUREID
               ,T1.ACCSUBJECT1
               ,T1.NORMALPRINCIPAL
               ,CASE WHEN T1.ACCSUBJECT1='11320901' THEN '12310100'      --Ӧ����Ϣ��ֵ׼��
                     WHEN T1.ACCSUBJECT1='11320601' THEN '12312000'      --�������˼�ֵ׼��
                    WHEN T1.ACCSUBJECT1 LIKE '1011%' THEN '12310300'       --���ͬҵ��ֵ׼��
                    WHEN T1.ACCSUBJECT1 LIKE '130201%' THEN '12310501'     --���ͬҵ��ֵ׼��
                    WHEN T1.ACCSUBJECT1 LIKE '1301%' THEN '40030102'       --�����ʲ���ֵ׼��
                    WHEN T1.ACCSUBJECT1 LIKE '1111%' THEN '12310200'       --���뷷�ۼ�ֵ׼��
                    WHEN T1.ACCSUBJECT1 LIKE '150101%' THEN '15020100'     --ծȯͶ�ʼ�ֵ׼��
                    --WHEN T1.ACCSUBJECT1 LIKE '150301%' THEN '40030101'     --�����ۺ������ֵ׼��   1503��Ŀ��������ֵ
                    WHEN T1.ACCSUBJECT1 LIKE '122201%' THEN '12310400'     --Ӧ�տ��ֵ׼��
                    WHEN T1.ACCSUBJECT1 LIKE '14410100%' THEN '14420000'   --��ծ�ʲ���ֵ׼��
                    WHEN (T1.ACCSUBJECT1 LIKE '7001%' OR T1.ACCSUBJECT1 LIKE '7002%' OR T1.ACCSUBJECT1 LIKE '7018%' OR T1.ACCSUBJECT1 LIKE '7119%')
                      THEN '28010101'       --����ҵ���ֵ
                    WHEN (T1.ACCSUBJECT1 LIKE '1303%' OR T1.ACCSUBJECT1 LIKE '1305%' OR T1.ACCSUBJECT1 LIKE '1307%' OR T1.ACCSUBJECT1 LIKE '1310%')
                      THEN '13040100'       --�������Ŀ��ֵ׼��
                    ELSE ''
                END AS PROVISIONSUBJECTNO
                ,CASE WHEN T1.ACCSUBJECT1='11320901' THEN 'Ӧ����Ϣ��ֵ׼��'     
                    WHEN T1.ACCSUBJECT1='11320601' THEN '�������˼�ֵ׼��'      
                    WHEN T1.ACCSUBJECT1 LIKE '1011%' THEN '���ͬҵ��ֵ׼��'       
                    WHEN T1.ACCSUBJECT1 LIKE '130201%' THEN '���ͬҵ��ֵ׼��'    
                    WHEN T1.ACCSUBJECT1 LIKE '1301%' THEN '�����ʲ���ֵ׼��'       
                    WHEN T1.ACCSUBJECT1 LIKE '1111%' THEN '���뷷�ۼ�ֵ׼��'       
                    WHEN T1.ACCSUBJECT1 LIKE '150101%' THEN 'ծȯͶ�ʼ�ֵ׼��'     
                    --WHEN T1.ACCSUBJECT1 LIKE '150301%' THEN '�����ۺ������ֵ׼��'     --   1503��Ŀ��������ֵ
                    WHEN T1.ACCSUBJECT1 LIKE '122201%' THEN 'Ӧ�տ�Ͷ�ʼ�ֵ׼��'     --
                    WHEN T1.ACCSUBJECT1 LIKE '14410100%' THEN '��ծ�ʲ���ֵ׼��'   --
                    WHEN (T1.ACCSUBJECT1 LIKE '7001%' OR T1.ACCSUBJECT1 LIKE '7002%' OR T1.ACCSUBJECT1 LIKE '7018%' OR T1.ACCSUBJECT1 LIKE '7119%')
                      THEN '����ҵ���ֵ'       --
                    WHEN (T1.ACCSUBJECT1 LIKE '1303%' OR T1.ACCSUBJECT1 LIKE '1305%' OR T1.ACCSUBJECT1 LIKE '1307%' OR T1.ACCSUBJECT1 LIKE '1310%')
                      THEN '�������Ŀ��ֵ׼��'       --
                    ELSE ''
                END AS PROVISIONSUBJECTNAME
                ,NVL(T1.GENERALPROVISION,0)
  FROM RWA_DEV.RWA_EI_EXPOSURE T1
  WHERE T1.DATANO=p_data_dt_str 
  AND T1.GENERALPROVISION>0
  UNION      --�ʲ�֤ȯ����ϸҲ�м�ֵ����Ҫ����
  SELECT 
        T2.ABSEXPOSUREID
        ,'12220101'
        ,T2.ASSETBALANCE
        ,'12310400'
        ,'Ӧ�տ�Ͷ�ʼ�ֵ׼��'
        ,T2.PROVISIONS
  FROM RWA_DEV.RWA_EI_ABSEXPOSURE T2
  WHERE T2.DATANO=p_data_dt_str
  AND T2.PROVISIONS>0;
  COMMIT;
  
  --2.2������ܺ�Ľ������ϸ��,����֮ǰ��ɾ��
  DELETE FROM RWA_DEV.RWA_GENERALPROVISION_RESULT WHERE DATANO = p_data_dt_str;
  COMMIT;
  
  --������ܺ�Ľ������ϸ��
  INSERT INTO RWA_DEV.RWA_GENERALPROVISION_RESULT
  (
     datano       
     ,subject_no  
     ,subject_name 
     ,currency    
     ,total_bal   
     ,detail_bal   
      ,diff_bal     
     ,percent_bal  
  )
  SELECT p_data_dt_str
         ,T1.PROVISIONSUBJECTNO
         ,T1.PROVISIONSUBJECTNAME
         ,'CNY'
         ,T2.BALANCE
         ,T1.GENERALPROVISIONBAL
         ,T2.BALANCE-T1.GENERALPROVISIONBAL
         ,(T2.BALANCE-T1.GENERALPROVISIONBAL)/T2.BALANCE*100
  FROM (SELECT PROVISIONSUBJECTNO
              ,PROVISIONSUBJECTNAME
              ,SUM(GENERALPROVISIONBAL)  AS GENERALPROVISIONBAL
       FROM RWA_DEV.TEMP_GENERALPROVISION 
       WHERE PROVISIONSUBJECTNO IS NOT NULL
       GROUP BY PROVISIONSUBJECTNO
               ,PROVISIONSUBJECTNAME
       ) T1
  INNER JOIN (SELECT 
               CASE WHEN T.SUBJECT_NO IN ('13040101','13040102','13040103') THEN '13040100' ELSE T.SUBJECT_NO END AS SUBJECTNO
              ,SUM(T.BALANCE_C-T.BALANCE_D) AS BALANCE
              FROM RWA_DEV.FNS_GL_BALANCE T
              WHERE T.DATANO=p_data_dt_str
              AND T.CURRENCY_CODE<>'RMB'
              GROUP BY CASE WHEN T.SUBJECT_NO IN ('13040101','13040102','13040103') THEN '13040100' ELSE T.SUBJECT_NO END
  ) T2
  ON T1.PROVISIONSUBJECTNO=T2.SUBJECTNO;
  COMMIT;

--3  ��ֵ�����̯   by wzb 20191127 

------3.1  �������ÿ���ֵ�����̯
select SUM(A.GENERALPROVISION)into V_BWXTZBJ from rwa_ei_exposure a   ----�����I9��ȡ�����м�ֵ
where accsubject1 in('70010100','70020000','70180001','70180002') 
and datano=p_data_dt_str;

 select SUM(BALANCE_C*NVL(B.MIDDLEPRICE/100, 1)-BALANCE_D*NVL(B.MIDDLEPRICE/100, 1)) INTO V_BWZZZBJ  ---�������˼�ֵ
 from fns_gl_balance a
 LEFT JOIN TMP_CURRENCY_CHANGE B
 ON A.currency_code=B.CURRENCYCODE
 AND B.DATANO=p_data_dt_str
 where subject_no ='28010101'
        AND A.CURRENCY_CODE<>'RMB'
        AND A.DATANO=p_data_dt_str;
        
--��ȡ����ѭ������δʹ�ö��        
SELECT SUM(FINAL_ECL) INTO V_BWXHZBJ FROM SYS_IFRS9_RESULT WHERE DATANO=p_data_dt_str AND ITEM_CODE LIKE '7120%';

------������켴���ÿ�������� ������-ϵͳ�жһ�Ʊ������������֤���Լ�ѭ������δʹ�ö����ϸ���ܣ��õ����ÿ��������ֵ
V_BWCHA := V_BWZZZBJ-V_BWXTZBJ-V_BWXHZBJ;

/*------��̯����ļ�ֵ���쵽���ÿ����Ⱪ¶ÿһ���� 
MERGE INTO RWA_EI_EXPOSURE T1    
USING (
SELECT T2.EXPOSUREID AS EXPOSUREID,
       T2.ASSETBALANCE AS ASSETBALANCE,
       SUM(T2.ASSETBALANCE) over(partition by T2.SSYSID) AS SUMBALANCE,
       (T2.ASSETBALANCE/(SUM(T2.ASSETBALANCE) over(partition by T2.SSYSID)))*V_BWCHA  AS YBZBJ
       FROM RWA_EI_EXPOSURE T2
       WHERE T2.DATANO=p_data_dt_str AND T2.EXPOSUREID LIKE'BW%'
)T3
ON (T1.EXPOSUREID=T3.EXPOSUREID AND T1.DATANO=p_data_dt_str AND T1.EXPOSUREID LIKE 'BW%')
WHEN MATCHED THEN
  UPDATE SET T1.GENERALPROVISION = T1.GENERALPROVISION+NVL(T3.YBZBJ,0)
  ;

COMMIT;*/

------��̯����ļ�ֵ���쵽���ÿ����Ⱪ¶ÿһ���� 
MERGE INTO RWA_EI_EXPOSURE T1    
USING (
SELECT T2.EXPOSUREID AS EXPOSUREID,
       T2.ASSETBALANCE AS ASSETBALANCE,
       SUM(T2.ASSETBALANCE) over(partition by T2.SSYSID) AS SUMBALANCE,
       (T2.ASSETBALANCE/(SUM(T2.ASSETBALANCE) over(partition by T2.SSYSID)))*V_BWCHA  AS YBZBJ
       FROM RWA_EI_EXPOSURE T2
       WHERE T2.DATANO=p_data_dt_str AND T2.EXPOSUREID LIKE'BW%'
)T3
ON (T1.EXPOSUREID=T3.EXPOSUREID AND T1.DATANO=p_data_dt_str AND T1.EXPOSUREID LIKE 'BW%')
WHEN MATCHED THEN
  UPDATE SET T1.GENERALPROVISION =NVL(T3.YBZBJ,0)
  ;

COMMIT;

------3.2 ���ڴ������ÿ������̯  BY  WZB  20191127
    select SUM(A.GENERALPROVISION) INTO V_BNXTZBJ from rwa_ei_exposure a   ----���ڴ�I9��ȡ�����д�������ÿ���ֵ
where (accsubject1 LIKE'1303%' OR accsubject1 LIKE'1305%'  OR accsubject1 LIKE'1307%' OR accsubject1 LIKE'1310%')
and datano=p_data_dt_str;

 select SUM(BALANCE_C*NVL(B.MIDDLEPRICE/100, 1)-BALANCE_D*NVL(B.MIDDLEPRICE/100, 1)) into V_BNZZZBJ  ---�������˼�ֵ
 from fns_gl_balance a
 LEFT JOIN TMP_CURRENCY_CHANGE B
 ON A.currency_code=B.CURRENCYCODE
 AND B.DATANO=p_data_dt_str
 where subject_no in('13040101','13040102','13040103')
        AND A.CURRENCY_CODE<>'RMB'
        AND A.DATANO=p_data_dt_str;

V_BNCHA :=V_BNZZZBJ-V_BNXTZBJ;------���ڲ��켴���ÿ��������  ����-ϵͳ


MERGE INTO RWA_EI_EXPOSURE T1     ------��̯���ڵļ�ֵ���쵽8.3�ı�¶ÿһ���� 
USING (
SELECT T2.EXPOSUREID AS EXPOSUREID,
       T2.ASSETBALANCE AS ASSETBALANCE,
       SUM(T2.ASSETBALANCE) over(partition by T2.EXPOSUBCLASSSTD) AS SUMBALANCE,
       (T2.ASSETBALANCE/(SUM(T2.ASSETBALANCE) over(partition by T2.EXPOSUBCLASSSTD)))*V_BNCHA  AS YBZBJ
       FROM RWA_EI_EXPOSURE T2
       WHERE T2.DATANO=p_data_dt_str AND T2.EXPOSUBCLASSSTD='010803' AND T2.EXPOBELONG='01'
)T3
ON (T1.EXPOSUREID=T3.EXPOSUREID AND T1.DATANO=p_data_dt_str AND T1.EXPOBELONG='01' AND T1.EXPOSUBCLASSSTD='010803')

WHEN MATCHED THEN
  UPDATE SET T1.GENERALPROVISION = T1.GENERALPROVISION+NVL(T3.YBZBJ,0)
  ;

COMMIT;


   /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO V_COUNT FROM RWA_DEV.RWA_ARTICULATION_RESULT WHERE DATADATE = V_DATADATE;
    DBMS_OUTPUT.PUT_LINE('RWA_DEV.rwa_articulation_result��ǰ��������ݼ�¼Ϊ:' || V_COUNT || '��');

    SELECT COUNT(1) INTO V_COUNT FROM RWA_DEV.RWA_EI_CONTRACT WHERE DATADATE = V_DATADATE AND SSYSID = 'GC';
    DBMS_OUTPUT.PUT_LINE('RWA_DEV.rwa_ei_contract��ǰ��������ݼ�¼Ϊ:' || V_COUNT || '��');

    SELECT COUNT(1) INTO V_COUNT FROM RWA_DEV.RWA_EI_EXPOSURE WHERE DATADATE = V_DATADATE AND SSYSID = 'GC';
    DBMS_OUTPUT.PUT_LINE('RWA_DEV.rwa_ei_exposure��ǰ��������ݼ�¼Ϊ:' || V_COUNT || '��');

    DBMS_OUTPUT.PUT_LINE('��ִ�� ' || V_PRO_NAME || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    p_po_rtncode := '1';
    p_po_rtnmsg := '�ɹ�';

    --�����쳣
   EXCEPTION
       WHEN OTHERS THEN
       ROLLBACK;
       p_po_rtncode := SQLCODE;
       p_po_rtnmsg := '���˹�������' || SQLERRM||';��������Ϊ:'|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE;
   RETURN;
END PRO_RWA_ACCOUNT_ARTICULATION;
/

