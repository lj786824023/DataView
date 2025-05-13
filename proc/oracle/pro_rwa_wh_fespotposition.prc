CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_WH_FESPOTPOSITION(
                                                         p_data_dt_str      IN     VARCHAR2,       --�������� yyyyMMdd
                                                         p_po_rtncode       OUT    VARCHAR2,       --���ر�� 1 �ɹ�,0 ʧ��
                                                         p_po_rtnmsg        OUT    VARCHAR2        --��������
                )
  /*
    �洢��������:RWA_DEV.PRO_RWA_WH_FESPOTPOSITION
    ʵ�ֹ���:����ϵͳ-�г�����-����ֻ�ͷ��(�������˼ӹ�)
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�����
    ��  ��  :V1.0.0
    ��д��  :LISY
    ��дʱ��:2016-04-12
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.FNS_GL_BALANCE|��������(�ӹ���)
    Դ  ��2 :RWA.CODE_LIBRARY|�����
    Ŀ���  :RWA_DEV.RWA_WH_FESPOTPOSITION|����ϵͳ����ֻ�ͷ���
    �����¼(�޸���|�޸�ʱ��|�޸�����):
     pxl  2019/09/09 ��������ֻ�ͷ��ӹ��߼�
    
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_WH_FESPOTPOSITION';
  --�����쳣����
  v_raise EXCEPTION;
  --���嵱ǰ����ļ�¼��
  v_count INTEGER;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_WH_FESPOTPOSITION';

    --2.1 ����ϵͳ-����ֻ�ͷ���
    INSERT INTO RWA_DEV.RWA_WH_FESPOTPOSITION(
                 DATADATE                              --��������
                ,POSITIONID                            --ͷ��ID
                ,ACCORGID                              --�������ID Ĭ������ 10000000
                ,INSTRUMENTSTYPE                       --���ڹ������� Ĭ��Ϊ ��0501��������ֻ���
                ,ACCSUBJECTS                           --��ƿ�Ŀ
                ,BOOKTYPE                              --�˻���� ��ֵ��BookType Ĭ��Ϊ 01 �����˻�
                ,STRUCTURALEXPOFLAG                    --�Ƿ�ṹ�Գ��� Ĭ�Ϸ� 1 �� 0 ��
                ,CURRENCY                              --����
                ,POSITIONTYPE                          --ͷ������ ��ֵ��PositionType �ʲ�Ϊ 01 ��ͷ����ծΪ 02 ��ͷ �����ࣺ���ڵ���0 01 ��ͷ��С��0 02 ��ͷ
                ,POSITION                              --ͷ��
    )
    SELECT
                 TEMP.DATADATE                         AS DATADATE                --��������
                ,p_data_dt_str||lpad(rownum, 10, '0')  AS POSITIONID              --ͷ��ID
                ,TEMP.ACCORGID                         AS ACCORGID                --�������ID
                ,TEMP.INSTRUMENTSTYPE                  AS INSTRUMENTSTYPE         --���ڹ�������
                ,TEMP.ACCSUBJECTS                      AS ACCSUBJECTS             --��ƿ�Ŀ
                ,TEMP.BOOKTYPE                         AS BOOKTYPE                --�˻����
                ,CASE 
                     WHEN FLAG = '4' THEN '1'  --�ṹ�Գ��� Ϊ��
                     ELSE '0'
                 END              AS STRUCTURALEXPOFLAG      --�Ƿ�ṹ�Գ���
                ,TEMP.CURRENCY                         AS CURRENCY                --����
                ,TEMP.POSITIONTYPE                     AS POSITIONTYPE            --ͷ������
                ,ABS(
                  CASE WHEN FLAG='1'
                        THEN  TEMP.POSITION1
                             +NVL(DECODE(SIGN(TEMP.POSITION3001),1,TEMP.POSITION3001),0)
                             +NVL(DECODE(SIGN(TEMP.POSITION3002),1,TEMP.POSITION3002),0)
                             +NVL(DECODE(SIGN(TEMP.POSITION3003),1,TEMP.POSITION3003),0)
                             +NVL(DECODE(SIGN(TEMP.POSITION3004),1,TEMP.POSITION3004),0)
                        WHEN FLAG='2'
                        THEN  TEMP.POSITION1
                             +NVL(DECODE(SIGN(TEMP.POSITION3001),-1,TEMP.POSITION3001),0)
                             +NVL(DECODE(SIGN(TEMP.POSITION3002),-1,TEMP.POSITION3002),0)
                             +NVL(DECODE(SIGN(TEMP.POSITION3003),-1,TEMP.POSITION3003),0)
                             +NVL(DECODE(SIGN(TEMP.POSITION3004),-1,TEMP.POSITION3004),0)
                             +NVL(DECODE(SIGN(TEMP.POSITION_CD1-TEMP.POSITION_CD2),1,TEMP.POSITION_CD1-TEMP.POSITION_CD2),0)   --231401 ί�д�� �� 132101 ί�д��� ���ܺ�ֵ�������0��С��0��Ϊ0
                        ELSE  TEMP.POSITION1 
                 END)         AS POSITION                --ͷ��
    FROM (
        --�ʲ�
        SELECT  TO_DATE(p_data_dt_str,'YYYYMMDD')   AS DATADATE           --��������
               ,'1'                                 AS FLAG               --�������ͱ�־
               ,'9998'                          AS ACCORGID           --�������ID Ĭ������ 9998
               ,'0501'                              AS INSTRUMENTSTYPE    --���ڹ������� Ĭ��Ϊ ��0501��������ֻ���
               ,''                                  AS ACCSUBJECTS        --��ƿ�Ŀ Ĭ��Ϊ ��
               ,'01'                                AS BOOKTYPE           --�˻���� ��ֵ��BookType Ĭ��Ϊ 01 �����˻�
               ,'0'                                 AS STRUCTURALEXPOFLAG --�Ƿ�ṹ�Գ��� Ĭ�Ϸ� 1 �� 0 ��
               ,CURRENCY_CODE                       AS CURRENCY           --����
               ,'01'                                AS POSITIONTYPE       --ͷ������ ��ֵ��PositionType �ʲ�Ϊ 01 ��ͷ����ծΪ 02 ��ͷ �����ࣺ���ڵ���0 01 ��ͷ��С��0 02 ��ͷ
               ,SUM(CASE WHEN    TE.SUBJECT_NO='1001' OR TE.SUBJECT_NO='1003' OR TE.SUBJECT_NO='1011' OR TE.SUBJECT_NO='1302'
                              OR TE.SUBJECT_NO='1101' OR TE.SUBJECT_NO='1111' OR TE.SUBJECT_NO='1132' OR TE.SUBJECT_NO='1301'
                              OR TE.SUBJECT_NO='1303' OR TE.SUBJECT_NO='1305' OR TE.SUBJECT_NO='1307' OR TE.SUBJECT_NO='1310'
                              OR TE.SUBJECT_NO='1503' OR TE.SUBJECT_NO='1222' OR TE.SUBJECT_NO='1501' OR TE.SUBJECT_NO='1511'
                              OR TE.SUBJECT_NO='1521' OR TE.SUBJECT_NO='1601' OR TE.SUBJECT_NO='1604' OR TE.SUBJECT_NO='1606'
                              OR TE.SUBJECT_NO='1701' OR TE.SUBJECT_NO='1811' OR TE.SUBJECT_NO='1124' OR TE.SUBJECT_NO='1221'
                              OR TE.SUBJECT_NO='1441'
                              OR TE.SUBJECT_NO='1801' OR TE.SUBJECT_NO='1802' OR TE.SUBJECT_NO='1901' OR TE.SUBJECT_NO='132101'
                              OR TE.SUBJECT_NO='132102' OR TE.SUBJECT_NO='132120' OR TE.SUBJECT_NO='1311'
                              OR TE.SUBJECT_NO='1231'
                         THEN BALANCE
                         WHEN    TE.SUBJECT_NO='123103'
                              OR TE.SUBJECT_NO='123101'
                              OR TE.SUBJECT_NO='123104'
                         THEN -ABS(BALANCE)
                         WHEN    TE.SUBJECT_NO='1304' OR TE.SUBJECT_NO='1504' OR TE.SUBJECT_NO='1502' OR TE.SUBJECT_NO='1512'
                              OR TE.SUBJECT_NO='1523' OR TE.SUBJECT_NO='160202' OR TE.SUBJECT_NO='160201'
                              OR TE.SUBJECT_NO='1603' OR TE.SUBJECT_NO='1702' OR TE.SUBJECT_NO='1703' OR TE.SUBJECT_NO='122109'
                              OR TE.SUBJECT_NO='1442' OR TE.SUBJECT_NO='1607'
                         THEN -ABS(BALANCE)
                         ELSE 0 END)                AS POSITION1     --ͷ��1
               ,SUM(CASE WHEN   TE.SUBJECT_NO='3001'
                         THEN BALANCE
                         ELSE 0 END)                AS POSITION3001  --ͷ��3001
               ,SUM(CASE WHEN   TE.SUBJECT_NO='3002'
                         THEN BALANCE
                         ELSE 0 END)                AS POSITION3002  --ͷ��3002
               ,SUM(CASE WHEN   TE.SUBJECT_NO='3003'
                         THEN BALANCE
                         ELSE 0 END)                AS POSITION3003  --ͷ��3003
               ,SUM(CASE WHEN   TE.SUBJECT_NO='3004'
                         THEN BALANCE
                         ELSE 0 END)                AS POSITION3004  --ͷ��3004
               ,0                                   AS POSITION_CD1  --ͷ��_ί�д��
               ,0                                   AS POSITION_CD2  --ͷ��_ί�д���
        FROM (SELECT  TT.SUBJECT_NO
                     ,TT.CURRENCY_CODE
                     ,SUM(TT.BALANCE) AS BALANCE
              FROM  (SELECT  CASE WHEN   T1.SUBJECT_NO LIKE '1001%' OR T1.SUBJECT_NO LIKE '1003%' OR T1.SUBJECT_NO LIKE '1011%' OR T1.SUBJECT_NO LIKE '1302%'
                                      OR T1.SUBJECT_NO LIKE '1101%' OR T1.SUBJECT_NO LIKE '1111%' OR T1.SUBJECT_NO LIKE '1132%' OR T1.SUBJECT_NO LIKE '1301%'
                                      OR T1.SUBJECT_NO LIKE '1303%' OR T1.SUBJECT_NO LIKE '1305%' OR T1.SUBJECT_NO LIKE '1307%' OR T1.SUBJECT_NO LIKE '1310%'
                                      OR T1.SUBJECT_NO LIKE '1503%' OR T1.SUBJECT_NO LIKE '1222%' OR T1.SUBJECT_NO LIKE '1501%' OR T1.SUBJECT_NO LIKE '1511%'
                                      OR T1.SUBJECT_NO LIKE '1521%' OR T1.SUBJECT_NO LIKE '1601%' OR T1.SUBJECT_NO LIKE '1604%' OR T1.SUBJECT_NO LIKE '1606%'
                                      OR T1.SUBJECT_NO LIKE '1701%' OR T1.SUBJECT_NO LIKE '1811%' OR T1.SUBJECT_NO LIKE '1124%' OR T1.SUBJECT_NO LIKE '1221%'
                                      OR T1.SUBJECT_NO LIKE '1441%' OR T1.SUBJECT_NO LIKE '1442%' OR T1.SUBJECT_NO LIKE '1607%' OR T1.SUBJECT_NO LIKE '1311%'
                                      OR T1.SUBJECT_NO LIKE '1801%' OR T1.SUBJECT_NO LIKE '1802%' OR T1.SUBJECT_NO LIKE '1901%' OR (T1.SUBJECT_NO LIKE '1231%' AND T1.SUBJECT_NO NOT LIKE '123103%' AND  T1.SUBJECT_NO NOT LIKE '123101%' AND  T1.SUBJECT_NO NOT LIKE '123104%')
                                      OR T1.SUBJECT_NO LIKE '1304%' OR T1.SUBJECT_NO LIKE '1504%' OR T1.SUBJECT_NO LIKE '1502%' OR T1.SUBJECT_NO LIKE '1512%'
                                      OR T1.SUBJECT_NO LIKE '1523%' OR T1.SUBJECT_NO LIKE '1603%' OR T1.SUBJECT_NO LIKE '1702%' OR T1.SUBJECT_NO LIKE '1703%'
                                      OR T1.SUBJECT_NO LIKE '3001%' OR T1.SUBJECT_NO LIKE '3002%' OR T1.SUBJECT_NO LIKE '3003%' OR T1.SUBJECT_NO LIKE '3004%'
                                  THEN SUBSTR(T1.SUBJECT_NO,0,4)
                                  WHEN   T1.SUBJECT_NO LIKE '132101%' OR T1.SUBJECT_NO LIKE '132102%' OR T1.SUBJECT_NO LIKE '132120%'
                                      OR T1.SUBJECT_NO LIKE '123103%' OR T1.SUBJECT_NO LIKE '123101%' OR T1.SUBJECT_NO LIKE '123104%'
                                      OR T1.SUBJECT_NO LIKE '160201%' OR T1.SUBJECT_NO LIKE '160202%'
                                  THEN SUBSTR(T1.SUBJECT_NO,0,6)
                                  ELSE '���ڷ�Χ' END AS SUBJECT_NO
                            ,T1.CURRENCY_CODE
                            ,SUM(CASE WHEN CL.ATTRIBUTE8='D-C' THEN T1.BALANCE_D - T1.BALANCE_C
                                      WHEN CL.ATTRIBUTE8='C-D' THEN T1.BALANCE_C - T1.BALANCE_D
                                      ELSE T1.BALANCE_D - T1.BALANCE_C END) AS BALANCE
                     FROM RWA_DEV.FNS_GL_BALANCE T1
                     LEFT JOIN RWA.CODE_LIBRARY CL
                     ON    CL.CODENO='NewSubject'
                     AND   T1.SUBJECT_NO=CL.ITEMNO
                     AND   CL.ISINUSE='1'
                     WHERE T1.CURRENCY_CODE IS NOT NULL
                     AND   T1.CURRENCY_CODE <> 'RMB'
                     AND   T1.DATANO = p_data_dt_str
                     GROUP BY T1.SUBJECT_NO, T1.CURRENCY_CODE) TT
              GROUP BY TT.SUBJECT_NO, TT.CURRENCY_CODE) TE
        GROUP BY CURRENCY_CODE
        --��ծ
        UNION ALL
        SELECT  TO_DATE(p_data_dt_str,'YYYYMMDD')      AS DATADATE           --��������
               ,'2'                                 AS FLAG               --�������ͱ�־
               ,'9998'                          AS ACCORGID           --�������ID Ĭ������ 9998
               ,'0501'                              AS INSTRUMENTSTYPE    --���ڹ������� Ĭ��Ϊ ��0501��������ֻ���
               ,''                                  AS ACCSUBJECTS        --��ƿ�Ŀ Ĭ��Ϊ ��
               ,'01'                                AS BOOKTYPE           --�˻���� ��ֵ��BookType Ĭ��Ϊ 01 �����˻�
               ,'0'                                 AS STRUCTURALEXPOFLAG --�Ƿ�ṹ�Գ��� Ĭ�Ϸ� 1 �� 0 ��
               ,CURRENCY_CODE                       AS CURRENCY           --����
               ,'02'                                AS POSITIONTYPE       --ͷ������ ��ֵ��PositionType �ʲ�Ϊ 01 ��ͷ����ծΪ 02 ��ͷ �����ࣺ���ڵ���0 01 ��ͷ��С��0 02 ��ͷ
               ,SUM(CASE WHEN    TE.SUBJECT_NO='2004' OR TE.SUBJECT_NO='2012' OR TE.SUBJECT_NO='2003' OR TE.SUBJECT_NO='2101'
                              OR TE.SUBJECT_NO='2111' OR TE.SUBJECT_NO='2002' OR TE.SUBJECT_NO='2011' OR TE.SUBJECT_NO='220402'
                              OR TE.SUBJECT_NO='231401' OR TE.SUBJECT_NO='132101' OR TE.SUBJECT_NO='2015' OR TE.SUBJECT_NO='2211'
                              OR TE.SUBJECT_NO='2221' OR TE.SUBJECT_NO='2231' OR TE.SUBJECT_NO='2801' OR TE.SUBJECT_NO='2502'
                              OR TE.SUBJECT_NO='2901' OR TE.SUBJECT_NO='2021' OR TE.SUBJECT_NO='220401' OR TE.SUBJECT_NO='2232'
                              OR TE.SUBJECT_NO='2241' OR TE.SUBJECT_NO='2313' OR TE.SUBJECT_NO='2701' OR TE.SUBJECT_NO='231420'
                              OR TE.SUBJECT_NO='2312' OR  TE.SUBJECT_NO='2240'
                         THEN BALANCE
                         ELSE 0 END)                AS POSITION1     --ͷ��1
               ,SUM(CASE WHEN   TE.SUBJECT_NO='3001'
                         THEN BALANCE
                         ELSE 0 END)                AS POSITION3001  --ͷ��3001
               ,SUM(CASE WHEN   TE.SUBJECT_NO='3002'
                         THEN BALANCE
                         ELSE 0 END)                AS POSITION3002  --ͷ��3002
               ,SUM(CASE WHEN   TE.SUBJECT_NO='3003'
                         THEN BALANCE
                         ELSE 0 END)                AS POSITION3003  --ͷ��3003
               ,SUM(CASE WHEN   TE.SUBJECT_NO='3004'
                         THEN BALANCE
                         ELSE 0 END)                AS POSITION3004  --ͷ��3004
               ,SUM(BALANCE_CD1)                    AS POSITION_CD1   --ͷ��_ί�д��
               ,SUM(BALANCE_CD2)                    AS POSITION_CD2   --ͷ��_ί�д���
        FROM (SELECT  TT.SUBJECT_NO
                     ,TT.CURRENCY_CODE
                     ,SUM(TT.BALANCE) AS BALANCE
                     ,SUM(TT.BALANCE_CD1) AS BALANCE_CD1
                     ,SUM(TT.BALANCE_CD2) AS BALANCE_CD2
              FROM (SELECT  CASE WHEN   T1.SUBJECT_NO LIKE '2004%' OR T1.SUBJECT_NO LIKE '2012%' OR T1.SUBJECT_NO LIKE '2003%' OR T1.SUBJECT_NO LIKE '2101%'
                                     OR T1.SUBJECT_NO LIKE '2111%' OR T1.SUBJECT_NO LIKE '2002%' OR T1.SUBJECT_NO LIKE '2011%' OR T1.SUBJECT_NO LIKE '2015%'
                                     OR T1.SUBJECT_NO LIKE '2211%' OR T1.SUBJECT_NO LIKE '2221%' OR T1.SUBJECT_NO LIKE '2231%' OR T1.SUBJECT_NO LIKE '2801%'
                                     OR T1.SUBJECT_NO LIKE '2502%' OR T1.SUBJECT_NO LIKE '2901%' OR T1.SUBJECT_NO LIKE '2021%' OR T1.SUBJECT_NO LIKE '2232%'
                                     OR T1.SUBJECT_NO LIKE '2241%' OR T1.SUBJECT_NO LIKE '2313%' OR T1.SUBJECT_NO LIKE '2701%' OR T1.SUBJECT_NO LIKE '2312%'
                                     OR T1.SUBJECT_NO LIKE '3001%' OR T1.SUBJECT_NO LIKE '3002%' OR T1.SUBJECT_NO LIKE '3003%' OR T1.SUBJECT_NO LIKE '3004%'
                                     OR T1.SUBJECT_NO LIKE '2240%'
                                 THEN SUBSTR(T1.SUBJECT_NO,0,4)
                                 WHEN   T1.SUBJECT_NO LIKE '220402%' OR T1.SUBJECT_NO LIKE '231401%'
                                     OR T1.SUBJECT_NO LIKE '132101%' OR T1.SUBJECT_NO LIKE '220401%'
                                     OR T1.SUBJECT_NO LIKE '231420%'
                                 THEN SUBSTR(T1.SUBJECT_NO,0,6)
                                 ELSE '���ڷ�Χ' END AS SUBJECT_NO
                           ,T1.CURRENCY_CODE
                           ,CASE WHEN T1.SUBJECT_NO LIKE '231401%' OR T1.SUBJECT_NO LIKE '132101%'
                                 THEN 0
                                 ELSE SUM(CASE WHEN CL.ATTRIBUTE8='D-C' THEN T1.BALANCE_D - T1.BALANCE_C
                                               WHEN CL.ATTRIBUTE8='C-D' THEN T1.BALANCE_C - T1.BALANCE_D
                                               ELSE T1.BALANCE_D - T1.BALANCE_C END) END AS BALANCE
                           ,CASE WHEN T1.SUBJECT_NO LIKE '231401%'
                                 THEN SUM(CASE WHEN CL.ATTRIBUTE8='D-C' THEN T1.BALANCE_D - T1.BALANCE_C
                                               WHEN CL.ATTRIBUTE8='C-D' THEN T1.BALANCE_C - T1.BALANCE_D
                                               ELSE T1.BALANCE_D - T1.BALANCE_C END)
                                 ELSE 0                                END AS BALANCE_CD1   --ί�д��
                           ,CASE WHEN T1.SUBJECT_NO LIKE '132101%'
                                 THEN SUM(CASE WHEN CL.ATTRIBUTE8='D-C' THEN T1.BALANCE_D - T1.BALANCE_C
                                               WHEN CL.ATTRIBUTE8='C-D' THEN T1.BALANCE_C - T1.BALANCE_D
                                               ELSE T1.BALANCE_D - T1.BALANCE_C END)
                                 ELSE 0                                END AS BALANCE_CD2   --ί�д���
                    FROM RWA_DEV.FNS_GL_BALANCE T1
                    LEFT JOIN RWA.CODE_LIBRARY CL
                    ON    CL.CODENO='NewSubject'
                    AND   T1.SUBJECT_NO=CL.ITEMNO
                    AND   CL.ISINUSE='1'
                    WHERE T1.CURRENCY_CODE IS NOT NULL
                    AND   T1.CURRENCY_CODE <> 'RMB'
                    AND   T1.DATANO = p_data_dt_str
                    GROUP BY T1.SUBJECT_NO, T1.CURRENCY_CODE) TT
        GROUP BY TT.SUBJECT_NO, TT.CURRENCY_CODE) TE
        GROUP BY CURRENCY_CODE
        --����
        UNION ALL
        SELECT  TO_DATE(p_data_dt_str,'YYYYMMDD')   AS DATADATE           --��������
               ,'3'                                 AS FLAG               --�������ͱ�־
               ,'9998'                          AS ACCORGID           --�������ID Ĭ������ 9998
               ,'0501'                              AS INSTRUMENTSTYPE    --���ڹ������� Ĭ��Ϊ ��0501��������ֻ���
               ,TE.SUBJECT_NO                       AS ACCSUBJECTS        --��ƿ�Ŀ Ĭ��Ϊ ��
               ,'01'                                AS BOOKTYPE           --�˻���� ��ֵ��BookType Ĭ��Ϊ 01 �����˻�
               ,'0'                                 AS STRUCTURALEXPOFLAG --�Ƿ�ṹ�Գ��� Ĭ�Ϸ� 1 �� 0 ��
               ,TE.CURRENCY_CODE                    AS CURRENCY           --����
               ,CASE WHEN TE.BALANCE>=0
                     THEN '01'
                     ELSE '02' END                  AS POSITIONTYPE       --ͷ������ ��ֵ��PositionType �ʲ�Ϊ 01 ��ͷ����ծΪ 02 ��ͷ �����ࣺ���ڵ���0 01 ��ͷ��С��0 02 ��ͷ
               ,TE.BALANCE                          AS POSITION1          --ͷ��1
               ,0                                   AS POSITION3001       --ͷ��3001
               ,0                                   AS POSITION3002       --ͷ��3002
               ,0                                   AS POSITION3003       --ͷ��3003
               ,0                                   AS POSITION3004       --ͷ��3004
               ,0                                   AS POSITION_CD1       --ͷ��_ί�д��
               ,0                                   AS POSITION_CD2       --ͷ��_ί�д���
        FROM (
              --�������Ŀ���⴦��   
              SELECT T1.SUBJECT_NO,
                     T1.CURRENCY_CODE,
                     SUM(CASE
                           WHEN CL.ATTRIBUTE8 = 'D-C' THEN
                            T1.BALANCE_D_BEQ - T1.BALANCE_C_BEQ
                           WHEN CL.ATTRIBUTE8 = 'C-D' THEN
                            T1.BALANCE_C_BEQ - T1.BALANCE_D_BEQ
                           ELSE
                            T1.BALANCE_D_BEQ - T1.BALANCE_C_BEQ  --��ȡ�ۺ�����  ���ֱ�������  ����ȡ�ۺ�����
                         END) AS BALANCE
                FROM RWA_DEV.FNS_GL_BALANCE T1
                LEFT JOIN RWA.CODE_LIBRARY CL
                  ON CL.CODENO = 'NewSubject'
                 AND T1.SUBJECT_NO = CL.ITEMNO
                 AND CL.ISINUSE = '1'
               WHERE T1.CURRENCY_CODE IS NOT NULL
                 AND T1.CURRENCY_CODE NOT IN ('RMB', 'CNY')
                 AND T1.DATANO = p_data_dt_str
                 AND T1.SUBJECT_NO IN ('60110200',
                                       '60110200',
                                       '60110200',
                                       '60110200',
                                       '60110200',
                                       '60110200',
                                       '60110200',
                                       '60110200',
                                       '60110200',
                                       '60110300',
                                       '60110300',
                                       '60110300',
                                       '60110301',
                                       '60110301',
                                       '60110310',
                                       '60110310',
                                       '60110500',
                                       '60110500',
                                       '60110500',
                                       '60110500',
                                       '60110500',
                                       '60110500',
                                       '60110500',
                                       '60110500',
                                       '60110600',
                                       '60110600',
                                       '60110600',
                                       '60110600',
                                       '60110600',
                                       '60110600',
                                       '60110801',
                                       '60110801',
                                       '60110801',
                                       '60110801',
                                       '60110801',
                                       '60110901',
                                       '60110901',
                                       '60110901',
                                       '60110901',
                                       '60110902',
                                       '60110902',
                                       '60110902',
                                       '60110902',
                                       '60110902',
                                       '60110902',
                                       '60110902',
                                       '60110902',
                                       '60110902',
                                       '60110902',
                                       '60110902',
                                       '60110902',
                                       '60110902',
                                       '60110903',
                                       '60110903',
                                       '60110903',
                                       '60110903',
                                       '60111200',
                                       '60111701',
                                       '60111701',
                                       '60111704',
                                       '60111801',
                                       '60112000',
                                       '60112000',
                                       '60112000',
                                       '60112300',
                                       '60113100',
                                       '60210100',
                                       '60210100',
                                       '60210100',
                                       '60210100',
                                       '60210100',
                                       '60210100',
                                       '60210100',
                                       '60210100',
                                       '60210100',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210400',
                                       '60210500',
                                       '60210500',
                                       '60210500',
                                       '60210600',
                                       '60210600',
                                       '60210600',
                                       '60210600',
                                       '60210600',
                                       '60210600',
                                       '60210600',
                                       '60210600',
                                       '60210600',
                                       '60210600',
                                       '60210600',
                                       '60210600',
                                       '60210600',
                                       '60210600',
                                       '60210600',
                                       '60210600',
                                       '60210600',
                                       '60210700',
                                       '60210700',
                                       '60210700',
                                       '60210800',
                                       '60210800',
                                       '60210800',
                                       '60210800',
                                       '60211000',
                                       '60211000',
                                       '60211007',
                                       '60211010',
                                       '60211010',
                                       '60211302',
                                       '60211302',
                                       '60211303',
                                       '60211303',
                                       '60211303',
                                       '60211303',
                                       '60212000',
                                       '60212000',
                                       '60212800',
                                       '60212800',
                                       '60512000',
                                       '60512000',
                                       '60512000',
                                       '60512000',
                                       '60512000',
                                       '60512000',
                                       '60512000',
                                       '60512000',
                                       '60512000',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60610400',
                                       '60612001',
                                       '60612002',
                                       '61110201',
                                       '61110201',
                                       '61110301',
                                       '61110301',
                                       '61110602',
                                       '64022000',
                                       '64110100',
                                       '64110100',
                                       '64110100',
                                       '64110100',
                                       '64110100',
                                       '64110300',
                                       '64110300',
                                       '64110300',
                                       '64110300',
                                       '64110300',
                                       '64110400',
                                       '64110400',
                                       '64110400',
                                       '64110500',
                                       '64110500',
                                       '64110500',
                                       '64110500',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110600',
                                       '64110700',
                                       '64110700',
                                       '64110700',
                                       '64110700',
                                       '64110700',
                                       '64110700',
                                       '64110700',
                                       '64110700',
                                       '64110700',
                                       '64110700',
                                       '64110700',
                                       '64110700',
                                       '64110700',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110800',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64110900',
                                       '64111100',
                                       '64111500',
                                       '64111500',
                                       '64111500',
                                       '64111500',
                                       '64111500',
                                       '64111500',
                                       '64111500',
                                       '64111500',
                                       '64111500',
                                       '64111500',
                                       '64112000',
                                       '64112000',
                                       '64210100',
                                       '64210100',
                                       '64210200',
                                       '64210200',
                                       '64210200',
                                       '64210200',
                                       '64210200',
                                       '64210200',
                                       '64210200',
                                       '64210200',
                                       '64210200',
                                       '64212000',
                                       '64212000',
                                       '64212000',
                                       '64212000',
                                       '64212000',
                                       '66020600',
                                       '66021000',
                                       '66021000',
                                       '66021400',
                                       '66021400',
                                       '66021400',
                                       '66021400',
                                       '66021800',
                                       '66022910',
                                       '66022910',
                                       '66024000',
                                       '67010401',
                                       '67010401',
                                       '67010401',
                                       '67112000')
               GROUP BY T1.SUBJECT_NO, T1.CURRENCY_CODE
             
        ) TE                     
                            
    ) TEMP
    WHERE CURRENCY <> 'CNY' --ֻ�������
    ;

    COMMIT;
    
    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_WH_FESPOTPOSITION',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼��
    SELECT COUNT(1) INTO v_count FROM RWA_DEV.RWA_WH_FESPOTPOSITION;
    
    p_po_rtncode := '1';
      p_po_rtnmsg  := '�ɹ�' || '-' || v_count;
        --�����쳣
EXCEPTION
    WHEN OTHERS THEN
                 --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
            ROLLBACK;
                p_po_rtncode := sqlcode;
                p_po_rtnmsg  := '����ֻ�ͷ��('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_WH_FESPOTPOSITION;
/

