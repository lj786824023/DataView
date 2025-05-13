CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_ZX_CONTRACT(
                            p_data_dt_str IN  VARCHAR2,   --��������
                            p_po_rtncode  OUT VARCHAR2,   --���ر�� 1 �ɹ�,0 ʧ��
                            p_po_rtnmsg   OUT VARCHAR2    --��������
        )
  /*
    �洢��������:RWA_DEV.PRO_RWA_ZX_CONTRACT
    ʵ�ֹ���:����ϵͳ-ֱ�����е��-���÷��ձ�¶
    ���ݿھ�:ȫ��
    ����Ƶ��:��ĩ����
    ��  ��  :V1.0.0
    ��д��  :YUSJ
    ��дʱ��:2016-10-18
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1 :RWA_DEV.CBS_IAC|ͨ�÷ֻ���
    Դ  ��2 :RWA.ORG_INFO|������
    Դ  ��3 :RWA.RWA_WS_DSBANK_ADV|ֱ�����е�¼��
    Դ  ��4 :RWA.RWA_WP_DATASUPPLEMENT|��¼������ϸ��Ϣ��
    Ŀ���1 :RWA_DEV.RWA_ZX_COMTRACT|ֱ�����к�ͬ��
    ������ :��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_ZX_CONTRACT';
  --�����ж�ֵ����
  v_count1 INTEGER;
  --�����ж�ֵ����
  --v_count2 INTEGER;
  --�����쳣����
  v_raise EXCEPTION;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_ZX_CONTRACT';
    --2.���������������ݴ�Դ����뵽Ŀ�����

    /*�������ϵͳ��ͬ-�����ͬ��Ϣ*/
    INSERT INTO RWA_DEV.RWA_ZX_CONTRACT(
               DATADATE                              --��������
              ,DATANO                                --������ˮ��
              ,CONTRACTID                            --��ͬID
              ,SCONTRACTID                           --Դ��ͬID
              ,SSYSID                                --ԴϵͳID
              ,CLIENTID                              --��������ID
              ,SORGID                                --Դ����ID
              ,SORGNAME                              --Դ��������
              ,ORGSORTNO                             --�������������
              ,ORGID                                 --��������ID
              ,ORGNAME                               --������������
              ,INDUSTRYID                            --������ҵ����
              ,INDUSTRYNAME                          --������ҵ����
              ,BUSINESSLINE                          --����
              ,ASSETTYPE                             --�ʲ�����
              ,ASSETSUBTYPE                          --�ʲ�С��
              ,BUSINESSTYPEID                        --ҵ��Ʒ�ִ���
              ,BUSINESSTYPENAME                      --ҵ��Ʒ������
              ,CREDITRISKDATATYPE                    --���÷�����������
              ,STARTDATE                             --��ʼ����
              ,DUEDATE                               --��������
              ,ORIGINALMATURITY                      --ԭʼ����
              ,RESIDUALM                             --ʣ������
              ,SETTLEMENTCURRENCY                    --�������
              ,CONTRACTAMOUNT                        --��ͬ�ܽ��
              ,NOTEXTRACTPART                        --��ͬδ��ȡ����
              ,UNCONDCANCELFLAG                      --�Ƿ����ʱ����������
              ,ABSUAFLAG                             --�ʲ�֤ȯ�������ʲ���ʶ
              ,ABSPOOLID                             --֤ȯ���ʲ���ID
              ,GROUPID                               --������
              ,GUARANTEETYPE                         --��Ҫ������ʽ
              ,ABSPROPORTION                         --�ʲ�֤ȯ������
    )
       SELECT
                 TO_DATE(T1.DATANO,'YYYYMMDD')                                     AS DATADATE            --��������
                ,T1.DATANO                                                         AS DATANO              --������ˮ��
                ,T1.IACAC_NO                                                       AS CONTRACTID          --��ͬID
                ,T1.IACAC_NO                                                       AS SCONTRACTID         --Դ��ͬID
                ,'ZX'                                                              AS SSYSID              --ԴϵͳID
                ,T2.CUSTID1                                                        AS CLIENTID            --��������ID      ȡ��������������ID
                ,T1.IACGACBR                                                       AS SORGID              --Դ����ID
                ,T3.ORGNAME                                                        AS SORGNAME            --Դ��������
                ,T3.SORTNO                                                         AS ORGSORTNO           --�������������
                ,T1.IACGACBR                                                       AS ORGID               --��������ID
                ,T3.ORGNAME                                                        AS ORGNAME             --������������
                ,T2.INDUSTRYID                                                     AS INDUSTRYID          --������ҵ����
                ,T4.ITEMNAME                                                       AS INDUSTRYNAME        --������ҵ����
                ,'0101'                                                            AS BUSINESSLINE        --����            Ĭ�ϣ�01-����
                ,''                                                                AS ASSETTYPE           --�ʲ�����
                ,''                                                                AS ASSETSUBTYPE        --�ʲ�С��
                ,'109010'                                                          AS BUSINESSTYPEID      --ҵ��Ʒ�ִ���
                ,'ֱ�����е��'                                                     AS BUSINESSTYPENAME     --ҵ��Ʒ������
                ,'01'                                                              AS CREDITRISKDATATYPE  --���÷�����������   Ĭ�ϡ�һ������ۡ�01
                ,NVL(T2.IACCRTDAT,P_DATA_DT_STR)                                   AS STARTDATE           -- ��ʼ����
                ,NVL(T2.IACDLTDAT,TO_CHAR(TO_DATE(P_DATA_DT_STR,'YYYYMMDD')+30,'YYYYMMDD'))           AS DUEDATE             -- ��������
                ,NVL((TO_DATE(T2.IACDLTDAT,'YYYYMMDD')-TO_DATE(T2.IACCRTDAT,'YYYYMMDD'))/365,30/365)  AS ORIGINALMATURITY    --ԭʼ����
                ,NVL((TO_DATE(T2.IACDLTDAT,'YYYYMMDD')-TO_DATE(P_DATA_DT_STR,'YYYYMMDD'))/365,30/365) AS RESIDUALM           --ʣ������
                ,'CNY'                                                             AS SETTLEMENTCURRENCY  --�������
                ,ABS(T1.IACCURBAL)                                                 AS CONTRACTAMOUNT      --��ͬ�ܽ��
                ,0                                                                 AS NOTEXTRACTPART      --��ͬδ��ȡ����    Ĭ��0
                ,'0'                                                               AS UNCONDCANCELFLAG    --�Ƿ����ʱ����������    Ĭ�Ϸ� 0
                ,'0'                                                               AS ABSUAFLAG           --�ʲ�֤ȯ�������ʲ���ʶ  Ĭ�Ϸ� 0
                ,''                                                                AS ABSPOOLID           --֤ȯ���ʲ���ID        ����Ϊ��
                ,''                                                                AS GROUPID             --������
                ,''                                                                AS GUARANTEETYPE       --��Ҫ������ʽ
                ,NULL                                                              AS ABSPROPORTION       --�ʲ�֤ȯ������
    FROM        RWA_DEV.CBS_IAC T1                                        --ͨ�÷ֻ���
    LEFT JOIN   (SELECT WDA.IACAC_NO
                       ,WDA.CUSTID1
                       ,WDA.INDUSTRYID
                       ,TO_CHAR(TO_DATE(WDA.IACCRTDAT,'YYYY-MM-DD'),'YYYYMMDD') AS IACCRTDAT
                       ,TO_CHAR(TO_DATE(WDA.IACDLTDAT,'YYYY-MM-DD'),'YYYYMMDD') AS IACDLTDAT
                   FROM RWA.RWA_WS_DSBANK_ADV WDA                         --ֱ�����е�����ݲ�¼��
             INNER JOIN RWA.RWA_WP_DATASUPPLEMENT T6                      --���ݲ�¼��
                     ON WDA.SUPPORGID = T6.ORGID
                    AND T6.DATADATE = TO_DATE(P_DATA_DT_STR,'YYYYMMDD')
                    AND T6.SUPPTMPLID = 'M-0190'
                    AND T6.SUBMITFLAG = '1'
                  WHERE WDA.DATADATE = TO_DATE(P_DATA_DT_STR,'YYYYMMDD')
                ) T2
    ON          T1.IACAC_NO = T2.IACAC_NO
    LEFT JOIN   RWA.ORG_INFO T3
    ON          T1.IACGACBR = T3.ORGID
    LEFT JOIN   RWA.CODE_LIBRARY  T4                                      --�����ȡ��ҵ
    ON          T2.INDUSTRYID = T4.ITEMNO
    AND         T4.CODENO = 'IndustryType'
    WHERE       T1.IACITMNO = '13070800'                                  --ֱ�����е��
    AND         T1.IACCURBAL <> 0                                         --�˻�������0
    AND         T1.DATANO = P_DATA_DT_STR
    ;
    COMMIT;

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_ZX_CONTRACT',cascade => true);

    /*Ŀ�������ͳ��*/
    --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_ZX_CONTRACT;

    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�' || '-' || v_count1;
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
         ROLLBACK;
         p_po_rtncode := sqlcode;
         p_po_rtnmsg  := '��ͬ��Ϣ('|| v_pro_name ||')ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_ZX_CONTRACT;
/

