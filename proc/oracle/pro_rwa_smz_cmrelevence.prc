CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_SMZ_CMRELEVENCE(
                             P_DATA_DT_STR  IN  VARCHAR2,    --��������
                             P_PO_RTNCODE  OUT  VARCHAR2,    --���ر�� 1 �ɹ�,0 ʧ��
                            P_PO_RTNMSG    OUT  VARCHAR2    --��������
        )
  /*
    �洢��������:PRO_RWA_SMZ_CMRELEVENCE
    ʵ�ֹ���:˽ļծ-��ͬ�뻺�������,��ṹΪ��ͬ�����������
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�
    ��  ��  :V1.0.0
    ��д��  :SHUXD
    ��дʱ��:2016-07-08
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1  :RWA_WS_PRIVATE_BOND|˽ļծҵ��¼ģ��
    Ŀ���  :RWA_SMZ_CMRELEVENCE|˽ļծ-��ͬ�뻺�������
    ������  :��
    �����¼(�޸���|�޸�ʱ��|�޸�����):
  */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'PRO_RWA_SMZ_CMRELEVENCE';
  --�����ж�ֵ����
  v_count INTEGER;
  --�����쳣����
  v_raise EXCEPTION;
    --������ʱ����
  v_tabname VARCHAR2(200);
  --���崴�����
  v_create VARCHAR2(1000);

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_SMZ_CMRELEVENCE';
    --2 ���²�¼��ĵ���ID
    UPDATE RWA.RWA_WS_PRIVATE_BOND T1 SET T1.DBID = p_data_dt_str || 'SMZ' || lpad(rownum, 10, '0')
       WHERE T1.DATADATE = TO_DATE(p_data_dt_str,'yyyymmdd') and T1.DBLX IS NOT NULL
    ;
    COMMIT;
    /*������Ч����º�ͬ��Ӧ�ĵ���ѺƷ����*/
    INSERT INTO RWA_DEV.RWA_SMZ_CMRELEVENCE(
                  DATADATE                         --��������
                 ,DATANO                           --������ˮ��
                 ,CONTRACTID                       --��ͬ����
                 ,MITIGATIONID                     --���������
                 ,MITIGCATEGORY                    --����������
                 ,SGUARCONTRACTID                  --Դ������ͬ����
                 ,GROUPID                          --������
    )
    SELECT
                 TO_DATE(P_DATA_DT_STR,'YYYYMMDD')                              AS  datadate                         --��������
                 ,P_DATA_DT_STR                                                AS  datano                          --������ˮ��
                 ,T1.ZQID                                                      AS  contractid                      --��ͬ����  (��������ͬ���ж�����ͬ�Ƿ���Ч)
                 ,T1.DBID                                                      AS   mitigationid                    --���������(��֤���ݵĻ��ͺ�ͬ�͵���Ѻ�Ļ��ͺ�ͬ�ֿ�ȡ)
                 ,CASE WHEN T1.DBLX IN ('030010','030020','030030','020080','020090') THEN '02'                      --��֤
                      ELSE '03'                                                                                              --����ѺƷ
                END                                                            AS  mitigcategory                   --����������
                 ,''                                                            AS  sguarcontractid                 --Դ������ͬ����(�������)
                 ,''                                                            AS  groupid                         --������
    FROM    RWA.RWA_WS_PRIVATE_BOND T1
    INNER JOIN  RWA.RWA_WP_DATASUPPLEMENT T2                    --���ݲ�¼��
    ON          T1.SUPPORGID=T2.ORGID
    AND         T2.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
    AND         T2.SUPPTMPLID='M-0110'
    AND         T2.SUBMITFLAG='1'
    WHERE   T1.DATADATE=TO_DATE(p_data_dt_str,'YYYYMMDD')
    AND     T1.DBLX IS NOT NULL
    ;
    COMMIT;


    /*Ŀ�������ͳ��*/
    SELECT COUNT(1) INTO v_count FROM RWA_SMZ_CMRELEVENCE;
    --Dbms_output.Put_line('RWA_SMZ_CMRELEVENCE��ǰ��������ݼ�¼Ϊ:' || v_count1 || '��');

    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̽��� ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    P_PO_RTNCODE := '1';
    P_PO_RTNMSG  := '�ɹ�'||'-'||v_count;
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          P_PO_RTNCODE := sqlcode;
          P_PO_RTNMSG  := '˽ļծ-��ͬ�뻺�������(PRO_RWA_SMZ_CMRELEVENCE)ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace;
         RETURN;
END PRO_RWA_SMZ_CMRELEVENCE;
/

