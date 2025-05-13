CREATE OR REPLACE PROCEDURE RWA_DEV.PRO_RWA_DZ_CONTRACT(
                             p_data_dt_str  IN  VARCHAR2,    --��������
                             p_po_rtncode  OUT  VARCHAR2,    --���ر�� 1 �ɹ�,0 ʧ��
                             p_po_rtnmsg    OUT  VARCHAR2    --��������
        )
  /*
    �洢��������:RWA_DEV.PRO_RWA_DZ_CONTRACT
    ʵ�ֹ���:��ծ�ʲ���¼��ͬ��Ϣ
    ���ݿھ�:ȫ��
    ����Ƶ��:�³�
    ��  ��  :V1.0.0
    ��д��  :YUSJ
    ��дʱ��:2016-10-09
    ��  λ  :�Ϻ���˶��Ϣ�����ɷ����޹�˾
    Դ  ��1  :NCM_ASSET_DEBT_INFO |��ծ�ʲ���Ϣ��
    Ŀ���  :RWA_DEV.RWA_DZ_CONTRACT|�Ŵ�ϵͳ��ͬ��
    ������  :��
    �����¼(�޸���|�޸�ʱ��|�޸�����)��
    */
  AS
  --����һ����������
  PRAGMA AUTONOMOUS_TRANSACTION;

  /*��������*/
  --����洢�������Ʋ���ֵ
  v_pro_name VARCHAR2(200) := 'RWA_DEV.PRO_RWA_DZ_CONTRACT';
  --�����ж�ֵ����
  v_count1 INTEGER;
  --�����쳣����
  v_raise EXCEPTION;

  BEGIN
    --Dbms_output.Put_line('��ִ�� ' || v_pro_name || ' �洢���̿�ʼ ��:' || TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss'));

    /*�����ȫ�����ݼ��������Ŀ���*/
    --1.���Ŀ����е�ԭ�м�¼
    EXECUTE IMMEDIATE 'TRUNCATE TABLE RWA_DEV.RWA_DZ_CONTRACT';
/*
    \*������Ч����µĺ�ͬ��Ϣ(�Ŵ�ϵͳ���Խ��Ϊ׼)*\
    INSERT INTO RWA_DEV.RWA_DZ_CONTRACT(
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
							,ABSUAFLAG         										 --�ʲ�֤ȯ�������ʲ���ʶ
							,ABSPOOLID         										 --֤ȯ���ʲ���ID
							,GROUPID           										 --������
							,GUARANTEETYPE												 --��Ҫ������ʽ
							,ABSPROPORTION                         --�ʲ�֤ȯ������
    )
     SELECT
                 TO_DATE(T1.DATANO,'YYYYMMDD')                                     AS DATADATE            --��������
                ,T1.DATANO                                                         AS DATANO              --������ˮ��
                ,'DZ-' || T1.GUARANTYID                                            AS CONTRACTID          --��ͬID
                ,T1.GUARANTYID                                                     AS SCONTRACTID         --Դ��ͬID
                ,'DZ'                                                              AS SSYSID              --ԴϵͳID
                ,'DZ-' || T1.GUARANTYID                                            AS CLIENTID            --��������ID
                ,'01050000'	                                                       AS SORGID              --Դ����ID
                ,'�����ʲ���ծ����'                                              AS SORGNAME            --Դ��������
                ,'1010050'                                                         AS ORGSORTNO           --�������������
                ,'01050000'	                                                   		 AS ORGID               --��������ID
                ,'�����ʲ���ծ����'                                              AS ORGNAME             --������������
                ,'999999'                                                          AS INDUSTRYID          --������ҵ����
                ,'δ֪'                                                            AS INDUSTRYNAME        --������ҵ����
                ,CASE WHEN T4.TYPEDIVISION='1' THEN '0501'
                			ELSE '0401'
                 END	                                                             AS BUSINESSLINE        --����
                ,'129'                                                             AS ASSETTYPE           --�ʲ�����
                ,'12901'                                                           AS ASSETSUBTYPE        --�ʲ�С��
                ,CASE WHEN T4.TYPEDIVISION='1' THEN '109040'
                      ELSE '109050'
                 END                                                               AS BUSINESSTYPEID      --ҵ��Ʒ�ִ���
                ,CASE WHEN T4.TYPEDIVISION='1' THEN '��ծ�ʲ���������'
                      ELSE '��ծ�ʲ��ǲ�������'
                 END                                                               AS BUSINESSTYPENAME    --ҵ��Ʒ������
                ,'01'                                                              AS CREDITRISKDATATYPE  --���÷�����������
                ,T1.ACQUIREDATE                                                    AS STARTDATE           --��ʼ����
                ,T1.DATANO                                 	                       AS DUEDATE             --��������
                ,CASE WHEN (TO_DATE(T1.DATANO,'YYYYMMDD')-TO_DATE(REPLACE(T1.ACQUIREDATE,'/',''),'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T1.DATANO,'YYYYMMDD')-TO_DATE(REPLACE(T1.ACQUIREDATE,'/',''),'YYYYMMDD'))/365
                 END                                                               AS OriginalMaturity    --ԭʼ����
                ,CASE WHEN (TO_DATE(T1.DATANO,'YYYYMMDD')-TO_DATE(REPLACE(T1.ACQUIREDATE,'/',''),'YYYYMMDD'))/365<0
                      THEN 0
                      ELSE (TO_DATE(T1.DATANO,'YYYYMMDD')-TO_DATE(REPLACE(T1.ACQUIREDATE,'/',''),'YYYYMMDD'))/365
                 END                                                               AS ResidualM           --ʣ������
                ,'CNY'                                                             AS SETTLEMENTCURRENCY  --�������
                ,T1.ENTRYVALUE                                                     AS CONTRACTAMOUNT      --��ͬ�ܽ��
                ,0                                                                 AS NOTEXTRACTPART      --��ͬδ��ȡ����
                ,'0'                                                               AS UNCONDCANCELFLAG    --�Ƿ����ʱ����������    0:��1����
                ,'0'                                                               AS ABSUAFLAG           --�ʲ�֤ȯ�������ʲ���ʶ
                ,NULL                                                              AS ABSPOOLID           --֤ȯ���ʲ���ID
                ,''                                                                AS GROUPID             --������
                ,''                                                                AS GUARANTEETYPE       --��Ҫ������ʽ
                ,NULL                                                              AS ABSPROPORTION       --�ʲ�֤ȯ������
    FROM 				RWA_DEV.NCM_ASSET_DEBT_INFO T1
    LEFT JOIN 	RWA.ORG_INFO T2
    ON 					T1.MANAGEORGID=T2.ORGID
    LEFT JOIN   RWA_DEV.NCM_COL_PARAM T4
    ON          T1.GUARANTYTYPEID=T4.GUARANTYTYPE
    AND         T4.DATANO=P_DATA_DT_STR
    WHERE  			T1.DATANO=P_DATA_DT_STR
    ;
    COMMIT;*/
    
  ---�����ծ�ʲ���ͬ��Ϣ  
 /*    INSERT INTO RWA_DEV.RWA_DZ_CONTRACT(
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
                ,T1.CONTRACTID                                         AS CONTRACTID          --��ͬID
                ,T1.CONTRACTID                                                    AS SCONTRACTID         --Դ��ͬID
                ,T1.SSYSID                                                             AS SSYSID              --ԴϵͳID
                ,T1.CLIENTID                                            AS CLIENTID            --��������ID
                ,T1.SORGID                                                       AS SORGID              --Դ����ID
                ,T1.SORGNAME                                              AS SORGNAME            --Դ��������
                ,T1.ORGSORTNO                                                         AS ORGSORTNO           --�������������
                ,T1.ORGID                                                        AS ORGID               --��������ID
                ,T1.ORGNAME                                             AS ORGNAME             --������������
                ,T1.INDUSTRYID                                                          AS INDUSTRYID          --������ҵ����
                ,T1.INDUSTRYNAME                                                         AS INDUSTRYNAME        --������ҵ����
                ,T1.BUSINESSLINE                                                               AS BUSINESSLINE        --����
                ,T1.ASSETTYPE                                                             AS ASSETTYPE           --�ʲ�����
                ,T1.ASSETSUBTYPE                                                         AS ASSETSUBTYPE        --�ʲ�С��
                ,T1.BUSINESSTYPEID                                                              AS BUSINESSTYPEID      --ҵ��Ʒ�ִ���
                ,T1.BUSINESSTYPENAME                                                              AS BUSINESSTYPENAME    --ҵ��Ʒ������
                ,'01'                                                              AS CREDITRISKDATATYPE  --���÷�����������
                ,T1.STARTDATE                                                    AS STARTDATE           --��ʼ����
                ,T1.DUEDATE                                                         AS DUEDATE             --��������
                ,T1.ORIGINALMATURITY                                                              AS OriginalMaturity    --ԭʼ����
                ,T1.RESIDUALM                                                              AS ResidualM           --ʣ������
                ,'CNY'                                                             AS SETTLEMENTCURRENCY  --�������
                ,T1.NORMALPRINCIPAL                                                     AS CONTRACTAMOUNT      --��ͬ�ܽ��
                ,0                                                                 AS NOTEXTRACTPART      --��ͬδ��ȡ����
                ,'0'                                                               AS UNCONDCANCELFLAG    --�Ƿ����ʱ����������    0:��1����
                ,'0'                                                               AS ABSUAFLAG           --�ʲ�֤ȯ�������ʲ���ʶ
                ,NULL                                                              AS ABSPOOLID           --֤ȯ���ʲ���ID
                ,''                                                                AS GROUPID             --������
                ,''                                                                AS GUARANTEETYPE       --��Ҫ������ʽ
                ,NULL                                                              AS ABSPROPORTION       --�ʲ�֤ȯ������
    FROM        RWA_DZ_EXPOSURE T1
    WHERE       T1.DATANO=P_DATA_DT_STR
    ;
    COMMIT;*/

    dbms_stats.gather_table_stats(ownname => 'RWA_DEV',tabname => 'RWA_DZ_CONTRACT',cascade => true);

     --ͳ�Ʋ���ļ�¼
    SELECT COUNT(1) INTO v_count1 FROM RWA_DEV.RWA_DZ_CONTRACT;

    p_po_rtncode := '1';
    p_po_rtnmsg  := '�ɹ�'||'-'||v_count1;
    --�����쳣
    EXCEPTION
    WHEN OTHERS THEN
         --Dbms_output.Put_line('������,�������Ϊ:'||sqlcode||';������ϢΪ:'||sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace);
          ROLLBACK;
          p_po_rtncode := sqlcode;
          p_po_rtnmsg  := '��ծ�ʲ���¼��Ϣ-��ͬ(RWA_DEV.RWA_DZ_CONTRACT)ETLת��ʧ�ܣ�'|| sqlerrm||';��������Ϊ:'||dbms_utility.format_error_backtrace ;
         RETURN;
END PRO_RWA_DZ_CONTRACT;
/

